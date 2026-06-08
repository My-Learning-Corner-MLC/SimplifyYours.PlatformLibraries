using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SimplifyYours.Event.Abstractions;

namespace SimplifyYours.Event.Consumer;

public sealed class EventConsumerBackgroundService(
    IServiceScopeFactory serviceScopeFactory,
    IOptions<EventConsumerOptions> options,
    IKafkaEventConsumerFactory consumerFactory,
    IKafkaDeadLetterProducerFactory deadLetterProducerFactory,
    ILogger<EventConsumerBackgroundService> logger)
    : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var consumerOptions = options.Value;

        if (string.IsNullOrWhiteSpace(consumerOptions.BootstrapServers)
            || string.IsNullOrWhiteSpace(consumerOptions.GroupId)
            || consumerOptions.Subscriptions.Count == 0)
        {
            logger.LogInformation(
                "Event consumer is disabled because Kafka configuration is incomplete.");
            return;
        }

        using var consumer = consumerFactory.Create(consumerOptions);
        using var deadLetterProducer = deadLetterProducerFactory.Create(consumerOptions);

        var topics = consumerOptions.Subscriptions.Select(subscription => subscription.Topic).Distinct().ToArray();
        consumer.Subscribe(topics);
        logger.LogInformation(
            "Event consumer started. GroupId: {GroupId}. Topics: {Topics}. SubscriptionCount: {SubscriptionCount}.",
            consumerOptions.GroupId,
            string.Join(",", topics),
            consumerOptions.Subscriptions.Count);
        await Task.Yield();

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var message = consumer.Consume(stoppingToken);
                logger.LogDebug(
                    "Kafka event message consumed. Topic: {Topic}. Key: {Key}.",
                    message.Topic,
                    message.Key);
                var handled = await HandleAsync(
                    message,
                    consumerOptions,
                    deadLetterProducer,
                    stoppingToken);

                if (handled)
                {
                    consumer.Commit();
                    logger.LogDebug(
                        "Kafka event message committed. Topic: {Topic}. Key: {Key}.",
                        message.Topic,
                        message.Key);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (ConsumeException exception)
            {
                logger.LogError(exception, "Failed to consume Kafka event message.");
            }
            catch (JsonException exception)
            {
                logger.LogError(exception, "Failed to deserialize Kafka event message.");
            }
            catch (Exception exception)
            {
                logger.LogError(exception, "Failed to handle Kafka event message.");
            }
        }

        consumer.Close();
    }

    private async Task<bool> HandleAsync(
        ConsumedEventMessage message,
        EventConsumerOptions consumerOptions,
        IKafkaDeadLetterProducer deadLetterProducer,
        CancellationToken cancellationToken)
    {
        var envelope = JsonSerializer.Deserialize<IntegrationEventEnvelope>(
            message.Value,
            EventJsonSerializerOptions.Default)
            ?? throw new JsonException("Integration event envelope is required.");

        var subscription = ResolveSubscription(consumerOptions, message.Topic, envelope.EventType);

        if (subscription is null)
        {
            logger.LogDebug(
                "Skipping event {EventId} of type {EventType} from topic {Topic} because no subscription matches.",
                envelope.EventId,
                envelope.EventType,
                message.Topic);
            return true;
        }

        using var scope = serviceScopeFactory.CreateScope();
        var inboxStore = scope.ServiceProvider.GetRequiredService<IEventInboxStore>();

        var inboxRecord = await inboxStore.GetAsync(envelope.EventId, cancellationToken);

        if (inboxRecord?.Status == EventInboxRecordStatus.Processed)
        {
            logger.LogInformation(
                "Skipping already processed event. EventId: {EventId}. EventType: {EventType}. Topic: {Topic}.",
                envelope.EventId,
                envelope.EventType,
                message.Topic);
            return true;
        }

        if (inboxRecord?.Status == EventInboxRecordStatus.Failed
            && inboxRecord.HandleAttempts >= Math.Max(1, consumerOptions.MaxHandleAttempts))
        {
            logger.LogWarning(
                "Skipping terminally failed event. EventId: {EventId}. EventType: {EventType}. Attempts: {Attempts}.",
                envelope.EventId,
                envelope.EventType,
                inboxRecord.HandleAttempts);
            return true;
        }

        await inboxStore.MarkProcessingAsync(envelope, DateTimeOffset.UtcNow, cancellationToken);
        logger.LogInformation(
            "Event handling started. EventId: {EventId}. EventType: {EventType}. Topic: {Topic}.",
            envelope.EventId,
            envelope.EventType,
            message.Topic);

        try
        {
            await DispatchAsync(scope.ServiceProvider, subscription, envelope, message, cancellationToken);

            await inboxStore.MarkProcessedAsync(
                envelope.EventId,
                envelope.EventType,
                DateTimeOffset.UtcNow,
                cancellationToken);

            logger.LogInformation(
                "Event handled successfully. EventId: {EventId}. EventType: {EventType}. Topic: {Topic}.",
                envelope.EventId,
                envelope.EventType,
                message.Topic);

            return true;
        }
        catch (Exception exception)
        {
            var attemptsAfterFailure = (inboxRecord?.HandleAttempts ?? 0) + 1;
            var terminal = attemptsAfterFailure >= Math.Max(1, consumerOptions.MaxHandleAttempts);

            await inboxStore.MarkFailedAsync(
                envelope.EventId,
                envelope.EventType,
                exception.Message,
                terminal,
                cancellationToken);

            if (terminal)
            {
                await deadLetterProducer.ProduceAsync(
                    message.Topic + consumerOptions.DeadLetterTopicSuffix,
                    envelope.EventId.ToString(),
                    message.Value,
                    cancellationToken);

                logger.LogError(
                    exception,
                    "Event failed permanently and was published to the dead-letter topic. EventId: {EventId}. EventType: {EventType}. AttemptsAfterFailure: {AttemptsAfterFailure}.",
                    envelope.EventId,
                    envelope.EventType,
                    attemptsAfterFailure);

                return true;
            }

            logger.LogWarning(
                exception,
                "Event handling failed and will be retried. EventId: {EventId}. EventType: {EventType}. AttemptsAfterFailure: {AttemptsAfterFailure}.",
                envelope.EventId,
                envelope.EventType,
                attemptsAfterFailure);

            return false;
        }
    }

    private static EventSubscription? ResolveSubscription(
        EventConsumerOptions options,
        string topic,
        string eventType)
    {
        return options.Subscriptions.FirstOrDefault(subscription =>
            subscription.Topic == topic
            && (subscription.EventTypes.Count == 0 || subscription.EventTypes.Contains(eventType)));
    }

    private static async Task DispatchAsync(
        IServiceProvider serviceProvider,
        EventSubscription subscription,
        IntegrationEventEnvelope envelope,
        ConsumedEventMessage message,
        CancellationToken cancellationToken)
    {
        var payload = JsonSerializer.Deserialize(
            envelope.Payload,
            subscription.PayloadType,
            EventJsonSerializerOptions.Default)
            ?? throw new JsonException("Integration event payload is required.");

        var handlerType = typeof(IIntegrationEventHandler<>).MakeGenericType(subscription.PayloadType);
        var handler = serviceProvider.GetRequiredService(handlerType);
        var contextType = typeof(IntegrationEventContext<>).MakeGenericType(subscription.PayloadType);
        var context = Activator.CreateInstance(contextType, envelope, payload, message.Topic, message.Key)
            ?? throw new InvalidOperationException("Integration event context could not be created.");
        var handleMethod = handlerType.GetMethod(nameof(IIntegrationEventHandler<object>.HandleAsync))
            ?? throw new InvalidOperationException("Integration event handler is invalid.");

        var task = (Task?)handleMethod.Invoke(handler, [context, cancellationToken]);

        if (task is null)
        {
            throw new InvalidOperationException("Integration event handler did not return a task.");
        }

        await task;
    }
}
