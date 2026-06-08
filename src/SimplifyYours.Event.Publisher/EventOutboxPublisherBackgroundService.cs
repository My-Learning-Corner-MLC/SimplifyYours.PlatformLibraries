using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SimplifyYours.Event.Abstractions;

namespace SimplifyYours.Event.Publisher;

public sealed class EventOutboxPublisherBackgroundService(
    IServiceScopeFactory serviceScopeFactory,
    IOptions<EventPublisherOptions> options,
    IKafkaEventProducerFactory producerFactory,
    ILogger<EventOutboxPublisherBackgroundService> logger)
    : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var publisherOptions = options.Value;

        if (string.IsNullOrWhiteSpace(publisherOptions.BootstrapServers)
            || string.IsNullOrWhiteSpace(publisherOptions.DefaultTopic))
        {
            logger.LogInformation(
                "Event outbox publisher is disabled because Kafka configuration is incomplete.");
            return;
        }

        using var producer = producerFactory.Create(publisherOptions);

        logger.LogInformation(
            "Event outbox publisher started. DefaultTopic: {DefaultTopic}. BatchSize: {BatchSize}. PollingIntervalSeconds: {PollingIntervalSeconds}.",
            publisherOptions.DefaultTopic,
            Math.Max(1, publisherOptions.BatchSize),
            GetPollingInterval(publisherOptions).TotalSeconds);

        while (!stoppingToken.IsCancellationRequested)
        {
            await PublishBatchAsync(producer, publisherOptions, stoppingToken);
            await Task.Delay(GetPollingInterval(publisherOptions), stoppingToken);
        }
    }

    private async Task PublishBatchAsync(
        IKafkaEventProducer producer,
        EventPublisherOptions publisherOptions,
        CancellationToken cancellationToken)
    {
        using var scope = serviceScopeFactory.CreateScope();
        var outboxStore = scope.ServiceProvider.GetRequiredService<IEventOutboxStore>();

        var batchSize = Math.Max(1, publisherOptions.BatchSize);
        var maxAttempts = Math.Max(1, publisherOptions.MaxPublishAttempts);
        var records = await outboxStore.GetPendingAsync(batchSize, maxAttempts, cancellationToken);

        if (records.Count > 0)
        {
            logger.LogInformation(
                "Event outbox batch loaded. MessageCount: {MessageCount}. BatchSize: {BatchSize}.",
                records.Count,
                batchSize);
        }

        foreach (var record in records)
        {
            try
            {
                var topic = string.IsNullOrWhiteSpace(record.Topic)
                    ? publisherOptions.DefaultTopic!
                    : record.Topic;

                await producer.ProduceAsync(
                    topic,
                    record.Id.ToString(),
                    JsonSerializer.Serialize(record.ToEnvelope(), EventJsonSerializerOptions.Default),
                    cancellationToken);

                await outboxStore.MarkPublishedAsync(record.Id, DateTimeOffset.UtcNow, cancellationToken);

                logger.LogInformation(
                    "Outbox message published. MessageId: {MessageId}. EventType: {EventType}. CorrelationId: {CorrelationId}. Topic: {Topic}.",
                    record.Id,
                    record.EventType,
                    record.CorrelationId,
                    topic);
            }
            catch (Exception exception)
            {
                var attemptsAfterFailure = record.PublishAttempts + 1;
                var terminal = attemptsAfterFailure >= maxAttempts;

                await outboxStore.MarkPublishFailedAsync(
                    record.Id,
                    exception.Message,
                    terminal,
                    cancellationToken);

                logger.LogError(
                    exception,
                    "Failed to publish outbox message {MessageId}. EventType: {EventType}. CorrelationId: {CorrelationId}. AttemptsAfterFailure: {AttemptsAfterFailure}. Terminal: {Terminal}.",
                    record.Id,
                    record.EventType,
                    record.CorrelationId,
                    attemptsAfterFailure,
                    terminal);
            }
        }
    }

    private static TimeSpan GetPollingInterval(EventPublisherOptions options)
    {
        return options.PollingInterval <= TimeSpan.Zero
            ? TimeSpan.FromSeconds(5)
            : options.PollingInterval;
    }
}
