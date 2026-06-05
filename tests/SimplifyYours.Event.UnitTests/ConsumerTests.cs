using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using SimplifyYours.Event.Abstractions;
using SimplifyYours.Event.Consumer;

namespace SimplifyYours.Event.UnitTests;

public sealed class ConsumerTests
{
    [Fact]
    public async Task HandleAsync_DispatchesToTypedHandlerAndMarksProcessed()
    {
        var inbox = new InMemoryInboxStore();
        var handler = new TestHandler();
        var service = CreateService(inbox, handler, new FakeDeadLetterProducer());
        var message = CreateMessage();

        var handled = await service.HandleAsync(message, CancellationToken.None);

        Assert.True(handled);
        Assert.Single(handler.Handled);
        Assert.Equal(EventInboxRecordStatus.Processed, inbox.Records.Values.Single().Status);
    }

    [Fact]
    public async Task HandleAsync_WhenDuplicateProcessed_SkipsHandler()
    {
        var envelope = CreateEnvelope();
        var inbox = new InMemoryInboxStore();
        await inbox.MarkProcessedAsync(envelope.EventId, envelope.EventType, DateTimeOffset.UtcNow, CancellationToken.None);
        var handler = new TestHandler();
        var service = CreateService(inbox, handler, new FakeDeadLetterProducer());

        var handled = await service.HandleAsync(ToMessage(envelope), CancellationToken.None);

        Assert.True(handled);
        Assert.Empty(handler.Handled);
    }

    [Fact]
    public async Task HandleAsync_WhenHandlerFailsBelowMaxAttempts_DoesNotCommit()
    {
        var inbox = new InMemoryInboxStore();
        var handler = new TestHandler { ThrowOnHandle = true };
        var deadLetterProducer = new FakeDeadLetterProducer();
        var service = CreateService(inbox, handler, deadLetterProducer);

        var handled = await service.HandleAsync(CreateMessage(), CancellationToken.None);

        Assert.False(handled);
        Assert.Empty(deadLetterProducer.Messages);
        Assert.Equal(EventInboxRecordStatus.Processing, inbox.Records.Values.Single().Status);
        Assert.Equal(1, inbox.Records.Values.Single().HandleAttempts);
    }

    [Fact]
    public async Task HandleAsync_WhenHandlerFailsAtMaxAttempts_PublishesDeadLetterAndHandles()
    {
        var envelope = CreateEnvelope();
        var inbox = new InMemoryInboxStore();
        inbox.Records[envelope.EventId] = new EventInboxRecord(
            envelope.EventId,
            envelope.EventType,
            DateTimeOffset.UtcNow,
            null,
            4,
            EventInboxRecordStatus.Processing,
            "previous");
        var handler = new TestHandler { ThrowOnHandle = true };
        var deadLetterProducer = new FakeDeadLetterProducer();
        var service = CreateService(inbox, handler, deadLetterProducer);

        var handled = await service.HandleAsync(ToMessage(envelope), CancellationToken.None);

        Assert.True(handled);
        var deadLetter = Assert.Single(deadLetterProducer.Messages);
        Assert.Equal("event.references.dlq", deadLetter.Topic);
        Assert.Equal(EventInboxRecordStatus.Failed, inbox.Records[envelope.EventId].Status);
        Assert.Equal(5, inbox.Records[envelope.EventId].HandleAttempts);
    }

    private static EventConsumerBackgroundService CreateService(
        InMemoryInboxStore inbox,
        TestHandler handler,
        FakeDeadLetterProducer deadLetterProducer)
    {
        var services = new ServiceCollection();
        services.AddScoped<IEventInboxStore>(_ => inbox);
        services.AddScoped<IIntegrationEventHandler<TestPayload>>(_ => handler);
        var scopeFactory = services.BuildServiceProvider().GetRequiredService<IServiceScopeFactory>();
        var options = Options.Create(new EventConsumerOptions
        {
            BootstrapServers = "localhost:9092",
            GroupId = "guest-management-service",
            MaxHandleAttempts = 5,
            DeadLetterTopicSuffix = ".dlq"
        });
        options.Value.Subscribe<TestPayload>(
            "event.references",
            ["EventCreated", "EventUpdated", "EventDeleted"]);

        return new EventConsumerBackgroundService(
            scopeFactory,
            options,
            new FakeConsumerFactory(),
            new FakeDeadLetterProducerFactory(deadLetterProducer),
            NullLogger<EventConsumerBackgroundService>.Instance);
    }

    private static ConsumedEventMessage CreateMessage()
    {
        return ToMessage(CreateEnvelope());
    }

    private static IntegrationEventEnvelope CreateEnvelope()
    {
        return IntegrationEventEnvelope.Create(
            "EventCreated",
            new TestPayload(Guid.NewGuid(), "Launch"),
            DateTimeOffset.UtcNow);
    }

    private static ConsumedEventMessage ToMessage(IntegrationEventEnvelope envelope)
    {
        return new ConsumedEventMessage(
            "event.references",
            envelope.EventId.ToString(),
            JsonSerializer.Serialize(envelope, EventJsonSerializerOptions.Default));
    }

    private sealed record TestPayload(Guid EventId, string EventName);

    private sealed class TestHandler : IIntegrationEventHandler<TestPayload>
    {
        public List<IntegrationEventContext<TestPayload>> Handled { get; } = [];

        public bool ThrowOnHandle { get; init; }

        public Task HandleAsync(
            IntegrationEventContext<TestPayload> context,
            CancellationToken cancellationToken)
        {
            if (ThrowOnHandle)
            {
                throw new InvalidOperationException("Handle failed.");
            }

            Handled.Add(context);
            return Task.CompletedTask;
        }
    }

    private sealed class InMemoryInboxStore : IEventInboxStore
    {
        public Dictionary<Guid, EventInboxRecord> Records { get; } = [];

        public Task<EventInboxRecord?> GetAsync(Guid messageId, CancellationToken cancellationToken)
        {
            Records.TryGetValue(messageId, out var record);
            return Task.FromResult(record);
        }

        public Task MarkProcessingAsync(
            IntegrationEventEnvelope envelope,
            DateTimeOffset receivedAt,
            CancellationToken cancellationToken)
        {
            Records.TryAdd(
                envelope.EventId,
                new EventInboxRecord(
                    envelope.EventId,
                    envelope.EventType,
                    receivedAt,
                    null,
                    0,
                    EventInboxRecordStatus.Processing,
                    null));

            return Task.CompletedTask;
        }

        public Task MarkProcessedAsync(
            Guid messageId,
            string eventType,
            DateTimeOffset processedAt,
            CancellationToken cancellationToken)
        {
            if (!Records.TryGetValue(messageId, out var current))
            {
                current = new EventInboxRecord(
                    messageId,
                    eventType,
                    DateTimeOffset.UtcNow,
                    null,
                    0,
                    EventInboxRecordStatus.Processing,
                    null);
            }

            Records[messageId] = current with
            {
                ProcessedAt = processedAt,
                Status = EventInboxRecordStatus.Processed,
                Error = null
            };
            return Task.CompletedTask;
        }

        public Task MarkFailedAsync(
            Guid messageId,
            string eventType,
            string error,
            bool terminal,
            CancellationToken cancellationToken)
        {
            var current = Records[messageId];
            Records[messageId] = current with
            {
                HandleAttempts = current.HandleAttempts + 1,
                Status = terminal ? EventInboxRecordStatus.Failed : EventInboxRecordStatus.Processing,
                Error = error
            };
            return Task.CompletedTask;
        }
    }

    private sealed class FakeConsumerFactory : IKafkaEventConsumerFactory
    {
        public IKafkaEventConsumer Create(EventConsumerOptions options)
        {
            throw new NotSupportedException();
        }
    }

    private sealed class FakeDeadLetterProducerFactory(FakeDeadLetterProducer producer) : IKafkaDeadLetterProducerFactory
    {
        public IKafkaDeadLetterProducer Create(EventConsumerOptions options)
        {
            return producer;
        }
    }

    private sealed class FakeDeadLetterProducer : IKafkaDeadLetterProducer
    {
        public List<(string Topic, string Key, string Value)> Messages { get; } = [];

        public Task ProduceAsync(
            string topic,
            string key,
            string value,
            CancellationToken cancellationToken)
        {
            Messages.Add((topic, key, value));
            return Task.CompletedTask;
        }

        public void Dispose()
        {
        }
    }
}
