using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using SimplifyYours.Event.Abstractions;
using SimplifyYours.Event.Publisher;

namespace SimplifyYours.Event.UnitTests;

public sealed class PublisherTests
{
    [Fact]
    public async Task PublishBatchAsync_WhenOutboxIsEmpty_DoesNotPublish()
    {
        var store = new InMemoryOutboxStore([]);
        var producer = new FakeProducer();
        var service = CreateService(store, producer);

        await service.PublishBatchAsync(CancellationToken.None);

        Assert.Empty(producer.Messages);
    }

    [Fact]
    public async Task PublishBatchAsync_PublishesBatchAndMarksPublished()
    {
        var message = CreateOutboxRecord();
        var store = new InMemoryOutboxStore([message]);
        var producer = new FakeProducer();
        var service = CreateService(store, producer);

        await service.PublishBatchAsync(CancellationToken.None);

        var published = Assert.Single(producer.Messages);
        Assert.Equal("event.references", published.Topic);
        Assert.Equal(message.Id.ToString(), published.Key);
        Assert.Contains("EventCreated", published.Value, StringComparison.Ordinal);
        Assert.Contains(message.Id, store.Published);
    }

    [Fact]
    public async Task PublishBatchAsync_WhenProducerFails_MarksFailureAsTerminalAtMaxAttempts()
    {
        var message = CreateOutboxRecord(publishAttempts: 4);
        var store = new InMemoryOutboxStore([message]);
        var producer = new FakeProducer { ThrowOnPublish = true };
        var service = CreateService(store, producer);

        await service.PublishBatchAsync(CancellationToken.None);

        var failure = Assert.Single(store.Failures);
        Assert.Equal(message.Id, failure.MessageId);
        Assert.True(failure.Terminal);
    }

    private static EventOutboxPublisherBackgroundService CreateService(
        IEventOutboxStore store,
        FakeProducer producer)
    {
        var services = new ServiceCollection();
        services.AddScoped(_ => store);
        var scopeFactory = services.BuildServiceProvider().GetRequiredService<IServiceScopeFactory>();
        var options = Options.Create(new EventPublisherOptions
        {
            BootstrapServers = "localhost:9092",
            DefaultTopic = "event.references",
            BatchSize = 25,
            MaxPublishAttempts = 5
        });

        return new EventOutboxPublisherBackgroundService(
            scopeFactory,
            options,
            new FakeProducerFactory(producer),
            NullLogger<EventOutboxPublisherBackgroundService>.Instance);
    }

    private static EventOutboxRecord CreateOutboxRecord(int publishAttempts = 0)
    {
        return new EventOutboxRecord(
            Guid.NewGuid(),
            "EventCreated",
            DateTimeOffset.UtcNow,
            Guid.NewGuid(),
            null,
            "{}",
            1,
            publishAttempts);
    }

    private sealed class InMemoryOutboxStore(IReadOnlyList<EventOutboxRecord> records) : IEventOutboxStore
    {
        public List<Guid> Published { get; } = [];

        public List<(Guid MessageId, bool Terminal)> Failures { get; } = [];

        public Task<IReadOnlyList<EventOutboxRecord>> GetPendingAsync(
            int batchSize,
            int maxPublishAttempts,
            CancellationToken cancellationToken)
        {
            return Task.FromResult<IReadOnlyList<EventOutboxRecord>>(
                records
                    .Where(record => record.PublishAttempts < maxPublishAttempts)
                    .Take(batchSize)
                    .ToList());
        }

        public Task MarkPublishedAsync(
            Guid messageId,
            DateTimeOffset publishedAt,
            CancellationToken cancellationToken)
        {
            Published.Add(messageId);
            return Task.CompletedTask;
        }

        public Task MarkPublishFailedAsync(
            Guid messageId,
            string error,
            bool terminal,
            CancellationToken cancellationToken)
        {
            Failures.Add((messageId, terminal));
            return Task.CompletedTask;
        }
    }

    private sealed class FakeProducerFactory(FakeProducer producer) : IKafkaEventProducerFactory
    {
        public IKafkaEventProducer Create(EventPublisherOptions options)
        {
            return producer;
        }
    }

    private sealed class FakeProducer : IKafkaEventProducer
    {
        public List<(string Topic, string Key, string Value)> Messages { get; } = [];

        public bool ThrowOnPublish { get; init; }

        public Task ProduceAsync(
            string topic,
            string key,
            string value,
            CancellationToken cancellationToken)
        {
            if (ThrowOnPublish)
            {
                throw new InvalidOperationException("Publish failed.");
            }

            Messages.Add((topic, key, value));
            return Task.CompletedTask;
        }

        public void Dispose()
        {
        }
    }
}
