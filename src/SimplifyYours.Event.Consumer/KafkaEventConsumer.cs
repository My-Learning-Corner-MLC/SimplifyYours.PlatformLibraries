using Confluent.Kafka;

namespace SimplifyYours.Event.Consumer;

public sealed record ConsumedEventMessage(
    string Topic,
    string? Key,
    string Value);

public interface IKafkaEventConsumer : IDisposable
{
    void Subscribe(IEnumerable<string> topics);

    ConsumedEventMessage Consume(CancellationToken cancellationToken);

    void Commit();

    void Close();
}

public interface IKafkaEventConsumerFactory
{
    IKafkaEventConsumer Create(EventConsumerOptions options);
}

internal sealed class KafkaEventConsumerFactory : IKafkaEventConsumerFactory
{
    public IKafkaEventConsumer Create(EventConsumerOptions options)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = options.BootstrapServers,
            GroupId = options.GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        return new KafkaEventConsumer(new ConsumerBuilder<string, string>(config).Build());
    }
}

internal sealed class KafkaEventConsumer(IConsumer<string, string> consumer) : IKafkaEventConsumer
{
    private ConsumeResult<string, string>? currentResult;

    public void Subscribe(IEnumerable<string> topics)
    {
        consumer.Subscribe(topics);
    }

    public ConsumedEventMessage Consume(CancellationToken cancellationToken)
    {
        currentResult = consumer.Consume(cancellationToken);

        return new ConsumedEventMessage(
            currentResult.Topic,
            currentResult.Message.Key,
            currentResult.Message.Value);
    }

    public void Commit()
    {
        if (currentResult is not null)
        {
            consumer.Commit(currentResult);
        }
    }

    public void Close()
    {
        consumer.Close();
    }

    public void Dispose()
    {
        consumer.Dispose();
    }
}
