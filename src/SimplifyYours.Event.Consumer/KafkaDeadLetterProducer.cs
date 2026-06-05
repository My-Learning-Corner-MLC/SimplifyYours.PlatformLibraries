using Confluent.Kafka;

namespace SimplifyYours.Event.Consumer;

public interface IKafkaDeadLetterProducer : IDisposable
{
    Task ProduceAsync(
        string topic,
        string key,
        string value,
        CancellationToken cancellationToken);
}

public interface IKafkaDeadLetterProducerFactory
{
    IKafkaDeadLetterProducer Create(EventConsumerOptions options);
}

internal sealed class KafkaDeadLetterProducerFactory : IKafkaDeadLetterProducerFactory
{
    public IKafkaDeadLetterProducer Create(EventConsumerOptions options)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = options.BootstrapServers
        };

        return new KafkaDeadLetterProducer(new ProducerBuilder<string, string>(config).Build());
    }
}

internal sealed class KafkaDeadLetterProducer(IProducer<string, string> producer) : IKafkaDeadLetterProducer
{
    public async Task ProduceAsync(
        string topic,
        string key,
        string value,
        CancellationToken cancellationToken)
    {
        await producer.ProduceAsync(
            topic,
            new Message<string, string>
            {
                Key = key,
                Value = value
            },
            cancellationToken);
    }

    public void Dispose()
    {
        producer.Dispose();
    }
}
