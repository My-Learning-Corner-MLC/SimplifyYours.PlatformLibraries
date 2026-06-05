using Confluent.Kafka;

namespace SimplifyYours.Event.Publisher;

public interface IKafkaEventProducer : IDisposable
{
    Task ProduceAsync(
        string topic,
        string key,
        string value,
        CancellationToken cancellationToken);
}

public interface IKafkaEventProducerFactory
{
    IKafkaEventProducer Create(EventPublisherOptions options);
}

internal sealed class KafkaEventProducerFactory : IKafkaEventProducerFactory
{
    public IKafkaEventProducer Create(EventPublisherOptions options)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = options.BootstrapServers
        };

        return new KafkaEventProducer(new ProducerBuilder<string, string>(config).Build());
    }
}

internal sealed class KafkaEventProducer(IProducer<string, string> producer) : IKafkaEventProducer
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
