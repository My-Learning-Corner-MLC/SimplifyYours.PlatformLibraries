using Microsoft.Extensions.DependencyInjection;

namespace SimplifyYours.Event.Publisher;

public static class DependencyInjection
{
    public static IServiceCollection AddSimplifyYoursEventPublisher(
        this IServiceCollection services,
        Action<EventPublisherOptions> configureOptions)
    {
        services.Configure(configureOptions);
        services.AddSingleton<IKafkaEventProducerFactory, KafkaEventProducerFactory>();
        services.AddHostedService<EventOutboxPublisherBackgroundService>();

        return services;
    }
}
