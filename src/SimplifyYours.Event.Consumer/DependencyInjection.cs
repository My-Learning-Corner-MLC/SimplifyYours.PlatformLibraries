using Microsoft.Extensions.DependencyInjection;

namespace SimplifyYours.Event.Consumer;

public static class DependencyInjection
{
    public static IServiceCollection AddSimplifyYoursEventConsumer(
        this IServiceCollection services,
        Action<EventConsumerOptions> configureOptions)
    {
        services.Configure(configureOptions);
        services.AddSingleton<IKafkaEventConsumerFactory, KafkaEventConsumerFactory>();
        services.AddSingleton<IKafkaDeadLetterProducerFactory, KafkaDeadLetterProducerFactory>();
        services.AddHostedService<EventConsumerBackgroundService>();

        return services;
    }
}
