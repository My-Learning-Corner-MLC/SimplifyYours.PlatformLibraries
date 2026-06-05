namespace SimplifyYours.Event.Publisher;

public sealed class EventPublisherOptions
{
    public string? BootstrapServers { get; set; }

    public string? DefaultTopic { get; set; }

    public int BatchSize { get; set; } = 25;

    public TimeSpan PollingInterval { get; set; } = TimeSpan.FromSeconds(5);

    public int MaxPublishAttempts { get; set; } = 5;
}
