namespace SimplifyYours.Event.Consumer;

public sealed class EventConsumerOptions
{
    private readonly List<EventSubscription> subscriptions = [];

    public string? BootstrapServers { get; set; }

    public string? GroupId { get; set; }

    public int MaxHandleAttempts { get; set; } = 5;

    public string DeadLetterTopicSuffix { get; set; } = ".dlq";

    public IReadOnlyList<EventSubscription> Subscriptions => subscriptions;

    public void Subscribe<TPayload>(string? topic, IReadOnlyCollection<string> eventTypes)
    {
        if (string.IsNullOrWhiteSpace(topic))
        {
            return;
        }

        subscriptions.Add(new EventSubscription(
            topic,
            typeof(TPayload),
            eventTypes
                .Where(eventType => !string.IsNullOrWhiteSpace(eventType))
                .ToHashSet(StringComparer.Ordinal)));
    }
}

public sealed record EventSubscription(
    string Topic,
    Type PayloadType,
    IReadOnlySet<string> EventTypes);
