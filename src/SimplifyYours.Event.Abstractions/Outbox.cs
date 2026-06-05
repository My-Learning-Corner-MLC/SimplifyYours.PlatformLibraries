namespace SimplifyYours.Event.Abstractions;

public enum EventOutboxRecordStatus
{
    Pending = 0,
    Published = 1,
    Failed = 2
}

public sealed record EventOutboxRecord(
    Guid Id,
    string EventType,
    DateTimeOffset OccurredAt,
    Guid CorrelationId,
    Guid? CausationId,
    string Payload,
    int Version,
    int PublishAttempts,
    string? Topic = null)
{
    public IntegrationEventEnvelope ToEnvelope()
    {
        return new IntegrationEventEnvelope(
            Id,
            EventType,
            OccurredAt,
            CorrelationId,
            CausationId,
            Payload,
            Version);
    }
}

public interface IEventOutboxStore
{
    Task<IReadOnlyList<EventOutboxRecord>> GetPendingAsync(
        int batchSize,
        int maxPublishAttempts,
        CancellationToken cancellationToken);

    Task MarkPublishedAsync(
        Guid messageId,
        DateTimeOffset publishedAt,
        CancellationToken cancellationToken);

    Task MarkPublishFailedAsync(
        Guid messageId,
        string error,
        bool terminal,
        CancellationToken cancellationToken);
}
