namespace SimplifyYours.Event.Abstractions;

public enum EventInboxRecordStatus
{
    Processing = 0,
    Processed = 1,
    Failed = 2
}

public sealed record EventInboxRecord(
    Guid Id,
    string EventType,
    DateTimeOffset ReceivedAt,
    DateTimeOffset? ProcessedAt,
    int HandleAttempts,
    EventInboxRecordStatus Status,
    string? Error);

public interface IEventInboxStore
{
    Task<EventInboxRecord?> GetAsync(Guid messageId, CancellationToken cancellationToken);

    Task MarkProcessingAsync(
        IntegrationEventEnvelope envelope,
        DateTimeOffset receivedAt,
        CancellationToken cancellationToken);

    Task MarkProcessedAsync(
        Guid messageId,
        string eventType,
        DateTimeOffset processedAt,
        CancellationToken cancellationToken);

    Task MarkFailedAsync(
        Guid messageId,
        string eventType,
        string error,
        bool terminal,
        CancellationToken cancellationToken);
}
