using System.Text.Json;

namespace SimplifyYours.Event.Abstractions;

public sealed record IntegrationEventEnvelope(
    Guid EventId,
    string EventType,
    DateTimeOffset OccurredAt,
    Guid CorrelationId,
    Guid? CausationId,
    string Payload,
    int Version)
{
    public static IntegrationEventEnvelope Create<TPayload>(
        string eventType,
        TPayload payload,
        DateTimeOffset occurredAt,
        Guid? eventId = null,
        Guid? correlationId = null,
        Guid? causationId = null,
        int version = 1,
        JsonSerializerOptions? serializerOptions = null)
    {
        return new IntegrationEventEnvelope(
            eventId ?? Guid.NewGuid(),
            eventType,
            occurredAt.ToUniversalTime(),
            correlationId ?? Guid.NewGuid(),
            causationId,
            JsonSerializer.Serialize(payload, serializerOptions ?? EventJsonSerializerOptions.Default),
            version);
    }
}
