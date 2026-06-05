namespace SimplifyYours.Event.Abstractions;

public sealed record IntegrationEventContext<TPayload>(
    IntegrationEventEnvelope Envelope,
    TPayload Payload,
    string Topic,
    string? Key);
