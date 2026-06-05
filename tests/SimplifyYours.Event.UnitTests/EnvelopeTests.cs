using System.Text.Json;
using SimplifyYours.Event.Abstractions;

namespace SimplifyYours.Event.UnitTests;

public sealed class EnvelopeTests
{
    [Fact]
    public void Envelope_SerializesAndDeserializesWithPayload()
    {
        var payload = new TestPayload(Guid.NewGuid(), "Launch");
        var envelope = IntegrationEventEnvelope.Create(
            "EventCreated",
            payload,
            new DateTimeOffset(2026, 6, 4, 10, 0, 0, TimeSpan.Zero));

        var json = JsonSerializer.Serialize(envelope, EventJsonSerializerOptions.Default);
        var deserialized = JsonSerializer.Deserialize<IntegrationEventEnvelope>(
            json,
            EventJsonSerializerOptions.Default);
        var deserializedPayload = JsonSerializer.Deserialize<TestPayload>(
            deserialized!.Payload,
            EventJsonSerializerOptions.Default);

        Assert.Equal(envelope.EventId, deserialized.EventId);
        Assert.Equal("EventCreated", deserialized.EventType);
        Assert.Equal(payload, deserializedPayload);
    }

    private sealed record TestPayload(Guid EventId, string EventName);
}
