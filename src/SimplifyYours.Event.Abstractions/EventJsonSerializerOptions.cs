using System.Text.Json;

namespace SimplifyYours.Event.Abstractions;

public static class EventJsonSerializerOptions
{
    public static JsonSerializerOptions Default { get; } = new(JsonSerializerDefaults.Web);
}
