# Platform Event Libraries

Reusable .NET 8 event packages for Simplify Yours backend services.

This repository contains framework-level Kafka event publishing and consuming code. Services still own their payload contracts, EF `DbContext`, outbox/inbox tables, migrations, and business handlers.

## Packages

### `SimplifyYours.Event.Abstractions`

Shared framework contracts:

- `IntegrationEventEnvelope`
- `IntegrationEventContext<TPayload>`
- `IEventOutboxStore`
- `IEventInboxStore`
- `IIntegrationEventHandler<TPayload>`
- Outbox/inbox record DTOs and status enums

### `SimplifyYours.Event.Publisher`

Kafka transactional outbox publisher framework.

Responsibilities:

- Poll pending records from `IEventOutboxStore`.
- Serialize `IntegrationEventEnvelope`.
- Publish to Kafka with `Confluent.Kafka`.
- Mark records published or failed through the service-owned store.
- Support batch size, polling interval, and max publish attempts.

### `SimplifyYours.Event.Consumer`

Kafka consumer framework with typed handler dispatch.

Responsibilities:

- Subscribe to configured topics and event types.
- Deserialize envelopes and typed payloads.
- Check inbox idempotency through `IEventInboxStore`.
- Dispatch to `IIntegrationEventHandler<TPayload>`.
- Commit offsets only after successful handling, duplicate skip, or terminal DLQ handling.
- Publish terminal handler failures to `<topic>.dlq`.

## Publisher Usage

```csharp
services.AddSimplifyYoursEventPublisher(options =>
{
    options.BootstrapServers = configuration["Kafka:BootstrapServers"];
    options.DefaultTopic = configuration["Kafka:EventReferenceTopic"];
    options.BatchSize = 25;
    options.PollingInterval = TimeSpan.FromSeconds(2);
    options.MaxPublishAttempts = 5;
});

services.AddScoped<IEventOutboxStore, EventServiceOutboxStore>();
```

## Consumer Usage

```csharp
services.AddSimplifyYoursEventConsumer(options =>
{
    options.BootstrapServers = configuration["Kafka:BootstrapServers"];
    options.GroupId = configuration["Kafka:GroupId"];
    options.Subscribe<EventReferencePayload>(
        configuration["Kafka:EventReferenceTopic"],
        ["EventCreated", "EventUpdated", "EventDeleted"]);
    options.MaxHandleAttempts = 5;
    options.DeadLetterTopicSuffix = ".dlq";
});

services.AddScoped<IEventInboxStore, GuestManagementInboxStore>();
services.AddScoped<IIntegrationEventHandler<EventReferencePayload>, EventReferenceIntegrationEventHandler>();
```

## Configuration

Common Kafka settings used by consuming services:

- `Kafka:BootstrapServers`
- `Kafka:EventReferenceTopic`
- `Kafka:GroupId`
- `Kafka:OutboxBatchSize`
- `Kafka:OutboxPollingIntervalSeconds`
- `Kafka:MaxPublishAttempts`
- `Kafka:MaxHandleAttempts`
- `Kafka:DeadLetterTopicSuffix`

The publisher or consumer hosted service is disabled when required Kafka configuration is incomplete.

## Developer Commands

Run these commands from `code/backend/platform-libraries/`.

### Restore

```bash
dotnet restore SimplifyYours.Event.PlatformLibraries.sln
```

### Build

```bash
dotnet build SimplifyYours.Event.PlatformLibraries.sln --configuration Release --no-restore
```

### Pack

```bash
dotnet pack SimplifyYours.Event.PlatformLibraries.sln --configuration Release --no-build -p:PackageVersion=0.0.0-local --output artifacts
```

## Publishing

Packages publish to GitHub Packages.

The package workflow runs restore, build, and pack validation on pull requests and pushes to `main`. It publishes packages when:

- A tag matching `events-v*` is pushed.
- The workflow is manually dispatched with `packageVersion`.

Tag version example:

```bash
git tag events-v1.0.0
git push origin events-v1.0.0
```

The workflow strips `events-v` and uses the remaining value as `PackageVersion`.

After first publish, configure GitHub Packages visibility/access so Event Service, Guest Management Service, and other backend repositories can restore these packages.

## README Maintenance

Keep this README current when package APIs, configuration keys, publish behavior, developer commands, or supported event framework behavior changes.
