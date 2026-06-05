namespace SimplifyYours.Event.Abstractions;

public interface IIntegrationEventHandler<TPayload>
{
    Task HandleAsync(IntegrationEventContext<TPayload> context, CancellationToken cancellationToken);
}
