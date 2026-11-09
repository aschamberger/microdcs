namespace MicroDCS.Core.Authoring;

/// <summary>The default <see cref="IActionSink"/>: nothing is waiting for actions, so every call does nothing.</summary>
public sealed class NullActionSink : IActionSink
{
    /// <inheritdoc />
    public ValueTask CompleteAsync(ActionResponse response, CancellationToken cancellationToken) => ValueTask.CompletedTask;

    /// <inheritdoc />
    public ValueTask EventReceivedAsync(EventContext context, CancellationToken cancellationToken) => ValueTask.CompletedTask;
}
