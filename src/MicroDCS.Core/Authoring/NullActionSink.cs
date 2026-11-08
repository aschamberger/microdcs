namespace MicroDCS.Core.Authoring;

/// <summary>The default <see cref="IActionSink"/>: nothing is waiting for actions, so every call does nothing.</summary>
public sealed class NullActionSink : IActionSink
{
    /// <inheritdoc />
    public ValueTask CompleteAsync(string commandId, CancellationToken cancellationToken) => ValueTask.CompletedTask;

    /// <inheritdoc />
    public ValueTask FailAsync(string commandId, CancellationToken cancellationToken) => ValueTask.CompletedTask;

    /// <inheritdoc />
    public ValueTask EventReceivedAsync(EventContext context, CancellationToken cancellationToken) => ValueTask.CompletedTask;

    /// <inheritdoc />
    public ValueTask ScopeDiscoveredAsync(string scope, CancellationToken cancellationToken) => ValueTask.CompletedTask;
}
