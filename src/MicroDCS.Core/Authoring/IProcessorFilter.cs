namespace MicroDCS.Core.Authoring;

/// <summary>The rest of the filter chain, ending in the handler.</summary>
/// <param name="cancellationToken">Cancellation.</param>
/// <returns>The replies of the handler, possibly changed by later filters.</returns>
public delegate ValueTask<IReadOnlyList<Reply>> HandlerContinuation(CancellationToken cancellationToken);

/// <summary>
/// Wraps handler invocation (P-9). A filter can skip the handler and supply replies, or change the replies. Replaces the
/// Python <c>__pre_outgoing_callback__</c> and <c>__post_outgoing_callback__</c> hooks, which had misleading names and
/// were found by name, not registered. Filters are registered explicitly per processor or globally.
/// </summary>
public interface IProcessorFilter
{
    /// <summary>Runs the filter.</summary>
    /// <param name="context">The event.</param>
    /// <param name="continuation">The next filter or the handler. Not calling it skips the handler.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The replies.</returns>
    ValueTask<IReadOnlyList<Reply>> InvokeAsync(EventContext context, HandlerContinuation continuation, CancellationToken cancellationToken);
}
