using MicroDCS.Core.Messaging;

namespace MicroDCS.Core.Authoring;

/// <summary>
/// Implemented by the generator on every <c>[Processor]</c> class. A static abstract member lets
/// <c>AddProcessor&lt;T&gt;()</c> obtain the descriptor without reflection.
/// </summary>
public interface IDescribedProcessor
{
    /// <summary>Builds the descriptor of the processor.</summary>
    /// <returns>The descriptor.</returns>
    static abstract ProcessorDescriptor Describe();
}

/// <summary>Helpers called by generated invokers. Public because generated code lives in the user's assembly.</summary>
public static class GeneratedSupport
{
    private static readonly IReadOnlyList<Reply> _noReplies = [];

    /// <summary>No replies.</summary>
    /// <returns>An empty list.</returns>
    public static IReadOnlyList<Reply> None() => _noReplies;

    /// <summary>One reply, or none when the handler returned <see langword="null"/>.</summary>
    /// <param name="payload">The payload or <see langword="null"/>.</param>
    /// <returns>The replies.</returns>
    public static IReadOnlyList<Reply> One(object? payload) => payload is null ? _noReplies : [new Reply(payload)];

    /// <summary>Many replies.</summary>
    /// <param name="payloads">The payloads, <see langword="null"/> items are skipped.</param>
    /// <returns>The replies.</returns>
    public static IReadOnlyList<Reply> Many(IEnumerable<object?>? payloads)
    {
        if (payloads is null)
        {
            return _noReplies;
        }

        return payloads.OfType<object>().Select(payload => new Reply(payload)).ToList();
    }

    /// <summary>Many replies from an async sequence.</summary>
    /// <param name="payloads">The payloads.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The replies.</returns>
    public static async ValueTask<IReadOnlyList<Reply>> ManyAsync(IAsyncEnumerable<object?>? payloads, CancellationToken cancellationToken)
    {
        if (payloads is null)
        {
            return _noReplies;
        }

        var list = new List<Reply>();
        await foreach (var payload in payloads.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            if (payload is not null)
            {
                list.Add(new Reply(payload));
            }
        }

        return list;
    }

    /// <summary>The replies of a <see cref="Replies"/> builder.</summary>
    /// <param name="replies">The builder or <see langword="null"/>.</param>
    /// <returns>The replies.</returns>
    public static IReadOnlyList<Reply> From(Replies? replies) => replies is null ? _noReplies : replies;

    /// <summary>Default subscribe intents of a binding (P-2).</summary>
    /// <param name="binding">The binding.</param>
    /// <returns>The intents.</returns>
    public static Intent[] DefaultSubscribe(ProcessorBinding binding) => binding == ProcessorBinding.Northbound
        ? [Intent.Command]
        : [Intent.Data, Intent.Event, Intent.Meta];

    /// <summary>Default publish intents of a binding (P-2).</summary>
    /// <param name="binding">The binding.</param>
    /// <returns>The intents.</returns>
    public static Intent[] DefaultPublish(ProcessorBinding binding) => binding == ProcessorBinding.Northbound
        ? [Intent.Data, Intent.Event, Intent.Meta]
        : [Intent.Command];

    /// <summary>The scope part of a subject: before the first <c>/</c> (P-4).</summary>
    /// <param name="context">The event.</param>
    /// <returns>The scope.</returns>
    /// <exception cref="PayloadException">The event has no subject.</exception>
    public static string Scope(EventContext context) => SubjectPart(context, 0);

    /// <summary>A <c>/</c> separated part of the subject (P-4).</summary>
    /// <param name="context">The event.</param>
    /// <param name="index">Zero based index.</param>
    /// <returns>The part.</returns>
    /// <exception cref="PayloadException">The subject is missing or has no such part.</exception>
    public static string SubjectPart(EventContext context, int index)
    {
        ArgumentNullException.ThrowIfNull(context);
        var parts = context.Subject?.Split('/');
        if (parts is null || index < 0 || index >= parts.Length || parts[index].Length == 0)
        {
            throw new PayloadException($"The subject has no part {index}.");
        }

        return parts[index];
    }
}
