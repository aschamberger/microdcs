using MicroDCS.Core.Authoring;
using MicroDCS.Core.Messaging;

namespace MicroDCS.Core.Hosting;

/// <summary>The processors of the application with their handlers and payload descriptors, built from the registered descriptors.</summary>
public sealed class ProcessorRegistry
{
    private readonly Dictionary<string, ProcessorDescriptor> _byBinding = new(StringComparer.Ordinal);
    private readonly Dictionary<Type, IPayloadDescriptor> _byPayloadType = [];

    /// <summary>Creates the registry. A binding id or payload type registered twice is an error (D-27).</summary>
    /// <param name="processors">The processor descriptors.</param>
    /// <param name="extraPayloads">Further payload descriptors, for example for <see cref="Replies"/> of mixed types.</param>
    public ProcessorRegistry(IEnumerable<ProcessorDescriptor> processors, IEnumerable<IPayloadDescriptor> extraPayloads)
    {
        ArgumentNullException.ThrowIfNull(processors);
        ArgumentNullException.ThrowIfNull(extraPayloads);

        foreach (var processor in processors)
        {
            if (!_byBinding.TryAdd(processor.BindingId, processor))
            {
                throw new InvalidOperationException($"Binding '{processor.BindingId}' is used by more than one processor.");
            }

            foreach (var handler in processor.Handlers.Select(h => h.Payload).OfType<IPayloadDescriptor>()
                .Concat(processor.ReplyPayloads.IsDefault ? [] : processor.ReplyPayloads))
            {
                _byPayloadType[handler.PayloadType] = handler;
            }
        }

        foreach (var payload in extraPayloads)
        {
            _byPayloadType[payload.PayloadType] = payload;
        }
    }

    /// <summary>All processors.</summary>
    public IReadOnlyCollection<ProcessorDescriptor> Processors => _byBinding.Values;

    /// <summary>Finds the processor of a binding.</summary>
    /// <param name="bindingId">The binding id.</param>
    /// <param name="processor">The processor.</param>
    /// <returns><see langword="true"/> when found.</returns>
    public bool TryGetProcessor(string bindingId, out ProcessorDescriptor processor) => _byBinding.TryGetValue(bindingId, out processor!);

    /// <summary>Finds the descriptor for a payload object.</summary>
    /// <param name="payloadType">The CLR type.</param>
    /// <param name="descriptor">The descriptor.</param>
    /// <returns><see langword="true"/> when found.</returns>
    public bool TryGetDescriptor(Type payloadType, out IPayloadDescriptor descriptor) => _byPayloadType.TryGetValue(payloadType, out descriptor!);

    /// <summary>Checks that every processor has a configured binding (T-21).</summary>
    /// <param name="options">The options.</param>
    /// <exception cref="InvalidOperationException">A processor has no binding section.</exception>
    public void EnsureBindingsConfigured(MicroDcsOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var missing = _byBinding.Values.FirstOrDefault(p => !options.Bindings.ContainsKey(p.BindingId));
        if (missing is not null)
        {
            throw new InvalidOperationException($"Processor '{missing.ProcessorType.Name}' needs the configuration section 'MicroDcs:Bindings:{missing.BindingId}'.");
        }
    }
}
