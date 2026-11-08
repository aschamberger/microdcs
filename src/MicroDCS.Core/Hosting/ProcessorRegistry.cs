using MicroDCS.Core.Authoring;
using MicroDCS.Core.Messaging;

namespace MicroDCS.Core.Hosting;

/// <summary>The processors of the application with their handlers and payload descriptors, built from the registered descriptors.</summary>
public sealed class ProcessorRegistry
{
    private readonly Dictionary<string, ProcessorDescriptor> _byBinding = new(StringComparer.Ordinal);
    private readonly Dictionary<Type, IPayloadDescriptor> _byPayloadType = [];
    private readonly Dictionary<Type, SortedSet<string>> _ambiguous = [];

    /// <summary>
    /// Creates the registry. A binding id registered twice is an error (D-27). A CLR payload type with descriptors of different
    /// type ids (for example two <see cref="BinaryPayloadDescriptor"/>s) is ambiguous: handlers still decode it, but an outgoing
    /// event cannot be built from it (<see cref="GetDescriptor"/>).
    /// </summary>
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
                Add(handler);
            }
        }

        foreach (var payload in extraPayloads)
        {
            Add(payload);
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
    /// <returns><see langword="true"/> when found and not ambiguous.</returns>
    public bool TryGetDescriptor(Type payloadType, out IPayloadDescriptor descriptor) =>
        _byPayloadType.TryGetValue(payloadType, out descriptor!) && !_ambiguous.ContainsKey(payloadType);

    /// <summary>The descriptor an outgoing event of this CLR type is encoded with.</summary>
    /// <param name="payloadType">The CLR type.</param>
    /// <returns>The descriptor.</returns>
    /// <exception cref="InvalidOperationException">No descriptor is registered, or more than one type id uses the CLR type.</exception>
    public IPayloadDescriptor GetDescriptor(Type payloadType)
    {
        ArgumentNullException.ThrowIfNull(payloadType);
        if (_ambiguous.TryGetValue(payloadType, out var typeIds))
        {
            throw new InvalidOperationException($"The payload type '{payloadType.Name}' is registered for the event types {string.Join(", ", typeIds)}; an outgoing event cannot pick one. Publish an EventEnvelope or use a distinct CLR type per event type.");
        }

        return _byPayloadType.TryGetValue(payloadType, out var descriptor)
            ? descriptor
            : throw new InvalidOperationException($"No payload descriptor for type '{payloadType.Name}'. Register it with AddPayloads.");
    }

    private void Add(IPayloadDescriptor descriptor)
    {
        if (!_byPayloadType.TryAdd(descriptor.PayloadType, descriptor))
        {
            var existing = _byPayloadType[descriptor.PayloadType];
            if (!string.Equals(existing.TypeId, descriptor.TypeId, StringComparison.Ordinal))
            {
                if (!_ambiguous.TryGetValue(descriptor.PayloadType, out var typeIds))
                {
                    typeIds = new SortedSet<string>(StringComparer.Ordinal) { existing.TypeId };
                    _ambiguous[descriptor.PayloadType] = typeIds;
                }

                typeIds.Add(descriptor.TypeId);
            }
        }
    }

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
