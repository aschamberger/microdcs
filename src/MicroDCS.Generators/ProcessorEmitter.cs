using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace MicroDCS.Generators;

/// <summary>Analyses one <c>[Processor]</c> class and emits its <c>IDescribedProcessor</c> part.</summary>
internal static class ProcessorEmitter
{
    private const string Core = "global::MicroDCS.Core.";
    private const string Di = "global::Microsoft.Extensions.DependencyInjection.ServiceProviderServiceExtensions.GetRequiredService";

    private static readonly Dictionary<string, (string Kind, string Display)> _handlerAttributes = new(StringComparer.Ordinal)
    {
        [Names.Incoming] = ("Incoming", "[Incoming]"),
        [Names.Outgoing] = ("Outgoing", "[Outgoing]"),
        [Names.OnResponse] = ("Response", "[OnResponse]"),
        [Names.OnExpired] = ("Expired", "[OnExpired]"),
        [Names.Fallback] = ("Fallback", "[Fallback]"),
    };

    private static readonly string[] _lifecycleAttributes = [Names.OnInitialize, Names.OnStarted, Names.OnShutdown];

    public static string? Emit(INamedTypeSymbol type, PayloadAnalyzer payloads, Action<Diagnostic> report)
    {
        var location = type.Locations.FirstOrDefault();
        var partial = type.DeclaringSyntaxReferences.Any(r => r.GetSyntax() is ClassDeclarationSyntax c && c.Modifiers.Any(m => m.Text == "partial"));
        if (!partial || type.IsGenericType || type.ContainingType is not null)
        {
            report(Diagnostic.Create(Diagnostics.ProcessorNotPartial, location, type.Name));
            return null;
        }

        // An argument that does not resolve (for example a missing using) leaves the constructor arguments empty.
        if (PayloadAnalyzer.Find(type, Names.Processor) is not { ConstructorArguments.Length: > 0 } processorAttribute || processorAttribute.ConstructorArguments[0].Value is not int)
        {
            report(Diagnostic.Create(Diagnostics.InvalidProcessorAttribute, location, type.Name));
            return null;
        }

        var failed = false;
        var replyFields = new SortedSet<string>(StringComparer.Ordinal);
        var handlers = new List<string>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        string? initialize = null, started = null, shutdown = null;
        var singleton = false;

        foreach (var method in type.GetMembers().OfType<IMethodSymbol>().Where(m => m.MethodKind == MethodKind.Ordinary))
        {
            var emitted = TryHandler(type, method, payloads, report, seen, replyFields, out var handler);
            failed |= emitted == Outcome.Failed;
            if (emitted == Outcome.Emitted)
            {
                handlers.Add(handler);
                continue;
            }

            if (_lifecycleAttributes.FirstOrDefault(n => PayloadAnalyzer.Find(method, n) is not null) is { } lifecycle)
            {
                var lambda = EmitLifecycle(type, method, report);
                failed |= lambda is null;
                if (!seen.Add("lifecycle:" + lifecycle))
                {
                    report(Diagnostic.Create(Diagnostics.DuplicateHandler, method.Locations.FirstOrDefault(), type.Name, lifecycle, string.Empty));
                    failed = true;
                }

                switch (lifecycle)
                {
                    case Names.OnInitialize: initialize = lambda; break;
                    case Names.OnStarted:
                        started = lambda;
                        singleton = PayloadAnalyzer.Find(method, Names.OnStarted)!.NamedArguments.Any(a => a.Key == "Singleton" && a.Value.Value is true);
                        break;
                    default: shutdown = lambda; break;
                }
            }
        }

        return failed ? null : Render(type, handlers, initialize, started, singleton, shutdown, replyFields);
    }

    private enum Outcome
    {
        NotAHandler,
        Emitted,
        Failed,
    }

    private static Outcome TryHandler(INamedTypeSymbol type, IMethodSymbol method, PayloadAnalyzer payloads, Action<Diagnostic> report, HashSet<string> seen, SortedSet<string> replyFields, out string descriptor)
    {
        descriptor = string.Empty;
        var attribute = method.GetAttributes().FirstOrDefault(a => a.AttributeClass is not null && _handlerAttributes.ContainsKey(a.AttributeClass.ToDisplayString()));
        if (attribute is null)
        {
            return Outcome.NotAHandler;
        }

        var (kind, display) = _handlerAttributes[attribute.AttributeClass!.ToDisplayString()];
        var location = method.Locations.FirstOrDefault();
        var isOutgoing = kind == "Outgoing";
        var isFallback = kind == "Fallback";

        // The input: payload type (or the payload type named by [Outgoing]) and the first parameter type.
        PayloadInfo? payload = null;
        var expectedFirst = "a [CloudEventType] payload";
        if (isOutgoing)
        {
            expectedFirst = "CommandRequest";
        }
        else if (isFallback)
        {
            expectedFirst = "EventEnvelope";
        }

        var first = method.Parameters.FirstOrDefault();
        if (first is null || !FirstParameterMatches(first.Type, isOutgoing, isFallback))
        {
            report(Diagnostic.Create(Diagnostics.NoPayloadParameter, location, method.Name, expectedFirst));
            return Outcome.Failed;
        }

        if (isOutgoing)
        {
            payload = payloads.Lookup((ITypeSymbol)attribute.ConstructorArguments[0].Value!, location);
        }
        else if (!isFallback)
        {
            payload = payloads.Lookup(first.Type, location);
        }

        if ((isOutgoing || !isFallback) && payload is null)
        {
            return Outcome.Failed;
        }

        var key = kind + ":" + (payload?.TypeId ?? "*");
        if (!seen.Add(key))
        {
            report(Diagnostic.Create(Diagnostics.DuplicateHandler, location, type.Name, display, payload?.TypeId ?? "fallback"));
            return Outcome.Failed;
        }

        var invoker = EmitInvoker(type, method, report, out var item);
        if (invoker is null)
        {
            return Outcome.Failed;
        }

        if (item is not null && payloads.Lookup(item, location) is { } replyPayload)
        {
            replyFields.Add(replyPayload.FieldName);
        }

        var payloadExpr = payload is null ? "null" : "global::MicroDcsPayloads." + payload.FieldName;
        var typeId = payload is null ? "null" : Literal(payload.TypeId);
        descriptor = $"new {Core}Authoring.HandlerDescriptor({Core}Authoring.HandlerKind.{kind}, {typeId}, {payloadExpr}, {invoker}{OutcomesArgument(attribute)})";
        return Outcome.Emitted;
    }

    private static string OutcomesArgument(AttributeData attribute)
    {
        var values = attribute.NamedArguments.FirstOrDefault(a => a.Key == "Outcomes").Value;
        return values.Kind != TypedConstantKind.Array || values.Values.Length == 0
            ? string.Empty
            : ", global::System.Collections.Immutable.ImmutableArray.Create<string>(" + string.Join(", ", values.Values.Select(v => Literal((string)v.Value!))) + ")";
    }

    private static bool FirstParameterMatches(ITypeSymbol type, bool isOutgoing, bool isFallback)
    {
        var name = type.ToDisplayString();
        if (isOutgoing)
        {
            return name == Names.CommandRequest;
        }

        if (isFallback)
        {
            return name == Names.EventEnvelope;
        }

        return !type.IsValueType && type.SpecialType == SpecialType.None;
    }

    private static string? EmitInvoker(INamedTypeSymbol type, IMethodSymbol method, Action<Diagnostic> report, out ITypeSymbol? item)
    {
        item = null;
        var args = BindParameters(method, report, inputIndex: 0);
        if (args is null)
        {
            return null;
        }

        var shape = ReturnShape.Analyse(method.ReturnType);
        if (shape is null)
        {
            report(Diagnostic.Create(Diagnostics.UnsupportedReturn, method.Locations.FirstOrDefault(), method.Name));
            return null;
        }

        item = shape.Item;
        var typeName = type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
        var call = $"{Target(method, typeName)}.{method.Name}({string.Join(", ", args)})";
        var sb = new StringBuilder();
        sb.Append(shape.IsAsync ? "async " : string.Empty);
        sb.Append("(sp, ctx, input, ct) => ");
        if (shape.IsAsync)
        {
            sb.Append("{ ");
            sb.Append(AwaitStatement(shape, call));
            sb.Append($"return {shape.Convert("r")}; }}");
        }
        else
        {
            sb.Append(shape.IsVoid
                ? $"{{ {call}; return new global::System.Threading.Tasks.ValueTask<global::System.Collections.Generic.IReadOnlyList<{Core}Authoring.Reply>>({Core}Authoring.GeneratedSupport.None()); }}"
                : $"new global::System.Threading.Tasks.ValueTask<global::System.Collections.Generic.IReadOnlyList<{Core}Authoring.Reply>>({shape.Convert(call)})");
        }

        return sb.ToString();
    }

    // Static handlers are called on the type, instance handlers on the processor from the service provider.
    private static string Target(IMethodSymbol method, string typeName) =>
        method.IsStatic ? typeName : $"{Di}<{typeName}>(sp)";

    private static string AwaitStatement(ReturnShape shape, string call)
    {
        if (!shape.Awaited)
        {
            return $"var r = {call}; ";
        }

        return shape.IsVoid ? $"await {call}; " : $"var r = await {call}; ";
    }

    private static string? EmitLifecycle(INamedTypeSymbol type, IMethodSymbol method, Action<Diagnostic> report)
    {
        var args = BindParameters(method, report, inputIndex: -1);
        if (args is null)
        {
            return null;
        }

        var name = method.ReturnType.ToDisplayString();
        var returnsTask = name is "System.Threading.Tasks.Task" or "System.Threading.Tasks.ValueTask";
        if (!returnsTask && !method.ReturnsVoid)
        {
            report(Diagnostic.Create(Diagnostics.UnsupportedReturn, method.Locations.FirstOrDefault(), method.Name));
            return null;
        }

        var call = $"{Target(method, type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat))}.{method.Name}({string.Join(", ", args)})";
        var valueTask = "global::System.Threading.Tasks.ValueTask";
        return name switch
        {
            "System.Threading.Tasks.Task" => $"(sp, ct) => new {valueTask}({call})",
            "System.Threading.Tasks.ValueTask" => $"(sp, ct) => {call}",
            _ => $"(sp, ct) => {{ {call}; return default({valueTask}); }}",
        };
    }

    private static List<string>? BindParameters(IMethodSymbol method, Action<Diagnostic> report, int inputIndex)
    {
        var args = new List<string>();
        for (var i = 0; i < method.Parameters.Length; i++)
        {
            var parameter = method.Parameters[i];
            var typeName = parameter.Type.ToDisplayString();
            var full = parameter.Type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
            string? arg;
            if (i == inputIndex)
            {
                arg = $"({full})input!";
            }
            else if (typeName == Names.EventContext)
            {
                arg = "ctx";
            }
            else if (typeName == Names.CancellationToken)
            {
                arg = "ct";
            }
            else if (PayloadAnalyzer.Find(parameter, Names.Scope) is not null && parameter.Type.SpecialType == SpecialType.System_String)
            {
                arg = $"{Core}Authoring.GeneratedSupport.Scope(ctx)";
            }
            else if (PayloadAnalyzer.Find(parameter, Names.SubjectPart) is { } part && parameter.Type.SpecialType == SpecialType.System_String)
            {
                arg = $"{Core}Authoring.GeneratedSupport.SubjectPart(ctx, {part.ConstructorArguments[0].Value})";
            }
            else if (typeName == "System.IServiceProvider")
            {
                arg = "sp";
            }
            else if (!parameter.Type.IsValueType && parameter.Type.SpecialType == SpecialType.None && parameter.Type.TypeKind is TypeKind.Interface or TypeKind.Class)
            {
                arg = $"{Di}<{full}>(sp)";
            }
            else
            {
                report(Diagnostic.Create(Diagnostics.UnsupportedParameter, parameter.Locations.FirstOrDefault(), parameter.Name, method.Name));
                return null;
            }

            args.Add(arg);
        }

        return args;
    }

    private static string Render(INamedTypeSymbol type, List<string> handlers, string? initialize, string? started, bool singleton, string? shutdown, SortedSet<string> replyFields)
    {
        var attribute = PayloadAnalyzer.Find(type, Names.Processor)!;
        var binding = (int)attribute.ConstructorArguments[0].Value!;
        var bindingName = binding == 0 ? "Northbound" : "Southbound";
        var id = attribute.NamedArguments.FirstOrDefault(a => a.Key == "Id").Value.Value as string ?? DefaultId(type.Name);
        var bindingExpr = $"{Core}Messaging.ProcessorBinding.{bindingName}";
        var subscribe = IntentArray(attribute, "SubscribeIntents", $"{Core}Authoring.GeneratedSupport.DefaultSubscribe({bindingExpr})");
        var publish = IntentArray(attribute, "PublishIntents", $"{Core}Authoring.GeneratedSupport.DefaultPublish({bindingExpr})");
        var typeName = type.Name;

        var sb = new StringBuilder();
        sb.AppendLine("// <auto-generated/>");
        sb.AppendLine("#nullable enable");
        if (!type.ContainingNamespace.IsGlobalNamespace)
        {
            sb.AppendLine($"namespace {type.ContainingNamespace.ToDisplayString()};");
        }

        sb.AppendLine($"partial class {typeName} : {Core}Authoring.IDescribedProcessor");
        sb.AppendLine("{");
        sb.AppendLine("    /// <inheritdoc />");
        sb.AppendLine($"    public static {Core}Authoring.ProcessorDescriptor Describe() => new(");
        sb.AppendLine($"        typeof({typeName}), {Literal(id)}, {bindingExpr},");
        sb.AppendLine($"        global::System.Collections.Immutable.ImmutableArray.Create<{Core}Messaging.Intent>({subscribe}),");
        sb.AppendLine($"        global::System.Collections.Immutable.ImmutableArray.Create<{Core}Messaging.Intent>({publish}),");
        sb.AppendLine($"        {HandlerArray(handlers)},");
        sb.AppendLine($"        new {Core}Authoring.LifecycleDescriptor({initialize ?? "null"}, {started ?? "null"}, {(singleton ? "true" : "false")}, {shutdown ?? "null"}),");
        sb.AppendLine($"        {ReplyArray(replyFields)});");
        sb.AppendLine("}");
        return sb.ToString();
    }

    private static string ReplyArray(SortedSet<string> fields) => fields.Count == 0
        ? $"global::System.Collections.Immutable.ImmutableArray<{Core}Messaging.IPayloadDescriptor>.Empty"
        : $"global::System.Collections.Immutable.ImmutableArray.Create<{Core}Messaging.IPayloadDescriptor>({string.Join(", ", fields.Select(f => "global::MicroDcsPayloads." + f))})";

    private static string HandlerArray(List<string> handlers) => handlers.Count == 0
        ? $"global::System.Collections.Immutable.ImmutableArray<{Core}Authoring.HandlerDescriptor>.Empty"
        : $"global::System.Collections.Immutable.ImmutableArray.Create<{Core}Authoring.HandlerDescriptor>({string.Join(",\n            ", handlers)})";

    private static string IntentArray(AttributeData attribute, string name, string fallback)
    {
        var values = attribute.NamedArguments.FirstOrDefault(a => a.Key == name).Value;
        if (values.Kind != TypedConstantKind.Array || values.Values.Length == 0)
        {
            return fallback;
        }

        return "new " + Core + "Messaging.Intent[] { " + string.Join(", ", values.Values.Select(v => $"({Core}Messaging.Intent){v.Value}")) + " }";
    }

    private static string DefaultId(string className)
    {
        var name = className.EndsWith("Processor", StringComparison.Ordinal) && className.Length > "Processor".Length
            ? className.Substring(0, className.Length - "Processor".Length)
            : className;
        var sb = new StringBuilder();
        for (var i = 0; i < name.Length; i++)
        {
            if (char.IsUpper(name[i]) && i > 0)
            {
                sb.Append('-');
            }

            sb.Append(char.ToLowerInvariant(name[i]));
        }

        return sb.ToString();
    }

    private static string Literal(string value) => MicroDcsGenerator.Literal(value);
}

/// <summary>The return shape of a handler: awaitable, and how its value becomes replies.</summary>
internal sealed class ReturnShape
{
    private const string Support = "global::MicroDCS.Core.Authoring.GeneratedSupport.";
    private readonly string _kind;

    private ReturnShape(string kind, bool awaited, bool isVoid, ITypeSymbol? item = null)
    {
        _kind = kind;
        Awaited = awaited;
        IsVoid = isVoid;
        Item = item;
    }

    /// <summary>The payload type of the replies, or <see langword="null"/> when none is statically known.</summary>
    public ITypeSymbol? Item { get; }

    public bool Awaited { get; }

    public bool IsVoid { get; }

    public bool IsAsync => Awaited || _kind == "asyncmany";

    public string Convert(string expr) => _kind switch
    {
        "one" => Support + "One(" + expr + ")",
        "many" => Support + "Many(" + expr + ")",
        "asyncmany" => "await " + Support + "ManyAsync(" + expr + ", ct)",
        "replies" => Support + "From(" + expr + ")",
        _ => Support + "None()",
    };

    public static ReturnShape? Analyse(ITypeSymbol type)
    {
        var name = type.OriginalDefinition.ToDisplayString();
        if (type.SpecialType == SpecialType.System_Void)
        {
            return new ReturnShape("none", false, true);
        }

        switch (name)
        {
            case "System.Threading.Tasks.Task":
            case "System.Threading.Tasks.ValueTask":
                return new ReturnShape("none", true, true);
            case "System.Threading.Tasks.Task<TResult>":
            case "System.Threading.Tasks.ValueTask<TResult>":
                var inner = Analyse(((INamedTypeSymbol)type).TypeArguments[0]);
                return inner is null || inner.Awaited ? null : new ReturnShape(inner._kind, true, false, inner.Item);
            case "System.Collections.Generic.IEnumerable<T>":
                return IsPayloadClass(((INamedTypeSymbol)type).TypeArguments[0]) ? new ReturnShape("many", false, false, ((INamedTypeSymbol)type).TypeArguments[0]) : null;
            case "System.Collections.Generic.IAsyncEnumerable<T>":
                return IsPayloadClass(((INamedTypeSymbol)type).TypeArguments[0]) ? new ReturnShape("asyncmany", false, false, ((INamedTypeSymbol)type).TypeArguments[0]) : null;
        }

        if (type.ToDisplayString() == Names.Replies)
        {
            return new ReturnShape("replies", false, false);
        }

        if (type.ToDisplayString() == Names.ActionOutcome)
        {
            // The outcome of a response handler; the pipeline takes it out of the replies (D-44).
            return new ReturnShape("one", false, false);
        }

        if (type.ToDisplayString() == Names.EventEnvelope)
        {
            // A prebuilt envelope is published as is (the raw echo of a fallback, P-10).
            return new ReturnShape("one", false, false);
        }

        return IsPayloadClass(type) ? new ReturnShape("one", false, false, type) : null;
    }

    private static bool IsPayloadClass(ITypeSymbol type) => !type.IsValueType && PayloadAnalyzer.IsPayload(type);
}
