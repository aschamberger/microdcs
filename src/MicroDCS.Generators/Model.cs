using Microsoft.CodeAnalysis;

namespace MicroDCS.Generators;

internal sealed class PayloadInfo
{
    public PayloadInfo(INamedTypeSymbol type, string typeId, string? schema, string fieldName, string? contextAccess, List<CarriedInfo> carried)
    {
        Type = type;
        TypeId = typeId;
        Schema = schema;
        FieldName = fieldName;
        ContextAccess = contextAccess;
        Carried = carried;
    }

    public INamedTypeSymbol Type { get; }

    public string TypeId { get; }

    public string? Schema { get; }

    public string FieldName { get; }

    /// <summary>The JSON type info expression; <see langword="null"/> for an OPC UA JSON payload.</summary>
    public string? ContextAccess { get; }

    public List<CarriedInfo> Carried { get; }
}

internal sealed class CarriedInfo
{
    public CarriedInfo(string attributeName, string propertyName)
    {
        AttributeName = attributeName;
        PropertyName = propertyName;
    }

    public string AttributeName { get; }

    public string PropertyName { get; }
}

internal sealed class HandlerInfo
{
    public HandlerInfo(string kind, PayloadInfo? payload, string invoker)
    {
        Kind = kind;
        Payload = payload;
        Invoker = invoker;
    }

    public string Kind { get; }

    public PayloadInfo? Payload { get; }

    public string Invoker { get; }
}
