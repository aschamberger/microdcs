using System.Text.RegularExpressions;
using Microsoft.CodeAnalysis;

namespace MicroDCS.Generators;

/// <summary>Finds the <c>[CloudEventType]</c> payloads of a compilation and checks them.</summary>
internal sealed class PayloadAnalyzer
{
    private static readonly Regex _attributeNamePattern = new("^[a-z0-9]{1,20}$", RegexOptions.CultureInvariant, TimeSpan.FromSeconds(1));

    private readonly Dictionary<INamedTypeSymbol, PayloadInfo> _payloads = new(SymbolEqualityComparer.Default);
    private readonly Action<Diagnostic> _report;
    private readonly Dictionary<INamedTypeSymbol, string> _jsonAccess = new(SymbolEqualityComparer.Default);

    public PayloadAnalyzer(IEnumerable<INamedTypeSymbol> candidates, Action<Diagnostic> report)
    {
        _report = report;
        var types = candidates.ToList();
        foreach (var context in types.Where(t => Inherits(t, Names.JsonContext)))
        {
            CollectJsonAccess(context);
        }

        var seenIds = new Dictionary<string, INamedTypeSymbol>(StringComparer.Ordinal);
        foreach (var type in types)
        {
            var attribute = Find(type, Names.CloudEventType);
            if (attribute is null)
            {
                continue;
            }

            var typeId = (string)attribute.ConstructorArguments[0].Value!; // The attribute constructor requires the id.
            if (seenIds.ContainsKey(typeId))
            {
                report(Diagnostic.Create(Diagnostics.DuplicateTypeId, type.Locations.FirstOrDefault(), typeId));
                continue;
            }

            seenIds[typeId] = type;
            var info = Build(type, typeId, attribute);
            if (info is not null)
            {
                _payloads[type] = info;
            }
        }
    }

    public IEnumerable<PayloadInfo> Payloads => _payloads.Values.OrderBy(p => p.FieldName, StringComparer.Ordinal);

    public static bool IsPayload(ITypeSymbol type) => type is INamedTypeSymbol named && Find(named, Names.CloudEventType) is not null;

    public PayloadInfo? Lookup(ITypeSymbol type, Location? location)
    {
        if (type is not INamedTypeSymbol named || Find(named, Names.CloudEventType) is null)
        {
            _report(Diagnostic.Create(Diagnostics.PayloadWithoutAttribute, location, type.ToDisplayString()));
            return null;
        }

        // A null result without its own diagnostic means the payload was rejected above and already reported.
        return _payloads.TryGetValue(named, out var info) ? info : null;
    }

    public static AttributeData? Find(ISymbol symbol, string attributeName) =>
        symbol.GetAttributes().FirstOrDefault(a => a.AttributeClass?.ToDisplayString() == attributeName);

    private static bool Inherits(INamedTypeSymbol type, string baseName)
    {
        for (var current = type.BaseType; current is not null; current = current.BaseType)
        {
            if (current.ToDisplayString() == baseName)
            {
                return true;
            }
        }

        return false;
    }

    private void CollectJsonAccess(INamedTypeSymbol context)
    {
        foreach (var attribute in context.GetAttributes().Where(a => a.AttributeClass?.ToDisplayString() == Names.JsonSerializable))
        {
            if (attribute.ConstructorArguments.Length == 0 || attribute.ConstructorArguments[0].Value is not INamedTypeSymbol target)
            {
                continue;
            }

            var propertyName = attribute.NamedArguments.FirstOrDefault(a => a.Key == "TypeInfoPropertyName").Value.Value as string ?? target.Name;
            _jsonAccess[target] = context.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat) + ".Default." + propertyName;
        }
    }

    private PayloadInfo? Build(INamedTypeSymbol type, string typeId, AttributeData attribute)
    {
        var location = type.Locations.FirstOrDefault();
        if (type.IsValueType || type.IsGenericType || type.ContainingType is not null)
        {
            _report(Diagnostic.Create(Diagnostics.PayloadNotSupported, location, type.ToDisplayString()));
            return null;
        }

        var codec = attribute.NamedArguments.FirstOrDefault(a => a.Key == "Codec").Value.Value;
        if (codec is int and not 0)
        {
            _report(Diagnostic.Create(Diagnostics.CodecNotGenerated, location, type.ToDisplayString()));
            return null;
        }

        if (!_jsonAccess.TryGetValue(type, out var access))
        {
            _report(Diagnostic.Create(Diagnostics.NotInJsonContext, location, type.ToDisplayString()));
            return null;
        }

        var carried = new List<CarriedInfo>();
        foreach (var property in type.GetMembers().OfType<IPropertySymbol>())
        {
            if (Find(property, Names.CarriedAs) is { } carriedAttribute)
            {
                var name = carriedAttribute.ConstructorArguments[0].Value as string ?? string.Empty;
                if (!IsValidCarried(property, name))
                {
                    _report(Diagnostic.Create(Diagnostics.InvalidCarriedAs, property.Locations.FirstOrDefault(), property.Name));
                    return null;
                }

                carried.Add(new CarriedInfo(name, property.Name));
            }
        }

        var schema = attribute.NamedArguments.FirstOrDefault(a => a.Key == "Schema").Value.Value as string;
        var field = type.ToDisplayString().Replace('.', '_');
        return new PayloadInfo(type, typeId, schema, field, access, carried);
    }

    private static bool IsValidCarried(IPropertySymbol property, string name) =>
        property.Type.SpecialType == SpecialType.System_String
        && property.SetMethod is not null
        && Find(property, Names.JsonIgnore) is not null
        && _attributeNamePattern.IsMatch(name);
}
