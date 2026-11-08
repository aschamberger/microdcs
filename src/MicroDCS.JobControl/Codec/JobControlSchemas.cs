using Opc.Ua;
using Opc.Ua.Schema;
using Opc.Ua.Schema.Json;

namespace MicroDCS.JobControl.Codec;

/// <summary>Generates the JSON Schema for <c>dataschema</c> from the generated types at runtime (D-34).</summary>
public static class JobControlSchemas
{
    private static readonly DefaultSchemaProvider _provider = new(
        new EncodeableFactoryDefinitionSource(JobControlContext.Shared.Factory, JobControlContext.Shared.NamespaceUris),
        [new JsonSchemaGenerator()]);

    /// <summary>Generates the verbose JSON Schema of a type.</summary>
    /// <typeparam name="T">The generated type.</typeparam>
    /// <returns>The schema document.</returns>
    /// <exception cref="InvalidOperationException">The type is not registered.</exception>
    public static string GenerateJson<T>()
        where T : class, IEncodeable, new()
    {
        return _provider.TryGetSchema(new T().TypeId, UaSchemaFormat.JsonVerbose, UaSchemaScope.Type, out var schema)
            ? schema.ToSchemaString()
            : throw new InvalidOperationException($"No schema for '{typeof(T).Name}'.");
    }
}
