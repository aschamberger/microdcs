using System.Text.Json.Serialization;
using MicroDCS.Core.Authoring;
using MicroDCS.Core.Messaging;

namespace MicroDCS.Benchmarks;

[CloudEventType("bench.hello.v1")]
public sealed record Hello(string Name);

[JsonSerializable(typeof(Hello))]
internal sealed partial class BenchJsonContext : JsonSerializerContext;

[Processor(ProcessorBinding.Southbound, Id = "bench")]
public sealed partial class BenchProcessor
{
    [Incoming]
    public static Hello OnHello(Hello hello) => hello with { };
}
