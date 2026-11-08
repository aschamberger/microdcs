; Unshipped analyzer releases
; https://github.com/dotnet/roslyn-analyzers/blob/main/src/Microsoft.CodeAnalysis.Analyzers/ReleaseTrackingAnalyzers.Help.md

### New Rules

Rule ID | Category | Severity | Notes
--------|----------|----------|-------
MDCS001 | MicroDCS | Error | Processor class must be partial
MDCS002 | MicroDCS | Error | Handler without payload parameter
MDCS003 | MicroDCS | Error | Duplicate handler
MDCS004 | MicroDCS | Error | Unsupported parameter
MDCS005 | MicroDCS | Error | Unsupported return type
MDCS006 | MicroDCS | Error | Payload without [CloudEventType]
MDCS007 | MicroDCS | Error | Invalid [CarriedAs]
MDCS008 | MicroDCS | Error | Duplicate type id
MDCS009 | MicroDCS | Error | Payload not in a JsonSerializerContext
MDCS010 | MicroDCS | Error | Codec has no generated descriptor
MDCS011 | MicroDCS | Error | Payload type not supported
MDCS012 | MicroDCS | Error | Invalid [Processor] attribute
