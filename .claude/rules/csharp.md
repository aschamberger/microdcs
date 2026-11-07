---
paths:
  - "**/*.cs"
---

# C# rules

* AOT first: no reflection-based discovery, `dynamic`, `Activator`, `MakeGenericType`/`MakeGenericMethod`, `Reflection.Emit` or `Expression.Compile`. If an API needs `[RequiresUnreferencedCode]` or `[RequiresDynamicCode]`, find another way or stop and record it in the session log.
* JSON: System.Text.Json with a source-generated `JsonSerializerContext`; pass `JsonTypeInfo<T>`, never the reflection overloads.
* Logging: `[LoggerMessage]` partial methods in a `static partial class Log` next to the type; event ids unique per assembly. No payloads or secrets in log arguments.
* Async: `CancellationToken` as the last parameter of every async method, `ConfigureAwait(false)` in `src/`, `ValueTask` only on hot paths that often complete synchronously.
* Time: inject `TimeProvider`; no `DateTime.Now`/`UtcNow`, no `Task.Delay` without the provider.
* Types: `sealed` by default, records for immutable data, `required` for mandatory init properties, nullable annotations correct (no `!` without a comment saying why).
* Public API: XML docs on every public member; keep the public surface small (`internal` by default, `InternalsVisibleTo` for tests).
* Errors: throw specific exceptions; fail fast at startup (D-27); full bounded queues fail explicitly (D-28).
* Do not suppress warnings to get a green build. A `#pragma` or `[SuppressMessage]` needs a `Justification` and is reported in the session log.
