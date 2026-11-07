# CloudEvents SDK check

Background for [D-14](../decisions.md#d-14). The decision text itself is in the decisions file.

Checked on 2026-11-06 against the `cloudevents/sdk-csharp` repository (main branch), its guide, its release notes and NuGet. Not run: no .NET SDK in this environment, so the AOT findings are from source and issue tracker, not from a publish.

## Findings

| Topic | Finding |
|---|---|
| Version and activity | `CloudNative.CloudEvents` 2.9.0 released 2026-07-04 (previous release 2.8.0 in 2024-08). Commits continue (October 2026). Mostly one maintainer (Jon Skeet) with limited time; he asked for contributions for AOT |
| **AOT** | **Not AOT compatible.** Issue #272 "Make library AOT compatible" (open since 2023-10, produces `IL2104`/`IL3053`), the maintainer states "No, it isn't". PR #381 "AOT support" was opened on 2026-11-01 and is not merged. The `SystemTextJson` project has no AOT/trim annotations and calls `JsonSerializer.Serialize/Deserialize` with `JsonSerializerOptions` (reflection based). The earlier "100% compatible" assumption is therefore only true for the narrow case where `Data` is `byte[]`/`string`/`JsonElement`; this is not guaranteed by the library and not checked by analyzers |
| MQTT binding | `CloudNative.CloudEvents.Mqtt` 3.9.0 depends on **MQTTnet 4.3.x**; current MQTTnet is 5.2. Only **structured** mode (`// TODO: Support both binary and structured mode`). No MQTT 5 user properties, content type, response topic, correlation data or expiry interval. We use binary mode with user properties today (D-10: native features) |
| AMQP binding | `CloudNative.CloudEvents.Amqp` 2.9.0 on AMQPNetLite.Core. Structured and binary mode, `cloudEvents_` / `cloudEvents:` property prefixes. The only binding that matches our needs |
| NATS binding | None in the SDK. NuGet has the third-party `CloudNative.CloudEvents.NATS` (author Idar Bergli, versions 0.1.x to 1.0.0-preview) |
| Zenoh binding | None (and none in the CloudEvents spec) |
| Content modes | Structured and binary per binding; there is no common interface for bindings (by design) |
| Data model | `CloudEvent.Data` is `object`; attributes are boxed values in a dictionary (`DateTimeOffset`, `Uri`, `byte[]`); binary data is `byte[]` (no `ReadOnlyMemory<byte>`; the code has a TODO). The structured JSON formatter encodes `byte[]` data as `data_base64`. This is the opposite of the span/low-allocation goal (driver 5) |
| Extension attributes | Supported through `CloudEventAttribute` definitions (typed, validated); the SDK ships some predefined ones in `CloudNative.CloudEvents.Extensions` (the guide names `Sequence`; the full list was not checked). Our `mdcs*` attributes, `correlationid`/`causationid`, `expiryinterval` would be our own definitions. `custommetadata` (flattened arbitrary keys) works as string extension attributes |
| Not part of the SDK | `transportmetadata` (non-serialized transport context), request/response addressing, ack handling |
| License | Apache-2.0 |

## Size of the AOT gap (checked in a clone of the repository)

* Reflection in the core library: one `Activator.CreateInstance` in `CloudEventFormatterAttribute` (plus resource lookups). In `SystemTextJson`: the `JsonSerializer` calls that take `JsonSerializerOptions`.
* PR #381 ("AOT support") is small: 7 files, +269/-16, including tests. So the AOT gap is a **small, mostly non-breaking** change, not a redesign.
* Code size: core ~3.4k lines (including HTTP bindings and a generated resources file), `SystemTextJson` ~0.7k, `Amqp` ~0.25k, `Mqtt` 70 lines. Tests: ~10k lines (unit + integration), a conformance asset we would not want to rewrite.
* Upstream enforces API compatibility (`EnablePackageValidation` with a baseline version) and multi-targets `netstandard2.0;net8.0;net10.0`. Breaking changes and net10-only code cannot go upstream.

## What this means

* The SDK covers 1 of 4 required bindings (AMQP) well, MQTT in the wrong shape, NATS only by a preview third-party package, Zenoh not at all. With D-10 (native protocol features) we write the MQTT, NATS and Zenoh mappings ourselves anyway.
* The AOT problem is small and fixable. The allocation-heavy object model (boxed attributes, `byte[]` data) is a design property, but a minor cost compared with broker, Valkey and JSON payload work at DCS message rates. **Measure before breaking it** (R-18).
* Rewriting from scratch would discard the attribute type system, spec edge cases, the AMQP binding and ~10k lines of tests.

## AOT measurement (S02, 2026-11-07)

Measured with `src/MicroDCS.App` (`microdcs smoke`), .NET SDK 10.0.403, `CloudNative.CloudEvents` and `.SystemTextJson` 2.9.0 from NuGet, linux-x64. The binary ran on the host and in the chiseled `runtime-deps` image, locally and in CI (PR #40).

| Check | Result |
|---|---|
| Publish warnings | With `TrimmerSingleWarn`, exactly one `IL2104` and one `IL3053`, both for `CloudNative.CloudEvents.SystemTextJson`. The core `CloudNative.CloudEvents` assembly produces none in this usage. `scripts/aot-publish` enforces the allowlist |
| Structured mode, `Data` as `JsonElement` | Works, **only with** `new JsonEventFormatter(options, default)`, where `options.TypeInfoResolver` is a source-generated `JsonSerializerContext` that includes `JsonElement` |
| Structured mode, `Data` as `byte[]` | Works (`data_base64`) |
| Binary mode, `JsonElement` and `byte[]` | Works |
| `JsonEventFormatter<T>` with a source-generated type | Works with the same options |
| Default `new JsonEventFormatter()` with `JsonElement` data | Throws `InvalidOperationException` ("Reflection-based serialization has been disabled"). `PublishAot` sets `JsonSerializerIsReflectionEnabledByDefault=false`, also in JIT runs of the project. The smoke check asserts this failure, so a change in upstream behaviour shows up |

Result: no runtime failure that needs the fork; D-14 stays on upstream. The rule for the S06 envelope codec: always construct the formatter with source-generated options, never with the default constructor.
PR #381 status on 2026-11-07: open, not merged, last updated 2026-11-01. Latest release: 2.9.0 (2026-07-04).

## Follow-ups

* ~~Verify in S02~~: done, see above.
* Check which of our attributes are covered by spec extensions (`expirytime`, `recordedtime`, tracing, correlation) and adopt the spec's names and types where they exist, to stay interoperable; `mdcs*` stays ours. Check the spec NATS binding for the header prefix (not verified here).
* Benchmark the envelope (BenchmarkDotNet, R-18) in S10, once the pipeline hot path exists; S23 records the baseline.
* Decide the JSON shape of the transport-neutral envelope together with xRegistry/WoT later (out of scope list).
