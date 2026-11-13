using System.Globalization;
using System.Text;
using System.Text.Json;
using BenchmarkDotNet.Reports;

namespace MicroDCS.Benchmarks;

/// <summary>Splits the baseline options off the BenchmarkDotNet arguments.</summary>
internal static class BaselineArguments
{
    public static (string? Baseline, bool Update, string[] Remaining) Split(string[] args)
    {
        string? baseline = null;
        var update = false;
        var rest = new List<string>();
        using var arguments = ((IEnumerable<string>)args).GetEnumerator();
        while (arguments.MoveNext())
        {
            switch (arguments.Current)
            {
                case "--baseline":
                    baseline = arguments.MoveNext() ? arguments.Current : throw new ArgumentException("--baseline needs a file.");
                    break;
                case "--update-baseline":
                    update = true;
                    break;
                default:
                    rest.Add(arguments.Current);
                    break;
            }
        }

        if (update && baseline is null)
        {
            throw new ArgumentException("--update-baseline needs --baseline <file>.");
        }

        return (baseline, update, [.. rest]);
    }
}

/// <summary>One benchmark's result: allocated bytes per operation (the gate) and the mean time (reported only).</summary>
internal sealed record Measurement(string Name, long? Bytes, double? MeanNs)
{
    /// <summary>A change of the allocations beyond this share fails, either way; allocations are deterministic, so the margin is for runtime updates.</summary>
    private const double AllowedGrowth = 0.10;

    /// <summary>A change below this many bytes passes even beyond the share (one small object on a small baseline).</summary>
    private const long AllowedBytes = 64;

    private static long Margin(long baseline) => Math.Max(AllowedBytes, (long)(baseline * AllowedGrowth));

    public static List<Measurement> From(IEnumerable<Summary> summaries) =>
    [
        .. summaries.SelectMany(static s => s.Reports)
            .Select(static r => new Measurement(
                r.BenchmarkCase.Descriptor.WorkloadMethod.Name,
                r.Success && r.ResultStatistics is not null ? r.GcStats.GetBytesAllocatedPerOperation(r.BenchmarkCase) : null,
                r.Success ? r.ResultStatistics?.Mean : null))
            .OrderBy(static m => m.Name, StringComparer.Ordinal),
    ];

    public static void Write(string path, IReadOnlyList<Measurement> measured)
    {
        using var stream = File.Create(path);
        using var writer = new Utf8JsonWriter(stream, new JsonWriterOptions { Indented = true });
        writer.WriteStartObject();
        writer.WriteString("note", "Allocated bytes per operation are the gate (scripts/bench, D-58); meanNs is for reference only.");
        writer.WriteStartObject("benchmarks");
        foreach (var m in measured.Where(static m => m.Bytes is not null))
        {
            writer.WriteStartObject(m.Name);
            writer.WriteNumber("bytes", m.Bytes!.Value); // Filtered on Bytes above.
            writer.WriteNumber("meanNs", Math.Round(m.MeanNs ?? 0, 1));
            writer.WriteEndObject();
        }

        writer.WriteEndObject();
        writer.WriteEndObject();
        writer.Flush();
        stream.WriteByte((byte)'\n');
    }

    public static Dictionary<string, Measurement> Read(string path)
    {
        using var document = JsonDocument.Parse(File.ReadAllText(path));
        return document.RootElement.GetProperty("benchmarks").EnumerateObject().ToDictionary(
            static p => p.Name,
            static p => new Measurement(p.Name, p.Value.GetProperty("bytes").GetInt64(), p.Value.GetProperty("meanNs").GetDouble()),
            StringComparer.Ordinal);
    }

    public static string Compare(Dictionary<string, Measurement> baseline, IReadOnlyList<Measurement> measured, out bool failed)
    {
        failed = false;
        var text = new StringBuilder();
        text.AppendLine("### Benchmarks against bench/baseline.json")
            .AppendLine()
            .AppendLine("| Benchmark | Allocated (B) | Baseline (B) | Mean (ns) | Baseline (ns) | Result |")
            .AppendLine("|---|---:|---:|---:|---:|---|");
        foreach (var m in measured)
        {
            baseline.TryGetValue(m.Name, out var b);
            string result;
            if (m.Bytes is null)
            {
                result = "failed: no result";
                failed = true;
            }
            else if (b is null)
            {
                result = "new: run scripts/bench --update";
                failed = true;
            }
            else if (m.Bytes > b.Bytes + Margin(b.Bytes!.Value))
            {
                result = "failed: allocates more";
                failed = true;
            }
            else if (m.Bytes < b.Bytes - Margin(b.Bytes.Value))
            {
                // A lower baseline is the one to keep, or a later regression back up to the old figure would pass.
                result = "failed: allocates less, run scripts/bench --update";
                failed = true;
            }
            else
            {
                result = "ok";
            }

            text.AppendLine(CultureInfo.InvariantCulture, $"| {m.Name} | {m.Bytes?.ToString(CultureInfo.InvariantCulture) ?? "NA"} | {b?.Bytes?.ToString(CultureInfo.InvariantCulture) ?? "-"} | {m.MeanNs?.ToString("F0", CultureInfo.InvariantCulture) ?? "NA"} | {b?.MeanNs?.ToString("F0", CultureInfo.InvariantCulture) ?? "-"} | {result} |");
        }

        foreach (var missing in baseline.Keys.Except(measured.Select(static m => m.Name), StringComparer.Ordinal))
        {
            text.AppendLine(CultureInfo.InvariantCulture, $"| {missing} | - | {baseline[missing].Bytes} | - | - | failed: not run |");
            failed = true;
        }

        return text.AppendLine().ToString();
    }
}
