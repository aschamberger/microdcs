using BenchmarkDotNet.Running;
using MicroDCS.Benchmarks;

// dotnet run -c Release --project bench/MicroDCS.Benchmarks -- --filter '*'
// CI baseline (R-18, D-58): scripts/bench, which adds --baseline bench/baseline.json (compare) or --update-baseline (rewrite).
var (baseline, update, rest) = BaselineArguments.Split(args);
var summaries = BenchmarkSwitcher.FromAssembly(typeof(PipelineBenchmarks).Assembly).Run(rest).ToList();
if (baseline is null)
{
    return 0;
}

var measured = Measurement.From(summaries);
if (update)
{
    // A benchmark without a result would drop out of the baseline and no longer be gated.
    if (measured.Any(static m => m.Bytes is null))
    {
        await Console.Error.WriteLineAsync($"Not written: a benchmark has no result ({string.Join(", ", measured.Where(static m => m.Bytes is null).Select(static m => m.Name))}).").ConfigureAwait(false);
        return 1;
    }

    Measurement.Write(baseline, measured);
    Console.WriteLine($"Baseline written to {baseline}.");
    return 0;
}

var report = Measurement.Compare(Measurement.Read(baseline), measured, out var failed);
Console.WriteLine(report);
if (Environment.GetEnvironmentVariable("GITHUB_STEP_SUMMARY") is { Length: > 0 } summaryFile)
{
    await File.AppendAllTextAsync(summaryFile, report).ConfigureAwait(false);
}

return failed ? 1 : 0;
