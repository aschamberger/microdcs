using BenchmarkDotNet.Running;
using MicroDCS.Benchmarks;

// dotnet run -c Release --project bench/MicroDCS.Benchmarks -- --filter '*'
BenchmarkSwitcher.FromAssembly(typeof(PipelineBenchmarks).Assembly).Run(args);
