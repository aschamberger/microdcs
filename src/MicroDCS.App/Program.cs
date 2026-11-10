using MicroDCS.App;

// The application host with real transports arrives in S13 and later. Until then the binary runs the
// Native AOT smoke checks (S02, D-14; S08: the core end to end) and `serve`, the greetings demo on the
// in-memory transport with the management listener (S11).
switch (args)
{
    case ["serve"]:
        await ServeCommand.RunAsync(args).ConfigureAwait(false);
        return 0;
    case ["smoke"]:
        var cloudEvents = CloudEventsSmoke.Run(Console.Out);
        var host = await HostSmoke.RunAsync(Console.Out).ConfigureAwait(false);
        var valkey = await ValkeySmoke.RunAsync(Console.Out).ConfigureAwait(false);
        var inspection = await InspectionSmoke.RunAsync(Console.Out).ConfigureAwait(false);
        var jobControl = JobControlSmoke.Run(Console.Out);
        var telemetry = await TelemetrySmoke.RunAsync(Console.Out).ConfigureAwait(false);
        return cloudEvents + host + valkey + inspection + jobControl + telemetry == 0 ? 0 : 1;
    default:
        await Console.Error.WriteLineAsync("usage: microdcs smoke | serve").ConfigureAwait(false);
        return 2;
}
