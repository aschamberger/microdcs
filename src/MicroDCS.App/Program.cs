using MicroDCS.App;

// The application host with real transports arrives in S13 and later. Until then the binary runs the
// Native AOT smoke checks (S02, D-14; S08: the core end to end).
switch (args)
{
    case ["smoke"]:
        var cloudEvents = CloudEventsSmoke.Run(Console.Out);
        var host = await HostSmoke.RunAsync(Console.Out).ConfigureAwait(false);
        return cloudEvents + host == 0 ? 0 : 1;
    default:
        await Console.Error.WriteLineAsync("usage: microdcs smoke").ConfigureAwait(false);
        return 2;
}
