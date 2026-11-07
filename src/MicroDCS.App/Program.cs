using MicroDCS.App;

// The application host arrives with the core in S08. Until then the binary only runs the
// Native AOT smoke checks (S02, D-14).
return args switch
{
    ["smoke"] => CloudEventsSmoke.Run(Console.Out),
    _ => Usage(),
};

static int Usage()
{
    Console.Error.WriteLine("usage: microdcs smoke");
    return 2;
}
