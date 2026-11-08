using MicroDCS.JobControl.TableGen;

// Regenerates src/MicroDCS.JobControl/StateMachine/JobControlV2Transitions.g.cs (D-34).
// The drift test in MicroDCS.JobControl.Tests runs the same generator and compares the result.
if (args is not [var nodeSetPath, var outputPath])
{
    await Console.Error.WriteLineAsync("usage: tablegen <NodeSet2.xml> <output.cs>").ConfigureAwait(false);
    return 2;
}

using var nodeSet = new MemoryStream(await File.ReadAllBytesAsync(nodeSetPath).ConfigureAwait(false));
await File.WriteAllTextAsync(outputPath, TransitionTableGenerator.Generate(nodeSet)).ConfigureAwait(false);
return 0;
