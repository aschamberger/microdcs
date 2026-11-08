using System.Xml.Linq;
using MicroDCS.JobControl.StateMachine;
using MicroDCS.JobControl.TableGen;

namespace MicroDCS.JobControl.Tests;

/// <summary>Pinning of the vendored NodeSet and the drift test of the generated table (D-34).</summary>
public sealed class NodeSetTests
{
    private const string V2File = "NodeSets/Opc.Ua.ISA95.JobControl.V2.NodeSet2.xml";

    [Fact]
    public void Vendored_v2_nodeset_is_the_pinned_version()
    {
        var root = XDocument.Load(V2File).Root!;
        var ua = root.GetDefaultNamespace();
        var model = root.Element(ua + "Models")!.Element(ua + "Model")!;

        model.Attribute("ModelUri")!.Value.ShouldBe("http://opcfoundation.org/UA/ISA95-JOBCONTROL_V2/");
        model.Attribute("Version")!.Value.ShouldBe("2.0.0");
        model.Attribute("PublicationDate")!.Value.ShouldBe("2024-01-31T00:00:00Z");
        root.Elements().Count(e => e.Name.LocalName.StartsWith("UA", StringComparison.Ordinal)).ShouldBe(258);
    }

    [Fact]
    public void Generated_table_matches_the_nodeset()
    {
        using var nodeSet = File.OpenRead(V2File);

        TransitionTableGenerator.Generate(nodeSet).ShouldBe(File.ReadAllText("NodeSets/JobControlV2Transitions.g.cs.txt"));
    }

    [Fact]
    public void Table_holds_the_34_transitions_of_the_model()
    {
        JobControlV2Transitions.All.Length.ShouldBe(34);
        JobControlV2Transitions.All.Count(t => t.Machine == "ISA95JobOrderReceiverObjectType").ShouldBe(13);
        JobControlV2Transitions.All.Count(t => t.Machine == "ISA95InterruptedStateMachineType").ShouldBe(2);
        JobControlV2Transitions.All.Count(t => t.Machine == "ISA95EndedStateMachineType").ShouldBe(1);
        JobControlV2Transitions.All.Count(t => t.Machine == "ISA95PrepareStateMachineType").ShouldBe(5);
    }

    [Fact]
    public void Sub_states_machine_type_repeats_the_receiver_type()
    {
        static IEnumerable<(string, string, string?)> Rows(string machine) =>
            JobControlV2Transitions.All.Where(t => t.Machine == machine).Select(t => (t.From, t.To, t.Cause));

        Rows("ISA95JobOrderReceiverSubStatesType").ShouldBe(Rows("ISA95JobOrderReceiverObjectType"));
    }
}
