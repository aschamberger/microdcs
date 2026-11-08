using MicroDCS.JobControl.Codec;
using MicroDCS.JobControl.Conversions;
using MicroDCS.JobControl.StateMachine;
using Opc.Ua;
using V2 = Opc.Ua.ISA95.JobControl.V2;

namespace MicroDCS.JobControl.Tests;

/// <summary>The V2 state array. Numbers and browse paths are from the NodeSet and the upstream <c>Isa95V2StateMachineTests</c> vectors.</summary>
public sealed class JobStateArrayTests
{
    [Theory]
    [InlineData(JobOrderState.NotAllowedToStartWaiting, 1u, "NotAllowedToStart", "NotAllowedToStartSubstates", 1u, "Waiting")]
    [InlineData(JobOrderState.NotAllowedToStartReady, 1u, "NotAllowedToStart", "NotAllowedToStartSubstates", 2u, "Ready")]
    [InlineData(JobOrderState.NotAllowedToStartLoaded, 1u, "NotAllowedToStart", "NotAllowedToStartSubstates", 3u, "Loaded")]
    [InlineData(JobOrderState.AllowedToStartWaiting, 2u, "AllowedToStart", "AllowedToStartSubstates", 1u, "Waiting")]
    [InlineData(JobOrderState.AllowedToStartReady, 2u, "AllowedToStart", "AllowedToStartSubstates", 2u, "Ready")]
    [InlineData(JobOrderState.AllowedToStartLoaded, 2u, "AllowedToStart", "AllowedToStartSubstates", 3u, "Loaded")]
    [InlineData(JobOrderState.Held, 4u, "Interrupted", "InterruptedSubstates", 1u, "Held")]
    [InlineData(JobOrderState.Suspended, 4u, "Interrupted", "InterruptedSubstates", 2u, "Suspended")]
    [InlineData(JobOrderState.Completed, 5u, "Ended", "EndedSubstates", 1u, "Completed")]
    [InlineData(JobOrderState.Closed, 5u, "Ended", "EndedSubstates", 2u, "Closed")]
    public void States_with_a_sub_state_have_two_entries(JobOrderState state, uint top, string topText, string machine, uint sub, string subText)
    {
        var encoded = JobStateArray.ToV2(state);

        encoded.Count.ShouldBe(2);
        encoded[0].StateNumber.ShouldBe(top);
        encoded[0].StateText.Text.ShouldBe(topText);
        encoded[0].BrowsePath.Elements.Count.ShouldBe(0);
        encoded[1].StateNumber.ShouldBe(sub);
        encoded[1].StateText.Text.ShouldBe(subText);
        encoded[1].BrowsePath.Elements[0].TargetName.Name.ShouldBe(machine);
        encoded[1].BrowsePath.Elements[0].ReferenceTypeId.ShouldBe(ReferenceTypeIds.HasSubStateMachine);
    }

    [Theory]
    [InlineData(JobOrderState.Running, 3u, "Running")]
    [InlineData(JobOrderState.Aborted, 6u, "Aborted")]
    [InlineData(JobOrderState.V1Loaded, 3u, "Running")]
    [InlineData(JobOrderState.V1Error, 6u, "Aborted")]
    public void States_without_a_sub_state_have_one_entry(JobOrderState state, uint top, string text)
    {
        var encoded = JobStateArray.ToV2(state);

        encoded.Count.ShouldBe(1);
        encoded[0].StateNumber.ShouldBe(top);
        encoded[0].StateText.Text.ShouldBe(text);
    }

    [Fact]
    public void Every_v2_state_round_trips()
    {
        foreach (var state in Enum.GetValues<JobOrderState>().Except([JobOrderState.Initial, JobOrderState.Removed, JobOrderState.V1Loaded, JobOrderState.V1Error]))
        {
            JobStateArray.FromV2(JobStateArray.ToV2(state)).ShouldBe(state);
        }
    }

    [Theory]
    [InlineData(1u, JobOrderState.NotAllowedToStartReady)]
    [InlineData(2u, JobOrderState.AllowedToStartReady)]
    [InlineData(4u, JobOrderState.Held)]
    [InlineData(5u, JobOrderState.Completed)]
    public void A_top_level_entry_alone_takes_the_default_sub_state(uint number, JobOrderState expected) =>
        JobStateArray.FromV2([new V2.ISA95StateDataType { StateNumber = number }]).ShouldBe(expected);

    [Fact]
    public void Empty_or_unknown_arrays_are_initial()
    {
        JobStateArray.FromV2([]).ShouldBe(JobOrderState.Initial);
        JobStateArray.FromV2([new V2.ISA95StateDataType { StateNumber = 99 }]).ShouldBe(JobOrderState.Initial);
    }

    [Fact]
    public void Top_level_query_matches_every_sub_state()
    {
        ArrayOf<V2.ISA95StateDataType> query = [new V2.ISA95StateDataType { StateNumber = 4 }];

        JobStateArray.Matches(JobOrderState.Held, query).ShouldBeTrue();
        JobStateArray.Matches(JobOrderState.Suspended, query).ShouldBeTrue();
        JobStateArray.Matches(JobOrderState.Running, query).ShouldBeFalse();
    }

    [Fact]
    public void Sub_state_query_matches_exactly()
    {
        var query = JobStateArray.ToV2(JobOrderState.Suspended);

        JobStateArray.Matches(JobOrderState.Suspended, query).ShouldBeTrue();
        JobStateArray.Matches(JobOrderState.Held, query).ShouldBeFalse();
    }

    [Fact]
    public void Query_with_a_sub_state_of_another_parent_matches_nothing()
    {
        var query = JobStateArray.ToV2(JobOrderState.Held);
        query[0].StateNumber = 1;

        JobStateArray.Matches(JobOrderState.Held, query).ShouldBeFalse();
        JobStateArray.Matches(JobOrderState.NotAllowedToStartReady, query).ShouldBeFalse();
    }

    [Fact]
    public void Query_with_an_unknown_sub_state_matches_nothing()
    {
        var query = JobStateArray.ToV2(JobOrderState.Held);
        query[1].StateNumber = 99;

        JobStateArray.Matches(JobOrderState.Held, query).ShouldBeFalse();
    }

    [Fact]
    public void Array_with_mismatched_entries_falls_back_to_the_top_level_state()
    {
        var state = JobStateArray.ToV2(JobOrderState.Held);
        state[0].StateNumber = 1;

        JobStateArray.FromV2(state).ShouldBe(JobOrderState.NotAllowedToStartReady);
    }

    [Fact]
    public void Sub_state_browse_names_carry_the_v2_namespace()
    {
        var target = JobStateArray.ToV2(JobOrderState.Held)[1].BrowsePath.Elements[0].TargetName;

        target.NamespaceIndex.ShouldBe((ushort)JobControlContext.Shared.NamespaceUris.GetIndex(V2.Namespaces.ISA95JobControlV2));
    }

    [Fact]
    public void Removed_and_initial_have_no_v2_view()
    {
        JobStateArray.ToV2(JobOrderState.Initial).Count.ShouldBe(0);
        JobStateArray.ToV2(JobOrderState.Removed).Count.ShouldBe(0);
    }
}
