using MicroDCS.Core.Hosting;
using MicroDCS.Core.Messaging;
using MicroDCS.Core.Transport;
using MicroDCS.JobControl.Codec;
using MicroDCS.JobControl.Conversions;
using MicroDCS.JobControl.Extension;
using MicroDCS.JobControl.Model;
using MicroDCS.JobControl.StateMachine;
using MicroDCS.Testing;
using Microsoft.Extensions.DependencyInjection;
using Opc.Ua;
using V2 = Opc.Ua.ISA95.JobControl.V2;

namespace MicroDCS.MachineryJobs.Tests;

/// <summary>The Python processor tests (<c>test_machinery_jobs_processor.py</c>) through the pipeline, plus the deliberate changes of D-50.</summary>
public sealed class ProcessorTests : IAsyncLifetime
{
    private const string Subject = "site.area/station";
    private const string Scope = "site.area";
    private const ulong Success = (ulong)JobReturnStatus.Success;
    private const ulong Unknown = (ulong)JobReturnStatus.UnknownJobOrderId;
    private const ulong InvalidStatus = (ulong)JobReturnStatus.InvalidStatus;
    private const ulong Unable = (ulong)JobReturnStatus.UnableToAccept;
    private const ulong InvalidRequest = (ulong)JobReturnStatus.InvalidRequest;

    private readonly InMemoryJobStore _store = new();
    private JobAcceptanceOptions _options = new();
    private ProcessorHarness _harness = null!;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync() => await StartAsync();

    public async ValueTask DisposeAsync()
    {
        await _harness.DisposeAsync();
        GC.SuppressFinalize(this);
    }

    private static V2.ISA95JobOrderDataType Order(string id = "job-1", Action<JobOrderBuilder>? configure = null)
    {
        var builder = new JobOrderBuilder { Id = id };
        configure?.Invoke(builder);
        return JobControlConversions.ToV2Order(builder.Build());
    }

    private async Task StartAsync(Action<JobAcceptanceOptions>? configure = null)
    {
        if (_harness is not null)
        {
            await _harness.DisposeAsync();
        }

        _options = new JobAcceptanceOptions();
        configure?.Invoke(_options);
        _harness = await ProcessorHarness.StartAsync<MachineryJobsProcessor>(
            services =>
            {
                services.AddSingleton<IJobStore>(_store);

                // A redelivery reaches the handler only when the claim was released after a fault; these tests are about that case.
                services.AddSingleton<IDedupeStore, NoDedupeStore>();
                services.AddSingleton<JobAcceptanceCheck>();
                services.AddSingleton(Microsoft.Extensions.Options.Options.Create(_options));
                services.AddPayloads([.. MachineryJobsProcessor.PublishedPayloads]);
            },
            Ct);

        // The start publishes the metadata event through the queue; let it arrive so it cannot land in a test's window.
        await WaitForAsync(JobControlTypes.Metadata, 0);
    }

    private async Task<ulong> SendAsync<TCall, TResponse>(TCall call, string subject = Subject)
        where TCall : class
        where TResponse : JobMethodResponseDataType
    {
        var result = await _harness.SendAsync(call, subject, Ct);
        result.Outcome.ShouldBe(DeliveryOutcome.Ack);
        return _harness.Decode<TResponse>(result.Published.Single(m => m.Envelope.Type.EndsWith(".response.v2", StringComparison.Ordinal))).ReturnStatus;
    }

    private Task<ulong> StoreAsync(string id = "job-1") => SendAsync<StoreCall, StoreResponse>(new StoreCall { JobOrder = Order(id) });

    private Task<ulong> StoreAndStartAsync(string id = "job-1") => SendAsync<StoreAndStartCall, StoreAndStartResponse>(new StoreAndStartCall { JobOrder = Order(id) });

    private JobOrderState State(string id = "job-1") => _store.Order(id).ShouldNotBeNull().State;

    // Status events leave through the outgoing queue, after the handler returned.
    private async Task<List<V2.ISA95StateDataType[]>> StatusEventsAsync(Func<Task> act, int expected = 1)
    {
        var before = _harness.Transport.Published.Length;
        await act();
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (Events(before).Count < expected && DateTime.UtcNow < deadline)
        {
            await Task.Delay(5, Ct);
        }

        if (expected == 0)
        {
            await Task.Delay(100, Ct);
        }

        return Events(before);
    }

    private async Task<OutboundMessage> WaitForAsync(string type, int before)
    {
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (true)
        {
            if (_harness.Transport.Published.Skip(before).FirstOrDefault(m => m.Envelope.Type == type) is { } message)
            {
                return message;
            }

            (DateTime.UtcNow < deadline).ShouldBeTrue($"No message of type {type} was published.");
            await Task.Delay(5, Ct);
        }
    }

    private List<V2.ISA95StateDataType[]> Events(int before) =>
        [.. _harness.Transport.Published.Skip(before)
            .Where(static m => m.Envelope.Type == JobControlTypes.JobOrderStatusEvent)
            .Select(m => _harness.Decode<JobOrderStatusEvent>(m).JobState.ToArray()!)];

    [Fact]
    public async Task Store_success_sets_not_allowed_to_start_and_emits_a_status_event()
    {
        var events = await StatusEventsAsync(async () => (await StoreAsync()).ShouldBe(Success));

        State().ShouldBe(JobOrderState.NotAllowedToStartReady);
        events.Count.ShouldBe(1);
        _store.Changes.ShouldBe(["Store:job-1"]);
        _store.RecipeStarts.ShouldBeEmpty();
    }

    [Fact]
    public async Task Store_and_start_sets_allowed_to_start_and_queues_the_recipe()
    {
        (await StoreAndStartAsync()).ShouldBe(Success);

        State().ShouldBe(JobOrderState.AllowedToStartReady);
        _store.RecipeStarts.ShouldBe(["job-1"]);
    }

    [Theory]
    [InlineData("Abort")]
    [InlineData("Stop")]
    public async Task Ending_a_running_job_queues_the_end_of_its_recipe(string command)
    {
        await _store.SaveOrderAsync(new JobOrderWrite(new JobOrder { Id = "job-1" }, JobOrderState.Running, "site.area", "Run", 0, "seed", JobRecipeWork.None), Ct);

        var status = command == "Abort"
            ? await SendAsync<AbortCall, AbortResponse>(new AbortCall { JobOrderID = "job-1" })
            : await SendAsync<StopCall, StopResponse>(new StopCall { JobOrderID = "job-1" });

        status.ShouldBe(Success);
        _store.RecipeCancels.ShouldBe(["job-1"]);
    }

    [Fact]
    public async Task Pause_holds_the_recipe_and_Resume_releases_it()
    {
        await _store.SaveOrderAsync(new JobOrderWrite(new JobOrder { Id = "job-1" }, JobOrderState.Running, "site.area", "Run", 0, "seed", JobRecipeWork.None), Ct);

        (await SendAsync<PauseCall, PauseResponse>(new PauseCall { JobOrderID = "job-1" })).ShouldBe(Success);
        (await SendAsync<PauseCall, PauseResponse>(new PauseCall { JobOrderID = "job-1" })).ShouldBe(InvalidStatus);
        (await SendAsync<ResumeCall, ResumeResponse>(new ResumeCall { JobOrderID = "job-1" })).ShouldBe(Success);

        _store.RecipeWork.ShouldBe(["Hold:job-1", "Release:job-1"]);
        _store.RecipeCancels.ShouldBeEmpty();
    }

    [Theory]
    [InlineData("Abort")]
    [InlineData("Stop")]
    public async Task Ending_a_paused_job_queues_the_end_of_its_recipe(string command)
    {
        await _store.SaveOrderAsync(new JobOrderWrite(new JobOrder { Id = "job-1" }, JobOrderState.Running, "site.area", "Run", 0, "seed", JobRecipeWork.None), Ct);
        await SendAsync<PauseCall, PauseResponse>(new PauseCall { JobOrderID = "job-1" });

        var status = command == "Abort"
            ? await SendAsync<AbortCall, AbortResponse>(new AbortCall { JobOrderID = "job-1" })
            : await SendAsync<StopCall, StopResponse>(new StopCall { JobOrderID = "job-1" });

        status.ShouldBe(Success);
        _store.RecipeWork.ShouldBe(["Hold:job-1", "Cancel:job-1"]);
    }

    [Fact]
    public async Task Status_event_carries_subject_correlation_and_causation()
    {
        var before = _harness.Transport.Published.Length;
        var envelope = new EventEnvelope
        {
            Id = "cmd-1",
            Source = "urn:test",
            Type = JobControlTypes.Store,
            Subject = Subject,
            CorrelationId = "corr-1",
            Data = OpcUaJsonCodec.Encode(new StoreCall { JobOrder = Order() }),
        };

        await _harness.SendEnvelopeAsync(envelope, Ct);

        var status = await WaitForAsync(JobControlTypes.JobOrderStatusEvent, before);
        status.Envelope.Subject.ShouldBe(Scope);
        status.Envelope.CorrelationId.ShouldBe("corr-1");
        status.Envelope.CausationId.ShouldBe("cmd-1");
    }

    [Theory]
    [InlineData("JO.2026")]
    [InlineData("JO/1")]
    [InlineData("JO 1")]
    public async Task A_store_with_an_id_no_state_topic_can_carry_is_refused(string id)
    {
        (await StoreAsync(id)).ShouldBe(InvalidRequest);

        _store.Order(id).ShouldBeNull();
    }

    [Fact]
    public async Task Store_without_a_job_order_is_an_invalid_request() =>
        (await SendAsync<StoreCall, StoreResponse>(new StoreCall())).ShouldBe(InvalidRequest);

    [Fact]
    public async Task Store_and_start_without_a_job_order_is_an_invalid_request() =>
        (await SendAsync<StoreAndStartCall, StoreAndStartResponse>(new StoreAndStartCall())).ShouldBe(InvalidRequest);

    [Fact]
    public async Task Store_of_an_existing_id_does_not_overwrite_it()
    {
        await StoreAsync();
        _store.SetState("job-1", JobOrderState.Running);

        (await StoreAsync()).ShouldBe(Unable);

        State().ShouldBe(JobOrderState.Running);
    }

    [Fact]
    public async Task Store_of_an_id_from_another_scope_is_rejected()
    {
        await StoreAsync();

        (await SendAsync<StoreCall, StoreResponse>(new StoreCall { JobOrder = Order() }, "site.other/station")).ShouldBe(Unable);

        _store.Order("job-1")!.Scope.ShouldBe(Scope);
    }

    [Fact]
    public async Task A_command_from_another_scope_sees_an_unknown_order()
    {
        await StoreAsync();

        (await SendAsync<StartCall, StartResponse>(new StartCall { JobOrderID = "job-1" }, "site.other/station")).ShouldBe(Unknown);

        State().ShouldBe(JobOrderState.NotAllowedToStartReady);
    }

    [Fact]
    public async Task Start_from_not_allowed_to_start_queues_the_recipe()
    {
        await StoreAsync();

        (await SendAsync<StartCall, StartResponse>(new StartCall { JobOrderID = "job-1" })).ShouldBe(Success);

        State().ShouldBe(JobOrderState.AllowedToStartReady);
        _store.RecipeStarts.ShouldBe(["job-1"]);
    }

    [Fact]
    public async Task Start_without_an_id_is_an_invalid_request() =>
        (await SendAsync<StartCall, StartResponse>(new StartCall())).ShouldBe(InvalidRequest);

    [Fact]
    public async Task Start_of_an_unknown_order_reports_the_id() =>
        (await SendAsync<StartCall, StartResponse>(new StartCall { JobOrderID = "nope" })).ShouldBe(Unknown);

    [Fact]
    public async Task Start_in_the_wrong_state_is_invalid_status()
    {
        await StoreAndStartAsync();
        _store.SetState("job-1", JobOrderState.Running);

        (await SendAsync<StartCall, StartResponse>(new StartCall { JobOrderID = "job-1" })).ShouldBe(InvalidStatus);
    }

    [Theory]
    [InlineData(JobOrderState.Running)]
    [InlineData(JobOrderState.NotAllowedToStartReady)]
    [InlineData(JobOrderState.AllowedToStartReady)]
    [InlineData(JobOrderState.Held)]
    public async Task Abort_is_accepted_from_every_active_state(JobOrderState from)
    {
        await StoreAsync();
        _store.SetState("job-1", from);

        (await SendAsync<AbortCall, AbortResponse>(new AbortCall { JobOrderID = "job-1" })).ShouldBe(Success);

        State().ShouldBe(JobOrderState.Aborted);
    }

    [Fact]
    public async Task Pause_and_resume_move_between_running_and_interrupted()
    {
        await StoreAsync();
        _store.SetState("job-1", JobOrderState.Running);

        (await SendAsync<PauseCall, PauseResponse>(new PauseCall { JobOrderID = "job-1" })).ShouldBe(Success);
        State().ShouldBe(JobOrderState.Suspended);

        (await SendAsync<ResumeCall, ResumeResponse>(new ResumeCall { JobOrderID = "job-1" })).ShouldBe(Success);
        State().ShouldBe(JobOrderState.Running);
    }

    [Fact]
    public async Task Pause_when_not_running_fails()
    {
        await StoreAsync();

        (await SendAsync<PauseCall, PauseResponse>(new PauseCall { JobOrderID = "job-1" })).ShouldBe(InvalidStatus);
    }

    [Theory]
    [InlineData(JobOrderState.Running)]
    [InlineData(JobOrderState.Suspended)]
    public async Task Stop_ends_a_running_or_interrupted_order(JobOrderState from)
    {
        await StoreAsync();
        _store.SetState("job-1", from);

        (await SendAsync<StopCall, StopResponse>(new StopCall { JobOrderID = "job-1" })).ShouldBe(Success);

        State().ShouldBe(JobOrderState.Completed);
    }

    [Theory]
    [InlineData(JobOrderState.NotAllowedToStartReady)]
    [InlineData(JobOrderState.AllowedToStartReady)]
    public async Task Cancel_removes_the_order_without_a_status_event(JobOrderState from)
    {
        (await StatusEventsAsync(async () => await StoreAsync())).Count.ShouldBe(1);
        _store.SetState("job-1", from);

        var events = await StatusEventsAsync(async () => (await SendAsync<CancelCall, CancelResponse>(new CancelCall { JobOrderID = "job-1" })).ShouldBe(Success), expected: 0);

        _store.Order("job-1").ShouldBeNull();
        events.ShouldBeEmpty();
        _store.Changes.ShouldContain("Cancel:job-1");
    }

    [Fact]
    public async Task Cancel_from_running_fails()
    {
        await StoreAsync();
        _store.SetState("job-1", JobOrderState.Running);

        (await SendAsync<CancelCall, CancelResponse>(new CancelCall { JobOrderID = "job-1" })).ShouldBe(InvalidStatus);
    }

    [Theory]
    [InlineData(JobOrderState.Aborted)]
    [InlineData(JobOrderState.Completed)]
    public async Task Clear_removes_an_ended_order(JobOrderState from)
    {
        await StoreAsync();
        _store.SetState("job-1", from);

        (await SendAsync<ClearCall, ClearResponse>(new ClearCall { JobOrderID = "job-1" })).ShouldBe(Success);

        _store.Order("job-1").ShouldBeNull();
        _store.Changes.ShouldContain("Clear:job-1");
    }

    [Fact]
    public async Task Clear_from_running_fails()
    {
        await StoreAsync();
        _store.SetState("job-1", JobOrderState.Running);

        (await SendAsync<ClearCall, ClearResponse>(new ClearCall { JobOrderID = "job-1" })).ShouldBe(InvalidStatus);
    }

    [Fact]
    public async Task Revoke_start_returns_to_not_allowed_to_start()
    {
        await StoreAndStartAsync();

        (await SendAsync<RevokeStartCall, RevokeStartResponse>(new RevokeStartCall { JobOrderID = "job-1" })).ShouldBe(Success);

        State().ShouldBe(JobOrderState.NotAllowedToStartReady);
    }

    [Fact]
    public async Task Update_replaces_the_data_and_keeps_the_state()
    {
        await StoreAsync();
        _store.SetState("job-1", JobOrderState.AllowedToStartLoaded);

        (await SendAsync<UpdateCall, UpdateResponse>(new UpdateCall { JobOrder = Order(configure: static b => b.Priority = 9) })).ShouldBe(Success);

        State().ShouldBe(JobOrderState.AllowedToStartLoaded);
        _store.Order("job-1")!.Order.Priority.ShouldBe((short)9);
        _store.RecipeStarts.ShouldBeEmpty("an update inside AllowedToStart is not a start");
    }

    [Fact]
    public async Task Update_without_a_job_order_is_an_invalid_request() =>
        (await SendAsync<UpdateCall, UpdateResponse>(new UpdateCall())).ShouldBe(InvalidRequest);

    [Fact]
    public async Task Update_of_an_unknown_order_reports_the_id() =>
        (await SendAsync<UpdateCall, UpdateResponse>(new UpdateCall { JobOrder = Order("nope") })).ShouldBe(Unknown);

    [Fact]
    public async Task Update_from_running_fails()
    {
        await StoreAsync();
        _store.SetState("job-1", JobOrderState.Running);

        (await SendAsync<UpdateCall, UpdateResponse>(new UpdateCall { JobOrder = Order() })).ShouldBe(InvalidStatus);
    }

    [Fact]
    public async Task Update_that_is_not_acceptable_is_rejected_and_not_stored()
    {
        await StoreAsync();

        (await SendAsync<UpdateCall, UpdateResponse>(new UpdateCall { JobOrder = Order(configure: static b => b.Equipment = "unknown") })).ShouldBe(Unable);

        _store.Order("job-1")!.Order.EquipmentRequirements.Count.ShouldBe(0);
    }

    [Fact]
    public async Task Update_on_a_full_scope_is_not_counted_against_the_limit()
    {
        await StartAsync(static o => o.MaxDownloadableJobOrders = 1);
        await StoreAsync();

        (await SendAsync<UpdateCall, UpdateResponse>(new UpdateCall { JobOrder = Order(configure: static b => b.Priority = 2) })).ShouldBe(Success);
    }

    [Fact]
    public async Task A_full_lifecycle_walks_the_b3_example()
    {
        (await StoreAsync()).ShouldBe(Success);
        (await SendAsync<StartCall, StartResponse>(new StartCall { JobOrderID = "job-1" })).ShouldBe(Success);
        _store.SetState("job-1", JobOrderState.Running);
        (await SendAsync<PauseCall, PauseResponse>(new PauseCall { JobOrderID = "job-1" })).ShouldBe(Success);
        (await SendAsync<ResumeCall, ResumeResponse>(new ResumeCall { JobOrderID = "job-1" })).ShouldBe(Success);
        (await SendAsync<StopCall, StopResponse>(new StopCall { JobOrderID = "job-1" })).ShouldBe(Success);
        (await SendAsync<ClearCall, ClearResponse>(new ClearCall { JobOrderID = "job-1" })).ShouldBe(Success);

        _store.Order("job-1").ShouldBeNull();
    }

    [Fact]
    public async Task Store_then_cancel_removes_the_order()
    {
        await StoreAsync();

        (await SendAsync<CancelCall, CancelResponse>(new CancelCall { JobOrderID = "job-1" })).ShouldBe(Success);

        _store.Order("job-1").ShouldBeNull();
    }

    [Fact]
    public async Task Removed_orders_free_their_slot_in_the_limit()
    {
        await StartAsync(static o => o.MaxDownloadableJobOrders = 1);
        await StoreAsync();
        (await StoreAsync("job-2")).ShouldBe(Unable);

        await SendAsync<CancelCall, CancelResponse>(new CancelCall { JobOrderID = "job-1" });

        (await StoreAsync("job-2")).ShouldBe(Success);
    }

    [Fact]
    public async Task A_concurrent_write_is_retried_on_the_new_version()
    {
        await StoreAsync();
        var raced = false;
        _store.BeforeWrite = () =>
        {
            if (!raced)
            {
                raced = true;
                _store.SetState("job-1", JobOrderState.NotAllowedToStartLoaded);
            }
        };

        (await SendAsync<StartCall, StartResponse>(new StartCall { JobOrderID = "job-1" })).ShouldBe(Success);

        State().ShouldBe(JobOrderState.AllowedToStartReady);
    }

    [Fact]
    public async Task A_write_that_keeps_losing_reports_invalid_status()
    {
        await StoreAsync();
        _store.BeforeWrite = () => _store.SetState("job-1", JobOrderState.NotAllowedToStartWaiting);

        (await SendAsync<StartCall, StartResponse>(new StartCall { JobOrderID = "job-1" })).ShouldBe(InvalidStatus);
    }

    [Fact]
    public async Task A_redelivered_command_whose_write_happened_succeeds_without_a_second_event()
    {
        var call = new EventEnvelope
        {
            Id = "cmd-9",
            Source = "urn:test",
            Type = JobControlTypes.Store,
            Subject = Subject,
            Data = OpcUaJsonCodec.Encode(new StoreCall { JobOrder = Order() }),
        };
        (await StatusEventsAsync(async () => await _harness.SendEnvelopeAsync(call, Ct))).Count.ShouldBe(1);

        var events = await StatusEventsAsync(async () => await _harness.SendEnvelopeAsync(call, Ct), expected: 0);

        events.ShouldBeEmpty();
        _store.Order("job-1")!.Version.ShouldBe(1);
    }

    [Fact]
    public async Task A_redelivered_cancel_whose_removal_happened_succeeds()
    {
        await StoreAsync();
        var cancel = Envelope(JobControlTypes.Cancel, new CancelCall { JobOrderID = "job-1" }, "cmd-5");
        ReturnStatus<CancelResponse>(await _harness.SendEnvelopeAsync(cancel, Ct)).ShouldBe(Success);

        ReturnStatus<CancelResponse>(await _harness.SendEnvelopeAsync(cancel, Ct)).ShouldBe(Success);
    }

    [Fact]
    public async Task A_cancel_of_an_order_removed_by_another_command_is_unknown()
    {
        await StoreAsync();
        await SendAsync<CancelCall, CancelResponse>(new CancelCall { JobOrderID = "job-1" });

        (await SendAsync<CancelCall, CancelResponse>(new CancelCall { JobOrderID = "job-1" })).ShouldBe(Unknown);
    }

    [Fact]
    public async Task A_redelivered_store_is_not_refused_by_its_own_write()
    {
        await StartAsync(static o => o.MaxDownloadableJobOrders = 1);
        var store = Envelope(JobControlTypes.Store, new StoreCall { JobOrder = Order() }, "cmd-6");
        ReturnStatus<StoreResponse>(await _harness.SendEnvelopeAsync(store, Ct)).ShouldBe(Success);

        ReturnStatus<StoreResponse>(await _harness.SendEnvelopeAsync(store, Ct)).ShouldBe(Success);
    }

    [Fact]
    public async Task A_redelivered_update_is_not_refused_by_a_list_change()
    {
        await StoreAsync();
        await _store.AddReferenceAsync(JobReferenceKind.Equipment, "eq", Scope, Ct);
        var update = Envelope(JobControlTypes.Update, new UpdateCall { JobOrder = Order(configure: static b => b.Equipment = "eq") }, "cmd-7");
        ReturnStatus<UpdateResponse>(await _harness.SendEnvelopeAsync(update, Ct)).ShouldBe(Success);
        await _store.RemoveReferenceAsync(JobReferenceKind.Equipment, "eq", Scope, Ct);

        ReturnStatus<UpdateResponse>(await _harness.SendEnvelopeAsync(update, Ct)).ShouldBe(Success);
    }

    [Fact]
    public async Task A_command_from_another_scope_with_the_last_event_id_is_still_unknown()
    {
        ReturnStatus<StoreResponse>(await _harness.SendEnvelopeAsync(Envelope(JobControlTypes.Store, new StoreCall { JobOrder = Order() }, "cmd-8"), Ct)).ShouldBe(Success);
        var foreign = Envelope(JobControlTypes.Start, new StartCall { JobOrderID = "job-1" }, "cmd-8", "site.other/station");

        ReturnStatus<StartResponse>(await _harness.SendEnvelopeAsync(foreign, Ct)).ShouldBe(Unknown);
    }

    [Fact]
    public async Task Query_by_id_without_an_id_is_an_invalid_request()
    {
        var response = await QueryByIdAsync(new RequestJobResponseByJobOrderIdCall());

        response.ReturnStatus.ShouldBe(InvalidRequest);
    }

    [Fact]
    public async Task Query_by_unknown_id_reports_the_id()
    {
        var response = await QueryByIdAsync(new RequestJobResponseByJobOrderIdCall { JobOrderID = "nope" });

        response.ReturnStatus.ShouldBe(Unknown);
    }

    [Fact]
    public async Task Query_by_id_returns_the_response()
    {
        await _store.SaveResponseAsync(new JobResponse { Id = "resp-1", JobOrderId = "job-1", State = JobOrderState.Running }, Scope, Ct);

        var response = await QueryByIdAsync(new RequestJobResponseByJobOrderIdCall { JobOrderID = "job-1" });

        response.ReturnStatus.ShouldBe(Success);
        response.JobResponse.JobResponseID.ShouldBe("resp-1");
    }

    [Fact]
    public async Task Query_by_id_from_another_scope_is_unknown()
    {
        await _store.SaveResponseAsync(new JobResponse { Id = "resp-1", JobOrderId = "job-1", State = JobOrderState.Running }, "site.other", Ct);

        (await QueryByIdAsync(new RequestJobResponseByJobOrderIdCall { JobOrderID = "job-1" })).ReturnStatus.ShouldBe(Unknown);
    }

    [Fact]
    public async Task Query_by_state_without_a_state_is_an_invalid_request()
    {
        var response = await QueryByStateAsync(new RequestJobResponseByJobOrderStateCall());

        response.ReturnStatus.ShouldBe(InvalidRequest);
    }

    [Fact]
    public async Task Query_by_state_with_a_top_level_entry_matches_its_sub_states()
    {
        await _store.SaveResponseAsync(new JobResponse { Id = "r-1", JobOrderId = "j-1", State = JobOrderState.AllowedToStartReady }, Scope, Ct);
        await _store.SaveResponseAsync(new JobResponse { Id = "r-2", JobOrderId = "j-2", State = JobOrderState.AllowedToStartLoaded }, Scope, Ct);
        await _store.SaveResponseAsync(new JobResponse { Id = "r-3", JobOrderId = "j-3", State = JobOrderState.Running }, Scope, Ct);
        var query = new RequestJobResponseByJobOrderStateCall { JobOrderState = [JobStateArray.ToV2(JobOrderState.AllowedToStartReady)[0]] };

        var response = await QueryByStateAsync(query);

        response.ReturnStatus.ShouldBe(Success);
        response.JobResponses.ToArray()!.Select(static r => r.JobResponseID).Order().ShouldBe(["r-1", "r-2"]);
    }

    [Fact]
    public async Task Query_by_state_with_nothing_matching_succeeds_empty()
    {
        var query = new RequestJobResponseByJobOrderStateCall { JobOrderState = JobStateArray.ToV2(JobOrderState.Running) };

        var response = await QueryByStateAsync(query);

        response.ReturnStatus.ShouldBe(Success);
        response.JobResponses.Count.ShouldBe(0);
    }

    [Theory]
    [InlineData("limit")]
    [InlineData("equipment")]
    [InlineData("materialclass")]
    [InlineData("materialdefinition")]
    [InlineData("personnel")]
    [InlineData("physicalasset")]
    [InlineData("workmaster")]
    public async Task A_failing_acceptance_check_rejects_the_order(string check)
    {
        await StartAsync(o => o.MaxDownloadableJobOrders = check == "limit" ? 1 : 0);
        if (check == "limit")
        {
            await StoreAsync("job-0");
        }

        var status = await SendAsync<StoreCall, StoreResponse>(new StoreCall { JobOrder = Order(configure: b => b.Use(check)) });

        status.ShouldBe(Unable);
        _store.Order("job-1").ShouldBeNull();
    }

    [Theory]
    [InlineData("equipment")]
    [InlineData("materialclass")]
    [InlineData("materialdefinition")]
    [InlineData("personnel")]
    [InlineData("physicalasset")]
    [InlineData("workmaster")]
    public async Task A_disabled_acceptance_check_lets_the_order_through(string check)
    {
        await StartAsync(o =>
        {
            o.CheckEquipmentId = check != "equipment";
            o.CheckMaterialClassId = check != "materialclass";
            o.CheckMaterialDefinitionId = check != "materialdefinition";
            o.CheckPersonnelId = check != "personnel";
            o.CheckPhysicalAssetId = check != "physicalasset";
            o.CheckWorkMaster = check != "workmaster";
        });

        (await SendAsync<StoreCall, StoreResponse>(new StoreCall { JobOrder = Order(configure: b => b.Use(check)) })).ShouldBe(Success);
    }

    [Fact]
    public async Task All_checks_pass_with_configured_references()
    {
        await _store.AddReferenceAsync(JobReferenceKind.Equipment, "eq", Scope, Ct);
        await _store.AddReferenceAsync(JobReferenceKind.MaterialClass, "mc", Scope, Ct);

        var status = await SendAsync<StoreCall, StoreResponse>(new StoreCall { JobOrder = Order(configure: static b => { b.Equipment = "eq"; b.MaterialClass = "mc"; }) });

        status.ShouldBe(Success);
    }

    [Fact]
    public async Task Limit_zero_means_unlimited()
    {
        await StartAsync(static o => o.MaxDownloadableJobOrders = 0);

        (await StoreAsync("a")).ShouldBe(Success);
        (await StoreAsync("b")).ShouldBe(Success);
    }

    [Fact]
    public async Task Limit_check_can_be_disabled()
    {
        await StartAsync(static o => { o.MaxDownloadableJobOrders = 1; o.CheckMaxDownloadableJobOrders = false; });

        (await StoreAsync("a")).ShouldBe(Success);
        (await StoreAsync("b")).ShouldBe(Success);
    }

    [Fact]
    public async Task A_scope_limit_overrides_the_default()
    {
        await StartAsync(static o => o.MaxDownloadableJobOrders = 1);
        await _store.SetAcceptanceLimitAsync(Scope, 2, Ct);

        (await StoreAsync("a")).ShouldBe(Success);
        (await StoreAsync("b")).ShouldBe(Success);
        (await StoreAsync("c")).ShouldBe(Unable);
    }

    [Fact]
    public async Task A_scope_limit_applies_when_the_default_has_none()
    {
        await _store.SetAcceptanceLimitAsync(Scope, 1, Ct);

        (await StoreAsync("a")).ShouldBe(Success);
        (await StoreAsync("b")).ShouldBe(Unable);
    }

    [Fact]
    public async Task Without_a_scope_limit_the_default_applies()
    {
        await StartAsync(static o => o.MaxDownloadableJobOrders = 1);

        (await StoreAsync("a")).ShouldBe(Success);
        (await StoreAsync("b")).ShouldBe(Unable);
    }

    [Fact]
    public async Task Config_equipment_is_added_and_deleted()
    {
        await SendConfigAsync(new ConfigEquipment { ID = "eq" });
        (await _store.IsReferenceAsync(JobReferenceKind.Equipment, "eq", Scope, Ct)).ShouldBeTrue();

        await SendConfigAsync(new ConfigEquipment { ID = "eq" }, "DELETE");
        (await _store.IsReferenceAsync(JobReferenceKind.Equipment, "eq", Scope, Ct)).ShouldBeFalse();
    }

    [Fact]
    public async Task Config_material_class_and_definition_use_their_own_ids()
    {
        var classMask = (uint)V2.ISA95MaterialDataTypeFields.MaterialClassID;
        var definitionMask = (uint)V2.ISA95MaterialDataTypeFields.MaterialDefinitionID;
        await SendConfigAsync(new ConfigMaterialClass { MaterialClassID = "mc", EncodingMask = classMask });
        await SendConfigAsync(new ConfigMaterialDefinition { MaterialDefinitionID = "md", EncodingMask = definitionMask });

        (await _store.IsReferenceAsync(JobReferenceKind.MaterialClass, "mc", Scope, Ct)).ShouldBeTrue();
        (await _store.IsReferenceAsync(JobReferenceKind.MaterialDefinition, "md", Scope, Ct)).ShouldBeTrue();

        await SendConfigAsync(new ConfigMaterialClass { MaterialClassID = "mc", EncodingMask = classMask }, "DELETE");
        await SendConfigAsync(new ConfigMaterialDefinition { MaterialDefinitionID = "md", EncodingMask = definitionMask }, "DELETE");
        (await _store.IsReferenceAsync(JobReferenceKind.MaterialClass, "mc", Scope, Ct)).ShouldBeFalse();
        (await _store.IsReferenceAsync(JobReferenceKind.MaterialDefinition, "md", Scope, Ct)).ShouldBeFalse();
    }

    [Fact]
    public async Task Config_personnel_and_physical_asset_are_added_and_deleted()
    {
        await SendConfigAsync(new ConfigPersonnel { ID = "p" });
        await SendConfigAsync(new ConfigPhysicalAsset { ID = "a" });
        (await _store.IsReferenceAsync(JobReferenceKind.Personnel, "p", Scope, Ct)).ShouldBeTrue();
        (await _store.IsReferenceAsync(JobReferenceKind.PhysicalAsset, "a", Scope, Ct)).ShouldBeTrue();

        await SendConfigAsync(new ConfigPersonnel { ID = "p" }, "DELETE");
        await SendConfigAsync(new ConfigPhysicalAsset { ID = "a" }, "DELETE");
        (await _store.IsReferenceAsync(JobReferenceKind.Personnel, "p", Scope, Ct)).ShouldBeFalse();
        (await _store.IsReferenceAsync(JobReferenceKind.PhysicalAsset, "a", Scope, Ct)).ShouldBeFalse();
    }

    [Fact]
    public async Task Config_without_an_id_changes_nothing()
    {
        await SendConfigAsync(new ConfigEquipment());

        (await _store.IsReferenceAsync(JobReferenceKind.Equipment, string.Empty, Scope, Ct)).ShouldBeFalse();
    }

    [Fact]
    public async Task Config_work_master_is_stored_and_deleted()
    {
        await SendConfigAsync(new ConfigWorkMaster { ID = "wm" });
        (await _store.IsWorkMasterAsync("wm", Scope, Ct)).ShouldBeTrue();

        await SendConfigAsync(new ConfigWorkMaster { ID = "wm" }, "DELETE");
        (await _store.IsWorkMasterAsync("wm", Scope, Ct)).ShouldBeFalse();
    }

    [Fact]
    public async Task Config_extended_work_master_keeps_its_recipe()
    {
        await SendConfigAsync(new ConfigWorkMasterExt
        {
            ID = "wm",
            DataSchema = "schema",
            Data = "recipe",
            EncodingMask = (uint)(ISA95WorkMasterExtDataTypeFields.DataSchema | ISA95WorkMasterExtDataTypeFields.Data),
        });

        _store.WorkMaster("wm").ShouldNotBeNull().Data.ShouldBe("recipe");
    }

    [Fact]
    public async Task Config_work_master_of_another_scope_is_left_alone()
    {
        await SendConfigAsync(new ConfigWorkMaster { ID = "wm" });

        await SendConfigAsync(new ConfigWorkMaster { ID = "wm" }, subject: "site.other/station");

        (await _store.IsWorkMasterAsync("wm", "site.other", Ct)).ShouldBeFalse();
        (await _store.IsWorkMasterAsync("wm", Scope, Ct)).ShouldBeTrue();
    }

    [Fact]
    public async Task Config_job_acceptance_is_stored_and_deleted()
    {
        await SendConfigAsync(new ConfigJobAcceptance(3));
        (await _store.GetAcceptanceLimitAsync(Scope, Ct)).ShouldBe(3);

        await SendConfigAsync(new ConfigJobAcceptance(0), "DELETE");
        (await _store.GetAcceptanceLimitAsync(Scope, Ct)).ShouldBeNull();
    }

    [Fact]
    public async Task Config_job_acceptance_with_a_negative_limit_is_ignored()
    {
        await SendConfigAsync(new ConfigJobAcceptance(-1));

        (await _store.GetAcceptanceLimitAsync(Scope, Ct)).ShouldBeNull();
    }

    [Fact]
    public async Task Metadata_is_published_retained_with_the_commands()
    {
        var meta = await WaitForAsync(JobControlTypes.Metadata, 0);

        meta.Retain.ShouldBeTrue();
        var payload = _harness.Decode<JobMetadata>(meta);
        payload.SupportedCommands.Count.ShouldBe(13);
        payload.EventTypes.ShouldBe(["ISA95JobOrderStatusEventType"]);
    }

    [Fact]
    public async Task An_unknown_type_is_dropped_without_a_reply()
    {
        var result = await _harness.SendEnvelopeAsync(new EventEnvelope { Id = "x", Source = "urn:test", Type = "unknown.v1", Subject = Subject, Data = "{}"u8.ToArray() }, Ct);

        result.Outcome.ShouldBe(DeliveryOutcome.Drop);
        result.Published.ShouldBeEmpty();
    }

    private static EventEnvelope Envelope<T>(string type, T call, string id, string subject = Subject)
        where T : class, IEncodeable => new()
        {
            Id = id,
            Source = "urn:test",
            Type = type,
            Subject = subject,
            Data = OpcUaJsonCodec.Encode(call),
        };

    private ulong ReturnStatus<T>(HarnessResult result)
        where T : JobMethodResponseDataType =>
        _harness.Decode<T>(result.Published.Single(m => m.Envelope.Type.EndsWith(".response.v2", StringComparison.Ordinal))).ReturnStatus;

    private async Task SendConfigAsync<T>(T config, string? method = null, string subject = Subject)
        where T : class
    {
        var result = await _harness.SendEnvelopeAsync(ConfigEnvelope(config, method, subject), Ct);
        result.Outcome.ShouldBe(DeliveryOutcome.Ack);
        result.Published.ShouldBeEmpty();
    }

    private EventEnvelope ConfigEnvelope<T>(T config, string? method, string subject)
        where T : class
    {
        var registry = _harness.Services.GetRequiredService<ProcessorRegistry>();
        registry.TryGetDescriptor(typeof(T), out var descriptor).ShouldBeTrue();
        return new EventEnvelope
        {
            Id = Guid.NewGuid().ToString(),
            Source = "urn:test",
            Type = descriptor.TypeId,
            Subject = subject,
            Method = method,
            Data = descriptor.Encode(config, new Dictionary<string, string>(StringComparer.Ordinal)),
        };
    }

    private async Task<RequestJobResponseByJobOrderIdResponse> QueryByIdAsync(RequestJobResponseByJobOrderIdCall call)
    {
        var result = await _harness.SendAsync(call, Subject, Ct);
        return _harness.Decode<RequestJobResponseByJobOrderIdResponse>(result.Published.Single());
    }

    private async Task<RequestJobResponseByJobOrderStateResponse> QueryByStateAsync(RequestJobResponseByJobOrderStateCall call)
    {
        var result = await _harness.SendAsync(call, Subject, Ct);
        return _harness.Decode<RequestJobResponseByJobOrderStateResponse>(result.Published.Single());
    }

    private sealed class NoDedupeStore : IDedupeStore
    {
        public ValueTask<ClaimState> ClaimAsync(string source, string id, string token, CancellationToken cancellationToken) => new(ClaimState.Claimed);

        public ValueTask MarkDoneAsync(string source, string id, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask ReleaseAsync(string source, string id, string token, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask<TimeSpan> LeaseRemainingAsync(string source, string id, CancellationToken cancellationToken) => new(TimeSpan.Zero);
    }

    /// <summary>Builds the neutral order that is converted to the V2 payload, so the encoding masks are right.</summary>
    private sealed class JobOrderBuilder
    {
        public required string Id { get; init; }

        public short Priority { get; set; }

        public string? Equipment { get; set; }

        public string? MaterialClass { get; set; }

        public string? MaterialDefinition { get; set; }

        public string? Personnel { get; set; }

        public string? PhysicalAsset { get; set; }

        public string? WorkMaster { get; set; }

        public void Use(string check)
        {
            switch (check)
            {
                case "equipment":
                    Equipment = "x";
                    break;
                case "materialclass":
                    MaterialClass = "x";
                    break;
                case "materialdefinition":
                    MaterialDefinition = "x";
                    break;
                case "personnel":
                    Personnel = "x";
                    break;
                case "physicalasset":
                    PhysicalAsset = "x";
                    break;
                case "workmaster":
                    WorkMaster = "x";
                    break;
                default:
                    break;
            }
        }

        public JobOrder Build() => new()
        {
            Id = Id,
            Priority = Priority,
            EquipmentRequirements = Equipment is null ? [] : [new JobResource { Id = Equipment }],
            PersonnelRequirements = Personnel is null ? [] : [new JobResource { Id = Personnel }],
            PhysicalAssetRequirements = PhysicalAsset is null ? [] : [new JobResource { Id = PhysicalAsset }],
            MaterialRequirements = MaterialClass is null && MaterialDefinition is null ? [] : [new JobMaterial { MaterialClassId = MaterialClass, MaterialDefinitionId = MaterialDefinition }],
            WorkMasters = WorkMaster is null ? [] : [new JobWorkMaster { Id = WorkMaster }],
        };
    }
}
