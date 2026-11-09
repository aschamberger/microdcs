using System.Text;
using MicroDCS.Core.Hosting;
using MicroDCS.Core.Inspection;
using MicroDCS.Core.Messaging;
using MicroDCS.Core.Transport;
using MicroDCS.JobControl.Codec;
using MicroDCS.JobControl.Extension;
using MicroDCS.JobControl.Model;
using MicroDCS.JobControl.StateMachine;
using MicroDCS.MachineryJobs.Publishing;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using NSubstitute;
using V2 = Opc.Ua.ISA95.JobControl.V2;

namespace MicroDCS.MachineryJobs.Tests;

/// <summary>The Python publisher tests (<c>test_job_order_publisher.py</c>) on the state-topic port, plus the deliberate changes of D-51.</summary>
public sealed class JobOrderPublisherTests : IDisposable
{
    private const string Scope = "site.area";

    private readonly InMemoryJobStore _store = new();
    private readonly InMemoryPublisherState _state = new();
    private readonly FakeTimeProvider _time = new(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));
    private readonly InMemoryTransport _transport;
    private readonly JobStateTopics _topics;
    private readonly MicroDcsOptions _core = new() { InstanceId = "pub-1" };
    private readonly JobPublisherOptions _options = new();
    private JobOrderPublisher _publisher;

    public JobOrderPublisherTests()
    {
        _transport = new InMemoryTransport(time: _time);
        _core.Bindings[MachineryJobsProcessor.BindingId] = new BindingOptions { Transport = "memory", Prefix = "app", ScopeLevels = 2 };
        _topics = new JobStateTopics(_store, [_transport], Options.Create(_core));
        _publisher = NewPublisher();
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private LogicalAddress Address => _topics.AddressOf(Scope);

    public void Dispose() => _publisher.Dispose();

    private JobOrderPublisher NewPublisher() => new(
        _store,
        _state,
        _topics,
        Options.Create(_options),
        Options.Create(_core),
        Substitute.For<IHostApplicationLifetime>(),
        _time,
        NullLogger<JobOrderPublisher>.Instance);

    private async Task SaveAsync(string id, JobOrderState state, string change = "Store", string scope = Scope)
    {
        var version = _store.Order(id)?.Version ?? 0;
        await _store.SaveOrderAsync(new JobOrderWrite(new JobOrder { Id = id }, state, scope, change, version, "op", JobRecipeWork.None), Ct);
        _state.Append(scope, change, id);
    }

    private async Task SaveResultAsync(string id)
    {
        await _store.SaveResponseAsync(new JobResponse { Id = "r-" + id, JobOrderId = id }, Scope, Ct);
        _state.Append(Scope, "ResultUpdate", id);
    }

    private async Task RemoveAsync(string id, string change)
    {
        await _store.RemoveOrderAsync(id, Scope, change, _store.Order(id)!.Version, "op-remove", Ct);
        _state.Append(Scope, change, id);
    }

    private async Task RoundAsync(string scope = Scope)
    {
        await _publisher.RenewLeaseAsync(Ct);
        await _publisher.PollAsync(scope, Ct);
        await _publisher.RunRoundAsync(Ct);
    }

    private ReadOnlyMemory<byte>? Topic(string name) => _transport.StateTopics.Get(Address, name);

    private JobOrderStateIndexDataType Index() => OpcUaJsonCodec.Decode<JobOrderStateIndexDataType>(Topic(JobStateTopics.StateIndex)!.Value);

    [Fact]
    public async Task A_store_publishes_the_order_and_the_state_index()
    {
        await SaveAsync("jo-1", JobOrderState.NotAllowedToStartReady);

        await RoundAsync();

        OpcUaJsonCodec.Decode<V2.ISA95JobOrderDataType>(Topic("order/jo-1")!.Value).JobOrderID.ShouldBe("jo-1");
        var index = Index();
        index.Scope.ShouldBe(Scope);
        index.Sequence.ShouldBe(1);
        var entry = index.Jobs.ToArray().ShouldHaveSingleItem();
        entry.JobOrderID.ShouldBe("jo-1");
        entry.HasResult.ShouldBeFalse();
    }

    [Fact]
    public async Task The_order_topic_carries_no_state()
    {
        await SaveAsync("jo-1", JobOrderState.Running);

        await RoundAsync();

        Encoding.UTF8.GetString(Topic("order/jo-1")!.Value.Span).ShouldNotContain("\"State\"");
    }

    [Theory]
    [InlineData("StoreAndStart")]
    [InlineData("Update")]
    public async Task Other_order_changes_publish_the_order(string change)
    {
        await RoundAsync();
        await SaveAsync("jo-1", JobOrderState.AllowedToStartReady, change);

        await RoundAsync();

        Topic("order/jo-1").ShouldNotBeNull();
    }

    [Fact]
    public async Task A_transition_updates_only_the_state_index()
    {
        await SaveAsync("jo-1", JobOrderState.AllowedToStartReady);
        await RoundAsync();
        var order = Topic("order/jo-1")!.Value;

        await SaveAsync("jo-1", JobOrderState.Running, "Start");
        await RoundAsync();

        Topic("order/jo-1")!.Value.ShouldBe(order);
        Index().Sequence.ShouldBe(2);
        Index().Jobs.ToArray().ShouldHaveSingleItem().State.ToArray().ShouldHaveSingleItem().StateText.Text.ShouldBe("Running");
    }

    [Fact]
    public async Task Changes_of_one_batch_publish_one_state_index()
    {
        await SaveAsync("jo-1", JobOrderState.AllowedToStartReady);
        await SaveAsync("jo-2", JobOrderState.AllowedToStartReady);
        await SaveAsync("jo-1", JobOrderState.Running, "Start");

        await RoundAsync();

        Index().Sequence.ShouldBe(1);
        Index().Jobs.Count.ShouldBe(2);
    }

    [Theory]
    [InlineData("Cancel")]
    [InlineData("Clear")]
    public async Task A_removal_deletes_the_topics_of_the_order(string change)
    {
        await SaveAsync("jo-1", JobOrderState.Aborted);
        await SaveResultAsync("jo-1");
        await RoundAsync();
        Topic("result/jo-1").ShouldNotBeNull();

        await RemoveAsync("jo-1", change);
        await RoundAsync();

        Topic("order/jo-1").ShouldBeNull();
        Topic("result/jo-1").ShouldBeNull();
        Index().Jobs.ToArray().ShouldBeEmpty();
    }

    [Theory]
    [InlineData(JobOrderState.Completed)]
    [InlineData(JobOrderState.Closed)]
    [InlineData(JobOrderState.Aborted)]
    public async Task A_result_is_published_once_the_job_has_ended(JobOrderState state)
    {
        await SaveAsync("jo-1", state);
        await RoundAsync();

        await SaveResultAsync("jo-1");
        await RoundAsync();

        OpcUaJsonCodec.Decode<V2.ISA95JobResponseDataType>(Topic("result/jo-1")!.Value).JobOrderID.ShouldBe("jo-1");
        Index().Jobs.ToArray().ShouldHaveSingleItem().HasResult.ShouldBeTrue();
    }

    [Fact]
    public async Task A_running_job_publishes_no_result()
    {
        await SaveAsync("jo-1", JobOrderState.Running);
        await SaveResultAsync("jo-1");

        await RoundAsync();

        Topic("result/jo-1").ShouldBeNull();
        Index().Jobs.ToArray().ShouldHaveSingleItem().HasResult.ShouldBeFalse();
    }

    [Fact]
    public async Task A_result_stored_before_the_end_is_published_when_the_job_ends()
    {
        await SaveAsync("jo-1", JobOrderState.Running);
        await SaveResultAsync("jo-1");
        await RoundAsync();

        await SaveAsync("jo-1", JobOrderState.Completed, "Stop");
        await RoundAsync();

        Topic("result/jo-1").ShouldNotBeNull();
    }

    [Fact]
    public async Task A_removal_and_a_new_store_in_one_batch_drop_the_old_result()
    {
        await SaveAsync("jo-1", JobOrderState.Aborted);
        await SaveResultAsync("jo-1");
        await RoundAsync();
        await RemoveAsync("jo-1", "Clear");
        await _store.SaveResponseAsync(new JobResponse { Id = "r-new", JobOrderId = "other" }, Scope, Ct);
        await SaveAsync("jo-1", JobOrderState.NotAllowedToStartReady);

        await RoundAsync();

        Topic("result/jo-1").ShouldBeNull();
        Topic("order/jo-1").ShouldNotBeNull();
    }

    [Fact]
    public async Task An_undecodable_response_does_not_hold_back_the_scope()
    {
        await RoundAsync();
        await SaveAsync("jo-1", JobOrderState.Completed);
        await SaveAsync("jo-2", JobOrderState.Running);
        _store.FailDecodingResponseOf = "jo-1";

        await RoundAsync();

        Topic("order/jo-2").ShouldNotBeNull();
        _state.Cursor(Scope).ShouldBe(_state.LastId(Scope));
    }

    [Fact]
    public async Task A_lost_lease_stops_the_round_and_leaves_the_cursor()
    {
        await RoundAsync();
        await SaveAsync("jo-1", JobOrderState.Running);
        var cursor = _state.Cursor(Scope);
        _state.BeforeFirstSequence = async () =>
        {
            await _state.ReleaseLeaseAsync("pub-1", Ct);
            await _state.AcquireLeaseAsync("other", _options.LeaseTtl, Ct);
            await _publisher.RenewLeaseAsync(Ct);
        };
        _store.AfterOrderRead = () => _state.BeforeFirstSequence?.Invoke().AsTask().GetAwaiter().GetResult();
        await _publisher.PollAsync(Scope, Ct);

        await _publisher.RunRoundAsync(Ct);

        _publisher.IsActive.ShouldBeFalse();
        _state.Cursor(Scope).ShouldBe(cursor);
    }

    [Fact]
    public async Task A_failed_renewal_keeps_the_lease_until_it_runs_out()
    {
        await SaveAsync("jo-1", JobOrderState.Running);
        await RoundAsync();
        _state.FailLease = true;

        await _publisher.RenewLeaseAsync(Ct);

        _publisher.IsActive.ShouldBeTrue();

        _state.FailLease = false;
        await RoundAsync();
        Index().Sequence.ShouldBe(1); // No rebuild after a short failure.

        _state.FailLease = true;
        _time.Advance(_options.LeaseTtl);
        await _publisher.RenewLeaseAsync(Ct);
        _publisher.IsActive.ShouldBeFalse();
    }

    [Fact]
    public async Task A_cancellation_from_the_transport_is_a_failed_round_not_the_end()
    {
        await SaveAsync("jo-1", JobOrderState.Running);
        var failing = Substitute.For<ITransport, IStateTopicPort>();
        failing.Name.Returns("memory");
        failing.Profile.Returns(new DeliveryProfile(DeliveryGuarantee.AtLeastOnce, true, false, true, false));
        ((IStateTopicPort)failing)
            .When(p => p.PublishAsync(Arg.Any<LogicalAddress>(), Arg.Any<string>(), Arg.Any<ReadOnlyMemory<byte>>(), Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>()).AsTask())
            .Do(_ => throw new OperationCanceledException("timeout"));
        var topics = new JobStateTopics(_store, [failing], Options.Create(_core));
        using var publisher = new JobOrderPublisher(_store, _state, topics, Options.Create(_options), Options.Create(_core), Substitute.For<IHostApplicationLifetime>(), _time, NullLogger<JobOrderPublisher>.Instance);
        await publisher.RenewLeaseAsync(Ct);
        await publisher.PollAsync(Scope, Ct);

        await publisher.RunRoundAsync(Ct);

        _state.Cursor(Scope).ShouldBeNull();
    }

    [Fact]
    public async Task The_first_entry_after_an_empty_stream_is_no_gap()
    {
        await RoundAsync();
        _state.Cursor(Scope).ShouldBe("0-0");
        await SaveAsync("jo-1", JobOrderState.Running);

        await RoundAsync();

        _state.GapChecked.ShouldBeFalse();
        Topic("order/jo-1").ShouldNotBeNull();
    }

    [Fact]
    public async Task A_change_of_a_missing_order_is_skipped()
    {
        await RoundAsync();
        _state.Append(Scope, "Update", "gone");

        await RoundAsync();

        Topic("order/gone").ShouldBeNull();
    }

    [Fact]
    public async Task The_first_visit_rebuilds_all_orders_of_the_scope()
    {
        await SaveAsync("jo-1", JobOrderState.Running);
        await RoundAsync();
        _publisher.Dispose();
        _transport.StateTopics.Clear();
        _publisher = NewPublisher();

        await RoundAsync();

        Topic("order/jo-1").ShouldNotBeNull();
        Topic(JobStateTopics.StateIndex).ShouldNotBeNull();
    }

    [Fact]
    public async Task An_idle_scope_is_not_published_again()
    {
        await SaveAsync("jo-1", JobOrderState.Running);
        await RoundAsync();

        await RoundAsync();
        await RoundAsync();

        Index().Sequence.ShouldBe(1);
    }

    [Fact]
    public async Task A_scope_without_entries_gets_a_cursor_and_an_index_once()
    {
        await RoundAsync();
        await RoundAsync();

        Index().Sequence.ShouldBe(1);
        _state.Cursor(Scope).ShouldBe("0-0");
    }

    [Fact]
    public async Task The_cursor_advances_after_the_batch()
    {
        await SaveAsync("jo-1", JobOrderState.Running);

        await RoundAsync();

        _state.Cursor(Scope).ShouldBe(_state.LastId(Scope));
    }

    [Fact]
    public async Task A_failed_publish_keeps_the_cursor_and_is_tried_again()
    {
        await SaveAsync("jo-1", JobOrderState.Running);
        var failing = Substitute.For<ITransport, IStateTopicPort>();
        failing.Name.Returns("memory");
        failing.Profile.Returns(new DeliveryProfile(DeliveryGuarantee.AtLeastOnce, true, false, true, false));
        ((IStateTopicPort)failing)
            .When(p => p.PublishAsync(Arg.Any<LogicalAddress>(), Arg.Any<string>(), Arg.Any<ReadOnlyMemory<byte>>(), Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>()).AsTask())
            .Do(_ => throw new InvalidOperationException("broker"));
        var topics = new JobStateTopics(_store, [failing], Options.Create(_core));
        using var publisher = new JobOrderPublisher(_store, _state, topics, Options.Create(_options), Options.Create(_core), Substitute.For<IHostApplicationLifetime>(), _time, NullLogger<JobOrderPublisher>.Instance);
        await publisher.RenewLeaseAsync(Ct);
        await publisher.PollAsync(Scope, Ct);

        await publisher.RunRoundAsync(Ct);

        _state.Cursor(Scope).ShouldBeNull();

        await RoundAsync();
        _state.Cursor(Scope).ShouldBe(_state.LastId(Scope));
    }

    [Fact]
    public async Task A_full_batch_reads_again_in_the_next_round()
    {
        _options.ReadCount = 1;
        await SaveAsync("jo-1", JobOrderState.Running);
        await SaveAsync("jo-2", JobOrderState.Running);

        await RoundAsync();
        Topic("order/jo-2").ShouldNotBeNull(); // The first visit rebuilds from the documents.
        _state.Cursor(Scope).ShouldNotBe(_state.LastId(Scope));

        await _publisher.RunRoundAsync(Ct);
        _state.Cursor(Scope).ShouldBe(_state.LastId(Scope));
    }

    [Fact]
    public async Task A_gap_in_the_stream_rebuilds_the_scope()
    {
        await SaveAsync("jo-1", JobOrderState.Running);
        await RoundAsync();
        await SaveAsync("jo-2", JobOrderState.Running);
        await SaveAsync("jo-3", JobOrderState.Running, "Start");
        _transport.StateTopics.Clear();
        _state.TrimBefore(Scope, _state.LastId(Scope)!);

        await RoundAsync();

        Topic("order/jo-1").ShouldNotBeNull();
        Topic("order/jo-2").ShouldNotBeNull();
    }

    [Fact]
    public async Task Topics_are_refreshed_before_they_run_out()
    {
        await SaveAsync("jo-1", JobOrderState.Running);
        await RoundAsync();

        _time.Advance(_options.RetainedTtl / 2);
        await RoundAsync();

        Index().Sequence.ShouldBe(2);
        _time.Advance((_options.RetainedTtl / 2) + TimeSpan.FromMinutes(1));
        Topic("order/jo-1").ShouldNotBeNull();
    }

    [Fact]
    public async Task An_instance_without_the_lease_publishes_nothing()
    {
        await _state.AcquireLeaseAsync("other", _options.LeaseTtl, Ct);
        await SaveAsync("jo-1", JobOrderState.Running);

        await RoundAsync();

        _publisher.IsActive.ShouldBeFalse();
        Topic("order/jo-1").ShouldBeNull();
        _state.Cursor(Scope).ShouldBeNull();
    }

    [Fact]
    public async Task A_new_lease_holder_rebuilds_the_scope()
    {
        await SaveAsync("jo-1", JobOrderState.Running);
        await RoundAsync();
        await _state.ReleaseLeaseAsync("pub-1", Ct);
        await _state.AcquireLeaseAsync("other", _options.LeaseTtl, Ct);
        await _publisher.RenewLeaseAsync(Ct);
        await _state.ReleaseLeaseAsync("other", Ct);

        await RoundAsync();

        _publisher.IsActive.ShouldBeTrue();
        Index().Sequence.ShouldBe(2);
    }

    [Fact]
    public async Task The_poller_only_marks_the_scope()
    {
        await SaveAsync("jo-1", JobOrderState.Running);
        await _publisher.RenewLeaseAsync(Ct);

        (await _publisher.PollAsync(Scope, Ct)).ShouldBeFalse();

        Topic("order/jo-1").ShouldBeNull();
    }

    [Fact]
    public async Task An_instance_without_the_publisher_role_ignores_scopes()
    {
        _core.Roles = InstanceRoles.Processor;
        _publisher.Dispose();
        _publisher = NewPublisher();
        await SaveAsync("jo-1", JobOrderState.Running);

        await RoundAsync();

        Topic("order/jo-1").ShouldBeNull();
    }

    [Fact]
    public async Task An_id_no_topic_can_carry_does_not_hold_back_the_scope()
    {
        await SaveAsync("bad.id", JobOrderState.Running);
        await SaveAsync("jo-1", JobOrderState.Running);

        await RoundAsync();

        Topic("order/jo-1").ShouldNotBeNull();
        _state.Cursor(Scope).ShouldBe(_state.LastId(Scope));
    }

    [Fact]
    public async Task A_transport_without_state_topics_ends_the_start()
    {
        var plain = Substitute.For<ITransport>();
        plain.Name.Returns("memory");
        plain.Profile.Returns(new DeliveryProfile(DeliveryGuarantee.AtLeastOnce, true, false, false, false));
        var topics = new JobStateTopics(_store, [plain], Options.Create(_core));
        using var publisher = new JobOrderPublisher(_store, _state, topics, Options.Create(_options), Options.Create(_core), Substitute.For<IHostApplicationLifetime>(), _time, NullLogger<JobOrderPublisher>.Instance);

        await Should.ThrowAsync<InvalidOperationException>(() => publisher.StartAsync(Ct));
    }

    [Theory]
    [InlineData("1-5", "1-5", 0)]
    [InlineData("2-0", "1-9", 1)]
    [InlineData("10-0", "9-0", 1)]
    [InlineData("1-2", "1-10", -1)]
    public void Stream_ids_compare_as_numbers(string left, string right, int sign) =>
        Math.Sign(JobOrderPublisher.CompareIds(left, right)).ShouldBe(sign);

    [Fact]
    public async Task The_catalog_lists_the_topics_of_a_scope_or_an_order()
    {
        await SaveAsync("jo-1", JobOrderState.Running);

        (await _topics.ListAsync(Scope, null, Ct)).ShouldBe(["state-index", "order/jo-1", "result/jo-1"]);
        (await _topics.ListAsync(Scope, "gone", Ct)).ShouldBe(["order/gone", "result/gone"]);
        (await _topics.ListAsync(Scope, "bad.id", Ct)).ShouldBeEmpty();
        (await _topics.ListAsync("a/b", null, Ct)).ShouldBeEmpty();
        (await _topics.ListAsync("site.#", "jo-1", Ct)).ShouldBeEmpty();
    }

    [Fact]
    public async Task The_inspector_reports_lease_cursor_and_sequence()
    {
        await SaveAsync("jo-1", JobOrderState.Running);
        await RoundAsync();
        var inspector = new JobPublisherInspector(_publisher, _state, _topics);

        var page = await inspector.InspectAsync(new InspectionQuery(Kind: "publisher.status", Arguments: new Dictionary<string, string>(StringComparer.Ordinal) { ["scope"] = Scope }), Ct);

        var fields = page.Entries.ShouldHaveSingleItem().Fields;
        fields["active"].Text.ShouldBe("true");
        fields["leaseHolder"].Text.ShouldBe("pub-1");
        fields["cursor"].Text.ShouldBe(_state.LastId(Scope));
        fields["sequence"].Text.ShouldBe("1");
        fields["topics"].Text.ShouldContain("order/jo-1");
    }

    private sealed class InMemoryPublisherState : IJobPublisherStore
    {
        private readonly Dictionary<string, string> _cursors = new(StringComparer.Ordinal);
        private readonly Dictionary<string, List<JobChange>> _streams = new(StringComparer.Ordinal);
        private readonly Dictionary<string, long> _sequences = new(StringComparer.Ordinal);
        private string? _holder;
        private long _nextId = 1;

        public void Append(string scope, string change, string id)
        {
            if (!_streams.TryGetValue(scope, out var stream))
            {
                _streams[scope] = stream = [];
            }

            stream.Add(new JobChange($"{_nextId++}-0", change, id));
        }

        public void TrimBefore(string scope, string id) => _streams[scope].RemoveAll(c => JobOrderPublisher.CompareIds(c.Id, id) < 0);

        public bool FailLease { get; set; }

        public bool GapChecked { get; private set; }

        public Func<ValueTask>? BeforeFirstSequence { get; set; }

        public string? Cursor(string scope) => _cursors.GetValueOrDefault(scope);

        public string? LastId(string scope) => _streams.GetValueOrDefault(scope)?[^1].Id;

        public ValueTask<string?> GetCursorAsync(string scope, CancellationToken cancellationToken) => new(Cursor(scope));

        public ValueTask SaveCursorAsync(string scope, string id, CancellationToken cancellationToken)
        {
            _cursors[scope] = id;
            return ValueTask.CompletedTask;
        }

        public ValueTask<string?> FirstChangeIdAsync(string scope, CancellationToken cancellationToken)
        {
            GapChecked = true;
            return new(_streams.GetValueOrDefault(scope)?.FirstOrDefault()?.Id);
        }

        public ValueTask<string?> LastChangeIdAsync(string scope, CancellationToken cancellationToken) => new(LastId(scope));

        public ValueTask<IReadOnlyList<JobChange>> ReadChangesAsync(string scope, string? after, int count, CancellationToken cancellationToken) =>
            new([.. (_streams.GetValueOrDefault(scope) ?? []).Where(c => after is null || JobOrderPublisher.CompareIds(c.Id, after) > 0).Take(count)]);

        public ValueTask<long> NextSequenceAsync(string scope, CancellationToken cancellationToken)
        {
            var next = _sequences.GetValueOrDefault(scope) + 1;
            _sequences[scope] = next;
            return new(next);
        }

        public ValueTask<long> GetSequenceAsync(string scope, CancellationToken cancellationToken) => new(_sequences.GetValueOrDefault(scope));

        public ValueTask<bool> AcquireLeaseAsync(string holder, TimeSpan timeToLive, CancellationToken cancellationToken)
        {
            if (FailLease)
            {
                throw new InvalidOperationException("valkey");
            }

            _holder ??= holder;
            return new(_holder == holder);
        }

        public ValueTask ReleaseLeaseAsync(string holder, CancellationToken cancellationToken)
        {
            if (_holder == holder)
            {
                _holder = null;
            }

            return ValueTask.CompletedTask;
        }

        public ValueTask<string?> GetLeaseHolderAsync(CancellationToken cancellationToken) => new(_holder);
    }
}
