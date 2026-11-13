using System.Buffers;
using System.Text;
using MicroDCS.IntegrationTests.Mqtt;
using MicroDCS.JobControl.Codec;
using MicroDCS.JobControl.Conversions;
using MicroDCS.JobControl.Extension;
using MicroDCS.JobControl.Model;
using MicroDCS.JobControl.StateMachine;
using MicroDCS.MachineryJobs;
using MicroDCS.Sfc;
using MQTTnet;
using Opc.Ua;
using V2 = Opc.Ua.ISA95.JobControl.V2;

namespace MicroDCS.IntegrationTests.Acceptance;

/// <summary>
/// Parity acceptance (S23, Q-TEST-2): the Python integration scenarios against the application image, driven only over MQTT as an
/// MES and the equipment would. The image runs as the <c>app</c> service of the <c>image</c> profile of <c>compose.test.yaml</c>
/// (every role, greetings and Machinery Jobs bindings, SFC host); <c>scripts/image-tests</c> runs this class. Each test uses its own
/// scope, so the tests share the instance without seeing each other's jobs. The parity table is in
/// <c>docs/porting/research/parity-acceptance.md</c>.
/// </summary>
[Trait("Category", "Image")]
public sealed class AppAcceptanceTests : IAsyncLifetime
{
    private const string Skip = $"{TestEndpoints.AppVariable} or {TestEndpoints.MqttVariable} is not set";
    private const string HelloType = "com.github.aschamberger.microdcs.greetings.hello.v1";
    private const string ByeType = "com.github.aschamberger.microdcs.greetings.bye.v1";
    private const string RawType = "com.github.aschamberger.microdcs.greetings.raw.v1";
    private static readonly TimeSpan _patience = TimeSpan.FromSeconds(30);

    public static bool Configured => TestEndpoints.AppConfigured;

    private static string Prefix => TestEndpoints.App!;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>
    /// Waits until the image answers a hello. The image has no shell for a compose health check, and an event published before its
    /// subscriptions exist is lost, so the first test would otherwise race the start.
    /// </summary>
    /// <returns>A task.</returns>
    public async ValueTask InitializeAsync()
    {
        if (!Configured)
        {
            return;
        }

        await using var station = await Station.OpenAsync();
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        deadline.CancelAfter(TimeSpan.FromSeconds(120));
        while (true)
        {
            deadline.IsCancellationRequested.ShouldBeFalse("The application image did not answer a hello.");
            await station.Client.PublishEventAsync(station.Events, Guid.NewGuid().ToString(), HelloType, station.Scope, "{\"Name\":\"Ready\"}", station.Replies, Ct);
            using var attempt = CancellationTokenSource.CreateLinkedTokenSource(deadline.Token);
            attempt.CancelAfter(TimeSpan.FromSeconds(2));
            try
            {
                await station.Client.ReceiveAsync(attempt.Token);
                return;
            }
            catch (OperationCanceledException) when (!Ct.IsCancellationRequested)
            {
                // Not subscribed yet; send again.
            }
        }
    }

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private static string Json<T>(T value)
        where T : class, IEncodeable => Encoding.UTF8.GetString(OpcUaJsonCodec.Encode(value));

    private static StoreCall Store(string id, string? workMaster = null) => new()
    {
        JobOrder = JobControlConversions.ToV2Order(new JobOrder
        {
            Id = id,
            WorkMasters = workMaster is null ? [] : [new JobWorkMaster { Id = workMaster }],
        }),
    };

    private static ulong Success => (ulong)JobReturnStatus.Success;

    /// <summary>One scope of the station, its topics and a test client subscribed to the replies.</summary>
    private sealed class Station : IAsyncDisposable
    {
        private readonly List<MqttApplicationMessage> _held = [];

        private Station(MqttTestClient client, string site)
        {
            Client = client;
            Site = site;
        }

        public MqttTestClient Client { get; }

        public string Site { get; }

        public string Scope => $"{Site}.area";

        /// <summary>
        /// A job or event id of this station. Job identity is the id alone (D-42) and the dedupe claim holds an event id for its TTL, so
        /// ids must not repeat across runs against one Valkey.
        /// </summary>
        public string Id(string name) => $"{name}-{Site}";

        public string Events => $"{Prefix}/{Site}/area/events";

        public string Commands => $"{Prefix}/{Site}/area/commands";

        public string MesCommands => $"{Prefix}-mes/{Site}/area/commands";

        public string Data => $"{Prefix}-mes/{Site}/area/data";

        public string Replies => $"{Prefix}-test/{Site}/replies";

        public static async Task<Station> OpenAsync()
        {
            var client = await MqttTestClient.ConnectAsync(null, Ct);
            var station = new Station(client, "s" + Guid.NewGuid().ToString("N")[..10]);
            await client.SubscribeAsync(station.Replies, Ct);
            return station;
        }

        /// <summary>Next message that matches; others are held for a later call, since replies and events can arrive in any order.</summary>
        public async Task<MqttApplicationMessage> ReceiveAsync(Func<MqttApplicationMessage, bool> match, string what)
        {
            var held = _held.Find(m => match(m));
            if (held is not null)
            {
                _held.Remove(held);
                return held;
            }

            using var deadline = CancellationTokenSource.CreateLinkedTokenSource(Ct);
            deadline.CancelAfter(_patience);
            while (true)
            {
                MqttApplicationMessage message;
                try
                {
                    message = await Client.ReceiveAsync(deadline.Token);
                }
                catch (OperationCanceledException) when (!Ct.IsCancellationRequested)
                {
                    if (deadline.IsCancellationRequested)
                    {
                        throw new TimeoutException($"Nothing arrived for: {what}");
                    }

                    // The client's own receive window (15 s) ended before this deadline; keep waiting.
                    continue;
                }

                if (match(message))
                {
                    return message;
                }

                _held.Add(message);
            }
        }

        public Task<MqttApplicationMessage> ReplyAsync(string type, string causationId) =>
            ReceiveAsync(m => m.Topic == Replies && MqttTestClient.Property(m, "type") == type && MqttTestClient.Property(m, "causationid") == causationId, type);

        /// <summary>Sends a Machinery Jobs command and returns its reply.</summary>
        public async Task<MqttApplicationMessage> CallAsync(string type, string json, string responseType)
        {
            var id = Guid.NewGuid().ToString();
            await Client.PublishEventAsync(MesCommands, id, type, Scope, json, Replies, Ct);
            return await ReplyAsync(responseType, id);
        }

        /// <summary>
        /// Next state index on the data topics that matches, including indexes held from earlier receives. A predicate that an older index
        /// could also satisfy (a job missing, a count) must name a sequence after a known index.
        /// </summary>
        public async Task<JobOrderStateIndexDataType> IndexAsync(Func<JobOrderStateIndexDataType, bool> match, string what)
        {
            var message = await ReceiveAsync(
                m => m.Topic == $"{Data}/state-index" && m.Payload.Length > 0 && match(DecodeIndex(m)),
                what);
            return DecodeIndex(message);
        }

        public async ValueTask DisposeAsync() => await Client.DisposeAsync();
    }

    private static JobOrderStateIndexDataType DecodeIndex(MqttApplicationMessage message) =>
        OpcUaJsonCodec.Decode<JobOrderStateIndexDataType>(message.Payload.ToArray());

    private static JobOrderStateIndexEntryDataType[] Entries(JobOrderStateIndexDataType index) => index.Jobs.ToArray() ?? [];

    private static bool Lists(JobOrderStateIndexDataType index, string job) => Entries(index).Any(j => j.JobOrderID == job);

    /// <summary>Reads the retained value of a topic as a new subscriber, the way an MES resyncs; null when the topic holds nothing.</summary>
    private static async Task<MqttApplicationMessage?> RetainedAsync(string topic)
    {
        await using var late = await MqttTestClient.ConnectAsync(null, Ct);
        await late.SubscribeAsync(topic, Ct);
        using var wait = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        wait.CancelAfter(TimeSpan.FromSeconds(2));
        try
        {
            var message = await late.ReceiveAsync(wait.Token);
            message.Retain.ShouldBeTrue();
            return message;
        }
        catch (OperationCanceledException) when (!Ct.IsCancellationRequested)
        {
            return null;
        }
    }

    // Python: test_mqtt_integration.py test_publish_raw_greetings_message
    [Fact(SkipUnless = nameof(Configured), SkipType = typeof(AppAcceptanceTests), Skip = Skip)]
    public async Task An_event_without_a_handler_is_echoed_with_the_same_data()
    {
        await using var station = await Station.OpenAsync();
        const string Payload = "{\"message\":\"Hello, MQTT!\",\"some_number\":\"42\",\"something_else\":\"foo\",\"list_example\":[1,2,3]}";

        await station.Client.PublishEventAsync(station.Events, station.Id("raw"), "com.example.unknown.v1", station.Scope, Payload, station.Replies, Ct);

        var echo = await station.ReplyAsync(RawType, station.Id("raw"));
        Encoding.UTF8.GetString(echo.Payload).ShouldBe(Payload);
        echo.ContentType.ShouldBe("application/json");
        MqttTestClient.Property(echo, "subject").ShouldBe(station.Scope);
    }

    // Python: test_mqtt_integration.py test_publish_hello_greetings_message
    [Fact(SkipUnless = nameof(Configured), SkipType = typeof(AppAcceptanceTests), Skip = Skip)]
    public async Task A_hello_is_answered_with_two_hellos()
    {
        await using var station = await Station.OpenAsync();

        await station.Client.PublishEventAsync(station.Events, station.Id("hello"), HelloType, station.Scope, "{\"Name\":\"Bob\"}", station.Replies, Ct);

        var names = new List<string>();
        for (var i = 0; i < 2; i++)
        {
            var reply = await station.ReplyAsync(HelloType, station.Id("hello"));
            names.Add(Encoding.UTF8.GetString(reply.Payload));
        }

        names.ShouldBe(["{\"Name\":\"Bob\"}", "{\"Name\":\"Alice\"}"], ignoreOrder: true);
    }

    // Python: test_mqtt_integration.py test_publish_hello_with_additional_payload_fields (hidden fields became a carried attribute, E-4)
    [Fact(SkipUnless = nameof(Configured), SkipType = typeof(AppAcceptanceTests), Skip = Skip)]
    public async Task Unknown_fields_are_ignored_and_the_carried_attribute_comes_back()
    {
        await using var station = await Station.OpenAsync();
        var attributes = new Dictionary<string, string> { ["hiddenstr"] = "This is a hidden string" };

        await station.Client.PublishEventAsync(station.Events, station.Id("hello"), HelloType, station.Scope, "{\"Name\":\"Bob\",\"addition\":\"42\"}", station.Replies, Ct, attributes: attributes);

        var replies = new List<MqttApplicationMessage>();
        for (var i = 0; i < 2; i++)
        {
            replies.Add(await station.ReplyAsync(HelloType, station.Id("hello")));
        }

        // The unknown field is not in either reply; the carried attribute goes to every reply whose type declares it (D-35), Alice's too.
        replies.Select(r => Encoding.UTF8.GetString(r.Payload)).ShouldBe(["{\"Name\":\"Bob\"}", "{\"Name\":\"Alice\"}"], ignoreOrder: true);
        replies.ShouldAllBe(r => MqttTestClient.Property(r, "hiddenstr") == "This is a hidden string");
    }

    // Python: test_mqtt_integration.py test_publish_store_job_order_message
    [Fact(SkipUnless = nameof(Configured), SkipType = typeof(AppAcceptanceTests), Skip = Skip)]
    public async Task A_store_is_answered_with_a_store_response()
    {
        await using var station = await Station.OpenAsync();

        var reply = await station.CallAsync(JobControlTypes.Store, Json(Store(station.Id("store"))), JobControlTypes.StoreResponse);

        OpcUaJsonCodec.Decode<StoreResponse>(reply.Payload.ToArray()).ReturnStatus.ShouldBe(Success);
    }

    // Python: test_mqtt_integration.py test_publish_store_and_start_job_order_message
    [Fact(SkipUnless = nameof(Configured), SkipType = typeof(AppAcceptanceTests), Skip = Skip)]
    public async Task A_store_and_start_is_answered_with_its_response()
    {
        await using var station = await Station.OpenAsync();

        var reply = await station.CallAsync(JobControlTypes.StoreAndStart, Json(new StoreAndStartCall { JobOrder = Store(station.Id("store-and-start")).JobOrder }), JobControlTypes.StoreAndStartResponse);

        OpcUaJsonCodec.Decode<StoreAndStartResponse>(reply.Payload.ToArray()).ReturnStatus.ShouldBe(Success);
    }

    // Python: test_mqtt_integration.py test_publish_start_job_order_message
    [Fact(SkipUnless = nameof(Configured), SkipType = typeof(AppAcceptanceTests), Skip = Skip)]
    public async Task A_stored_order_can_be_started()
    {
        await using var station = await Station.OpenAsync();
        var job = station.Id("start");
        var stored = await station.CallAsync(JobControlTypes.Store, Json(Store(job)), JobControlTypes.StoreResponse);
        OpcUaJsonCodec.Decode<StoreResponse>(stored.Payload.ToArray()).ReturnStatus.ShouldBe(Success);

        var reply = await station.CallAsync(JobControlTypes.Start, Json(new StartCall { JobOrderID = job }), JobControlTypes.StartResponse);

        OpcUaJsonCodec.Decode<StartResponse>(reply.Payload.ToArray()).ReturnStatus.ShouldBe(Success);
    }

    // Python: test_mqtt_integration.py test_publish_store_job_order_raw_json. The wire text is OPC UA JSON (D-2, D-34): property values are
    // Variants with their UaType, not plain JSON values.
    [Fact(SkipUnless = nameof(Configured), SkipType = typeof(AppAcceptanceTests), Skip = Skip)]
    public async Task A_store_written_by_hand_in_opc_ua_json_is_accepted()
    {
        await using var station = await Station.OpenAsync();
        await station.Client.SubscribeAsync($"{station.Data}/state-index", Ct);
        var job = station.Id("raw-json");
        var call = $$$"""
            {"JobOrder":{"JobOrderID":"{{{job}}}",
              "Description":[{"Text":"Order_Forest_Utilize_01","Locale":"en"}],
              "StartTime":"2023-01-27T10:17:00Z","EndTime":"2023-01-27T10:19:00Z",
              "MaterialRequirements":[
                {"MaterialUse":"Consumed","Quantity":"1","Properties":[{"ID":"Param_771","Value":{"UaType":12,"Value":"200"}}]},
                {"MaterialLotID":"Forest_Spruce_ShelfFloor_1245","MaterialUse":"Produced","Quantity":"1",
                 "Properties":[{"ID":"Param_1234","Value":{"UaType":12,"Value":"2500"}}]}]},
             "Comment":[{"Text":"Store woodworking job (raw JSON)","Locale":"en"}]}
            """;

        var reply = await station.CallAsync(JobControlTypes.Store, call, JobControlTypes.StoreResponse);

        OpcUaJsonCodec.Decode<StoreResponse>(reply.Payload.ToArray()).ReturnStatus.ShouldBe(Success);

        // The publisher writes the order topic before the index of the same round, so once the index lists the job the topic is retained.
        await station.IndexAsync(i => Lists(i, job), "the job in the index");
        var order = (await RetainedAsync($"{station.Data}/order/{job}")).ShouldNotBeNull();
        var published = OpcUaJsonCodec.Decode<V2.ISA95JobOrderDataType>(order.Payload.ToArray());
        published.MaterialRequirements.Count.ShouldBe(2);
        published.MaterialRequirements[1].MaterialLotID.ShouldBe("Forest_Spruce_ShelfFloor_1245");
    }

    // Python: test_app_sfc_integration.py test_example_app_completes_sfc_recipe_via_greetings_response, then
    // test_mes_integration.py test_result_published_for_clearable_job and test_clear_deletes_retained_topics on the same job.
    [Fact(SkipUnless = nameof(Configured), SkipType = typeof(AppAcceptanceTests), Skip = Skip)]
    public async Task A_recipe_runs_through_the_equipment_and_the_mes_reads_and_clears_the_result()
    {
        await using var station = await Station.OpenAsync();
        await station.Client.SubscribeAsync(station.Commands, Ct);
        await station.Client.SubscribeAsync($"{station.Data}/#", Ct);
        var job = station.Id("job-sfc");
        var workMasterId = station.Id("wm-sfc");
        var recipe = $$"""
            {"Steps":[{"Name":"s1","Initial":true}],"Transitions":[],
             "Actions":[{"Name":"a1","Step":"s1","Qualifier":"N","Interaction":"push_command","TypeId":"{{ByeType}}","TimeoutSeconds":30,"MaxAttempts":3}]}
            """;
        var workMaster = JobControlConversions.ToWorkMasterExt(new JobWorkMaster { Id = workMasterId, DataSchema = SfcInterpreter.RecipeDataSchema, Data = recipe });
        var config = new ConfigWorkMasterExt { ID = workMaster.ID, DataSchema = workMaster.DataSchema, Data = workMaster.Data, EncodingMask = workMaster.EncodingMask };
        await station.Client.PublishEventAsync(station.MesCommands, station.Id("config"), JobControlTypes.WorkMasterExt, station.Scope, Json(config), null, Ct);

        // The configuration has no reply; the order is refused until the work master is stored (CheckWorkMaster).
        var startCall = Json(new StoreAndStartCall { JobOrder = Store(job, workMasterId).JobOrder });
        // There is no signal for that, so the order is offered again at a short interval; any other refusal fails at once.
        var deadline = TimeProvider.System.GetUtcNow() + _patience;
        ulong status;
        while ((status = OpcUaJsonCodec.Decode<StoreAndStartResponse>((await station.CallAsync(JobControlTypes.StoreAndStart, startCall, JobControlTypes.StoreAndStartResponse)).Payload.ToArray()).ReturnStatus) != Success)
        {
            status.ShouldBe((ulong)JobReturnStatus.UnableToAccept);
            (TimeProvider.System.GetUtcNow() < deadline).ShouldBeTrue("The work master was not stored.");
            await Task.Delay(TimeSpan.FromMilliseconds(200), TimeProvider.System, Ct);
        }

        // The equipment: the bye command arrives with the per-instance reply topic and is answered with a hello.
        var command = await station.ReceiveAsync(m => m.Topic == station.Commands && MqttTestClient.Property(m, "type") == ByeType, "the bye command");
        MqttTestClient.Property(command, "mdcsactionkey").ShouldBe($"{job}:a1");
        command.ResponseTopic.ShouldNotBeNullOrEmpty();
        await station.Client.PublishEventAsync(command.ResponseTopic, Guid.NewGuid().ToString(), HelloType, station.Scope, "{\"Name\":\"Plant\"}", null, Ct, causationId: MqttTestClient.Property(command, "id"));

        // The MES: the result topic appears and the index marks the job as having a result.
        var result = await station.ReceiveAsync(m => m.Topic == $"{station.Data}/result/{job}" && m.Payload.Length > 0, "the result topic");
        OpcUaJsonCodec.Decode<V2.ISA95JobResponseDataType>(result.Payload.ToArray()).JobOrderID.ShouldBe(job);
        var completed = await station.IndexAsync(i => Entries(i).Any(j => j.JobOrderID == job && j.HasResult && JobStateArray.FromV2(j.State) == JobOrderState.Completed), "the completed job in the index");

        var byId = await station.CallAsync(JobControlTypes.RequestJobResponseByJobOrderId, Json(new RequestJobResponseByJobOrderIdCall { JobOrderID = job }), JobControlTypes.RequestJobResponseByJobOrderIdResponse);
        var response = OpcUaJsonCodec.Decode<RequestJobResponseByJobOrderIdResponse>(byId.Payload.ToArray());
        response.ReturnStatus.ShouldBe(Success);
        response.JobResponse.JobOrderID.ShouldBe(job);

        var cleared = await station.CallAsync(JobControlTypes.Clear, Json(new ClearCall { JobOrderID = job }), JobControlTypes.ClearResponse);
        OpcUaJsonCodec.Decode<ClearResponse>(cleared.Payload.ToArray()).ReturnStatus.ShouldBe(Success);
        await station.ReceiveAsync(m => m.Topic == $"{station.Data}/order/{job}" && m.Payload.Length == 0, "the order topic to be deleted");
        await station.ReceiveAsync(m => m.Topic == $"{station.Data}/result/{job}" && m.Payload.Length == 0, "the result topic to be deleted");
        await station.IndexAsync(i => i.Sequence > completed.Sequence && !Lists(i, job), "a later index without the job");
        (await RetainedAsync($"{station.Data}/order/{job}")).ShouldBeNull();
        (await RetainedAsync($"{station.Data}/result/{job}")).ShouldBeNull();
    }

    // Python: test_mes_integration.py test_store_publishes_retained_order and test_resync_detects_sequence_gap
    [Fact(SkipUnless = nameof(Configured), SkipType = typeof(AppAcceptanceTests), Skip = Skip)]
    public async Task A_late_mes_reads_the_retained_topics_and_sees_the_sequence_move()
    {
        await using var station = await Station.OpenAsync();
        var (job1, job2, job3) = (station.Id("resync-1"), station.Id("resync-2"), station.Id("resync-3"));
        await station.Client.SubscribeAsync($"{station.Data}/state-index", Ct);
        await station.CallAsync(JobControlTypes.Store, Json(Store(job1)), JobControlTypes.StoreResponse);
        await station.IndexAsync(i => Lists(i, job1), "the first job in the index");

        var first = DecodeIndex((await RetainedAsync($"{station.Data}/state-index")).ShouldNotBeNull());
        first.Scope.ShouldBe(station.Scope);
        first.Jobs.Count.ShouldBe(1);
        var order = (await RetainedAsync($"{station.Data}/order/{job1}")).ShouldNotBeNull();
        OpcUaJsonCodec.Decode<V2.ISA95JobOrderDataType>(order.Payload.ToArray()).JobOrderID.ShouldBe(job1);

        // The MES is away while two more orders arrive.
        await station.CallAsync(JobControlTypes.Store, Json(Store(job2)), JobControlTypes.StoreResponse);
        await station.CallAsync(JobControlTypes.Store, Json(Store(job3)), JobControlTypes.StoreResponse);
        await station.IndexAsync(i => i.Jobs.Count == 3, "all three jobs in the index");

        var later = DecodeIndex((await RetainedAsync($"{station.Data}/state-index")).ShouldNotBeNull());
        later.Sequence.ShouldBeGreaterThan(first.Sequence);
        Entries(later).Select(j => j.JobOrderID).ShouldBe([job1, job2, job3], ignoreOrder: true);
        (await RetainedAsync($"{station.Data}/order/{job3}")).ShouldNotBeNull();
    }

    // Python: test_mes_integration.py test_multi_job_cadence
    [Fact(SkipUnless = nameof(Configured), SkipType = typeof(AppAcceptanceTests), Skip = Skip)]
    public async Task Ten_orders_stored_in_a_row_are_all_published()
    {
        await using var station = await Station.OpenAsync();
        await station.Client.SubscribeAsync($"{station.Data}/state-index", Ct);
        var jobs = Enumerable.Range(0, 10).Select(i => station.Id($"cadence-{i:D3}")).ToArray();
        var ids = new List<string>();
        foreach (var job in jobs)
        {
            var id = Guid.NewGuid().ToString();
            ids.Add(id);
            await station.Client.PublishEventAsync(station.MesCommands, id, JobControlTypes.Store, station.Scope, Json(Store(job)), station.Replies, Ct);
        }

        foreach (var id in ids)
        {
            OpcUaJsonCodec.Decode<StoreResponse>((await station.ReplyAsync(JobControlTypes.StoreResponse, id)).Payload.ToArray()).ReturnStatus.ShouldBe(Success);
        }

        await station.IndexAsync(i => i.Jobs.Count == jobs.Length, "all ten jobs in the index");
        foreach (var job in jobs)
        {
            (await RetainedAsync($"{station.Data}/order/{job}")).ShouldNotBeNull($"no retained order for {job}");
        }
    }
}
