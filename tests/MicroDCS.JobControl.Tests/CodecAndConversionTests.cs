using System.Text;
using System.Text.Json;
using MicroDCS.Core.Messaging;
using MicroDCS.JobControl.Codec;
using MicroDCS.JobControl.Conversions;
using MicroDCS.JobControl.Extension;
using MicroDCS.JobControl.Model;
using MicroDCS.JobControl.StateMachine;
using Opc.Ua;
using V1 = Opc.Ua.ISA95.JobControl.V1;
using V2 = Opc.Ua.ISA95.JobControl.V2;

namespace MicroDCS.JobControl.Tests;

/// <summary>Conversions, the OPC UA JSON codec, the Work Master extension and the schema (D-34).</summary>
public sealed class CodecAndConversionTests
{
    private static readonly DateTimeUtc _start = new(new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc));

    private static EUInformation Unit => new() { NamespaceUri = "http://www.opcfoundation.org/UA/units/un/cefact", UnitId = 4408652, DisplayName = new LocalizedText("en", "mm"), Description = new LocalizedText("en", "millimetre") };

    private static V2.ISA95JobOrderDataType FullV2Order()
    {
        var neutral = new JobOrder
        {
            Id = "12345",
            Description = [new LocalizedText("en", "Woodworking"), new LocalizedText("de", "Holzbearbeitung")],
            WorkMasters = [new JobWorkMaster { Id = "wm1", Description = [new LocalizedText("en", "plan")], Parameters = [new JobParameter { Id = "p", Value = new Variant(1.5), EngineeringUnits = Unit }] }],
            Priority = 3,
            StartTime = _start,
            Parameters = [new JobParameter { Id = "outer", Value = new Variant("x"), Description = [new LocalizedText("en", "d")], EngineeringUnits = Unit, Subparameters = [new JobParameter { Id = "inner", Value = new Variant(7) }] }],
            EquipmentRequirements = [new JobResource { Id = "eq", Use = "Produced", Quantity = "2", EngineeringUnits = Unit, Properties = [new JobProperty { Id = "pr", Value = new Variant(true), Subproperties = [new JobProperty { Id = "sub", Value = new Variant("s") }] }] }],
            PersonnelRequirements = [new JobResource { Id = "pe", Description = [new LocalizedText("en", "worker")] }],
            PhysicalAssetRequirements = [new JobResource { Id = "pa" }],
            MaterialRequirements = [new JobMaterial { MaterialClassId = "mc", MaterialDefinitionId = "md", MaterialLotId = "ml", MaterialSublotId = "ms", Use = "Consumed", Quantity = "4", EngineeringUnits = Unit, Description = [new LocalizedText("en", "wood")], Properties = [new JobProperty { Id = "grain" }] }],
        };
        return JobControlConversions.ToV2Order(neutral);
    }

    private static string Canonical(byte[] json) => Encoding.UTF8.GetString(json);

    [Fact]
    public void V2_order_survives_a_same_version_round_trip()
    {
        var order = FullV2Order();
        var encoded = OpcUaJsonCodec.Encode(order);

        var decoded = OpcUaJsonCodec.Decode<V2.ISA95JobOrderDataType>(encoded);
        var back = JobControlConversions.ToV2Order(JobControlConversions.FromV2Order(decoded));

        Canonical(OpcUaJsonCodec.Encode(back)).ShouldBe(Canonical(encoded));
    }

    [Fact]
    public void V2_encoding_mask_is_set_for_present_fields_only()
    {
        var order = JobControlConversions.ToV2Order(new JobOrder { Id = "a", Priority = 1 });

        order.EncodingMask.ShouldBe((uint)V2.ISA95JobOrderDataTypeFields.Priority);
        var json = Canonical(OpcUaJsonCodec.Encode(order));
        json.ShouldContain("\"Priority\":1");
        json.ShouldNotContain("Description");
    }

    [Fact]
    public void V2_response_round_trips_with_state_and_actuals()
    {
        var response = JobControlConversions.ToV2Response(new JobResponse
        {
            Id = "r1",
            JobOrderId = "12345",
            State = JobOrderState.NotAllowedToStartLoaded,
            ResponseData = [new JobParameter { Id = "yield", Value = new Variant(0.9) }],
            MaterialActuals = [new JobMaterial { MaterialDefinitionId = "md" }],
        });

        var decoded = JobControlConversions.FromV2Response(OpcUaJsonCodec.Decode<V2.ISA95JobResponseDataType>(OpcUaJsonCodec.Encode(response)));

        decoded.Id.ShouldBe("r1");
        decoded.JobOrderId.ShouldBe("12345");
        decoded.State.ShouldBe(JobOrderState.NotAllowedToStartLoaded);
        decoded.ResponseData[0].Id.ShouldBe("yield");
        decoded.MaterialActuals[0].MaterialDefinitionId.ShouldBe("md");
    }

    [Fact]
    public void V1_order_survives_a_same_version_round_trip()
    {
        var order = JobControlConversions.ToV1Order(JobControlConversions.FromV2Order(FullV2Order()));
        var encoded = OpcUaJsonCodec.Encode(order);

        var back = JobControlConversions.ToV1Order(JobControlConversions.FromV1Order(OpcUaJsonCodec.Decode<V1.ISA95JobOrderDataType>(encoded)));

        Canonical(OpcUaJsonCodec.Encode(back)).ShouldBe(Canonical(encoded));
    }

    [Fact]
    public void Cross_version_view_keeps_what_v1_can_represent()
    {
        var v1 = JobControlConversions.ToV1Order(JobControlConversions.FromV2Order(FullV2Order()));

        v1.ID.ShouldBe("12345");
        v1.Description.ShouldBe("Woodworking");
        v1.Priority.ShouldBe((short)3);
        v1.EquipmentRequirements[0].UoM.ShouldBe("mm");
    }

    [Theory]
    [InlineData(V1.ISA95JobOrderStateEnum.Waiting, JobOrderState.NotAllowedToStartWaiting)]
    [InlineData(V1.ISA95JobOrderStateEnum.Ready, JobOrderState.AllowedToStartReady)]
    [InlineData(V1.ISA95JobOrderStateEnum.Loaded, JobOrderState.V1Loaded)]
    [InlineData(V1.ISA95JobOrderStateEnum.Running, JobOrderState.Running)]
    [InlineData(V1.ISA95JobOrderStateEnum.Held, JobOrderState.Held)]
    [InlineData(V1.ISA95JobOrderStateEnum.Suspended, JobOrderState.Suspended)]
    [InlineData(V1.ISA95JobOrderStateEnum.Completed, JobOrderState.Completed)]
    [InlineData(V1.ISA95JobOrderStateEnum.Aborted, JobOrderState.Aborted)]
    [InlineData(V1.ISA95JobOrderStateEnum.Closed, JobOrderState.Closed)]
    [InlineData(V1.ISA95JobOrderStateEnum.Error, JobOrderState.V1Error)]
    public void V1_states_round_trip(V1.ISA95JobOrderStateEnum v1, JobOrderState neutral)
    {
        JobControlConversions.FromV1State(v1).ShouldBe(neutral);
        JobControlConversions.ToV1State(neutral).ShouldBe(v1);
    }

    [Theory]
    [InlineData(JobOrderState.NotAllowedToStartLoaded, V1.ISA95JobOrderStateEnum.Waiting)]
    [InlineData(JobOrderState.AllowedToStartLoaded, V1.ISA95JobOrderStateEnum.Ready)]
    [InlineData(JobOrderState.Initial, V1.ISA95JobOrderStateEnum.Undefined)]
    public void Prepare_sub_states_collapse_in_a_v1_view(JobOrderState neutral, V1.ISA95JobOrderStateEnum v1) =>
        JobControlConversions.ToV1State(neutral).ShouldBe(v1);

    [Theory]
    [InlineData(V1.ISA95JobOrderCommandEnum.Store, JobOrderOperation.Store)]
    [InlineData(V1.ISA95JobOrderCommandEnum.StoreAndStart, JobOrderOperation.StoreAndStart)]
    [InlineData(V1.ISA95JobOrderCommandEnum.Start, JobOrderOperation.Start)]
    [InlineData(V1.ISA95JobOrderCommandEnum.Update, JobOrderOperation.Update)]
    [InlineData(V1.ISA95JobOrderCommandEnum.Stop, JobOrderOperation.Stop)]
    [InlineData(V1.ISA95JobOrderCommandEnum.Cancel, JobOrderOperation.Cancel)]
    [InlineData(V1.ISA95JobOrderCommandEnum.Clear, JobOrderOperation.Clear)]
    public void V1_commands_map(V1.ISA95JobOrderCommandEnum command, JobOrderOperation operation)
    {
        JobControlConversions.TryMapV1Command(command, out var mapped).ShouldBeTrue();
        mapped.ShouldBe(operation);
    }

    [Fact]
    public void Undefined_v1_command_does_not_map() =>
        JobControlConversions.TryMapV1Command(V1.ISA95JobOrderCommandEnum.Undefined, out _).ShouldBeFalse();

    [Fact]
    public void Root_is_valid_json_with_plain_field_names()
    {
        using var document = JsonDocument.Parse(OpcUaJsonCodec.Encode(FullV2Order()));

        document.RootElement.GetProperty("JobOrderID").GetString().ShouldBe("12345");
    }

    [Fact]
    public void Unknown_fields_are_ignored_and_missing_ones_keep_defaults()
    {
        var order = OpcUaJsonCodec.Decode<V2.ISA95JobOrderDataType>("""{"JobOrderID":"a","Nope":1}"""u8.ToArray());

        order.JobOrderID.ShouldBe("a");
        order.Priority.ShouldBe((short)0);
    }

    [Fact]
    public void Decode_errors_do_not_echo_values()
    {
        var exception = Should.Throw<PayloadException>(() => OpcUaJsonCodec.Decode<V2.ISA95JobOrderDataType>("""{"Priority":"topsecret"}"""u8.ToArray()));

        exception.Message.ShouldNotContain("topsecret");
    }

    [Theory]
    [InlineData("{not json")]
    [InlineData("")]
    [InlineData("[1,2]")]
    public void Malformed_payloads_are_payload_errors(string payload) =>
        Should.Throw<PayloadException>(() => OpcUaJsonCodec.Decode<V2.ISA95JobOrderDataType>(Encoding.UTF8.GetBytes(payload)));

    [Fact]
    public void V1_response_keeps_its_v1_only_states_and_actuals()
    {
        var response = JobControlConversions.ToV1Response(new JobResponse
        {
            Id = "r1",
            JobOrderId = "12345",
            Description = [new LocalizedText("en", "done")],
            State = JobOrderState.V1Error,
            ResponseData = [new JobParameter { Id = "yield", Value = new Variant(0.9), EngineeringUnits = Unit, Subparameters = [new JobParameter { Id = "s" }] }],
            PersonnelActuals = [new JobResource { Id = "pe", Properties = [new JobProperty { Id = "p", EngineeringUnits = Unit }] }],
            EquipmentActuals = [new JobResource { Id = "eq" }],
            PhysicalAssetActuals = [new JobResource { Id = "pa" }],
            MaterialActuals = [new JobMaterial { MaterialDefinitionId = "md", EngineeringUnits = Unit }],
        });
        var encoded = OpcUaJsonCodec.Encode(response);

        var neutral = JobControlConversions.FromV1Response(OpcUaJsonCodec.Decode<V1.ISA95JobResponseDataType>(encoded));

        neutral.State.ShouldBe(JobOrderState.V1Error);
        neutral.ResponseData[0].EngineeringUnits!.DisplayName.Text.ShouldBe("mm");
        Canonical(OpcUaJsonCodec.Encode(JobControlConversions.ToV1Response(neutral))).ShouldBe(Canonical(encoded));
    }

    [Fact]
    public void V2_response_with_all_fields_is_lossless()
    {
        var response = JobControlConversions.ToV2Response(new JobResponse
        {
            Id = "r1",
            JobOrderId = "12345",
            Description = [new LocalizedText("en", "done")],
            StartTime = _start,
            EndTime = _start,
            State = JobOrderState.Closed,
            ResponseData = [new JobParameter { Id = "yield", Value = new Variant(0.9), EngineeringUnits = Unit, Subparameters = [new JobParameter { Id = "s" }] }],
            PersonnelActuals = [new JobResource { Id = "pe", Properties = [new JobProperty { Id = "p", EngineeringUnits = Unit, Subproperties = [new JobProperty { Id = "q" }] }] }],
            EquipmentActuals = [new JobResource { Id = "eq", Use = "u", Quantity = "1", EngineeringUnits = Unit }],
            PhysicalAssetActuals = [new JobResource { Id = "pa" }],
            MaterialActuals = [new JobMaterial { MaterialClassId = "mc", Use = "u", Quantity = "1", EngineeringUnits = Unit, Properties = [new JobProperty { Id = "p" }] }],
        });
        var encoded = OpcUaJsonCodec.Encode(response);

        var back = JobControlConversions.ToV2Response(JobControlConversions.FromV2Response(OpcUaJsonCodec.Decode<V2.ISA95JobResponseDataType>(encoded)));

        Canonical(OpcUaJsonCodec.Encode(back)).ShouldBe(Canonical(encoded));
    }

    [Fact]
    public void Validation_requires_ids()
    {
        JobControlValidation.Validate(new JobOrder { Id = "a" }).ShouldBe(JobReturnStatus.Success);
        JobControlValidation.Validate(new JobOrder { Id = string.Empty }).ShouldBe(JobReturnStatus.InvalidRequest);
        JobControlValidation.Validate(new JobResponse { Id = "r", JobOrderId = string.Empty }).ShouldBe(JobReturnStatus.InvalidRequest);
        JobControlValidation.Validate(new JobResponse { Id = "r", JobOrderId = "a" }).ShouldBe(JobReturnStatus.Success);
    }

    [Fact]
    public void Extended_work_master_round_trips_as_the_subtype()
    {
        var ext = JobControlConversions.ToWorkMasterExt(new JobWorkMaster { Id = "wm1", DataSchema = "urn:recipe", Data = "{\"steps\":[]}" });

        var neutral = JobControlConversions.FromWorkMasterExt(OpcUaJsonCodec.Decode<ISA95WorkMasterExtDataType>(OpcUaJsonCodec.Encode(ext)));

        neutral.Id.ShouldBe("wm1");
        neutral.DataSchema.ShouldBe("urn:recipe");
        neutral.Data.ShouldBe("{\"steps\":[]}");
    }

    [Fact]
    public void Plain_work_master_has_no_extension_fields()
    {
        var ext = JobControlConversions.ToWorkMasterExt(new JobWorkMaster { Id = "wm1" });

        var json = Canonical(OpcUaJsonCodec.Encode(ext));

        json.ShouldNotContain("DataSchema");
    }

    [Fact]
    public void Descriptor_round_trips_and_rejects_other_types()
    {
        var descriptor = new OpcUaJsonPayloadDescriptor<ISA95WorkMasterExtDataType>(JobControlTypes.WorkMasterExt, "urn:schema");
        var value = JobControlConversions.ToWorkMasterExt(new JobWorkMaster { Id = "wm1", Data = "d" });

        var encoded = descriptor.Encode(value, new Dictionary<string, string>());
        var decoded = (ISA95WorkMasterExtDataType)descriptor.Decode(encoded, new Dictionary<string, string>());

        decoded.Data.ShouldBe("d");
        descriptor.TypeId.ShouldBe(JobControlTypes.WorkMasterExt);
        descriptor.Schema.ShouldBe("urn:schema");
        descriptor.CarriedNames.ShouldBeEmpty();
        Should.Throw<PayloadException>(() => descriptor.Encode("x", new Dictionary<string, string>()));
    }

    [Fact]
    public void Json_schema_describes_the_generated_types()
    {
        JobControlSchemas.GenerateJson<V2.ISA95JobOrderDataType>().ShouldContain("JobOrderID");

        var ext = JobControlSchemas.GenerateJson<ISA95WorkMasterExtDataType>();
        ext.ShouldContain("DataSchema");
        ext.ShouldContain("Data");
    }
}
