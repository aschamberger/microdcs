using MicroDCS.Core.Messaging;
using MicroDCS.Core.Transport;
using MicroDCS.Transport.Mqtt;

namespace MicroDCS.Transport.Mqtt.Tests;

public sealed class MqttAddressRendererTests
{
    private readonly MqttAddressRenderer _renderer = new();

    [Fact]
    public void Publish_address_has_scope_discriminator_and_intent()
    {
        var address = new LogicalAddress("dcs/jobs", ["site", "area"], "mes", Intent.Command);

        _renderer.RenderPublish(address).ShouldBe("dcs/jobs/site/area/mes/commands");
    }

    [Fact]
    public void Subscriptions_cover_every_scope_depth()
    {
        var pattern = new AddressPattern("dcs/jobs", 2, null, Intent.Event, null);

        _renderer.RenderSubscriptions(pattern).ShouldBe(["dcs/jobs/events", "dcs/jobs/+/events", "dcs/jobs/+/+/events"]);
    }

    [Fact]
    public void Shared_group_is_rendered_natively()
    {
        var pattern = new AddressPattern("dcs/jobs", 1, "mes", Intent.Command, "workers");

        _renderer.RenderSubscriptions(pattern).ShouldBe(["$share/workers/dcs/jobs/mes/commands", "$share/workers/dcs/jobs/+/mes/commands"]);
    }

    [Theory]
    [InlineData("a/b")]
    [InlineData("a+")]
    [InlineData("#")]
    [InlineData("")]
    public void Shared_group_must_be_a_plain_segment(string group)
    {
        var pattern = new AddressPattern("dcs", 0, null, Intent.Data, group);

        Should.Throw<ArgumentException>(() => _renderer.RenderSubscriptions(pattern));
    }

    [Fact]
    public void Parse_returns_scope_without_discriminator()
    {
        MqttAddressRenderer.TryParse("dcs/jobs", "mes", "dcs/jobs/site/area/mes/commands", out var address).ShouldBeTrue();

        address.ScopePath.ShouldBe(["site", "area"]);
        address.Discriminator.ShouldBe("mes");
        address.Intent.ShouldBe(Intent.Command);
    }

    [Theory]
    [InlineData("other/site/commands")]
    [InlineData("dcs/jobs/site/unknown")]
    [InlineData("dcs/jobs/si te/commands")]
    [InlineData("dcs/jobs/site/commands")]
    public void Parse_rejects_foreign_or_malformed_topics_for_a_discriminator(string topic)
    {
        MqttAddressRenderer.TryParse("dcs/jobs", "mes", topic, out _).ShouldBeFalse();
    }

    [Fact]
    public void Level_zero_topic_has_an_empty_scope()
    {
        _renderer.TryParse("dcs/jobs", "dcs/jobs/events", out var address).ShouldBeTrue();

        address.ScopePath.ShouldBeEmpty();
    }

    [Theory]
    [InlineData("dcs/+/events", "dcs/site/events", true)]
    [InlineData("dcs/+/events", "dcs/site/area/events", false)]
    [InlineData("dcs/events", "dcs/events", true)]
    [InlineData("dcs/events", "dcs/data", false)]
    [InlineData("$share/g/dcs/+/events", "dcs/site/events", true)]
    [InlineData("$share/g/dcs/+/events", "$share/g/dcs/site/events", false)]
    public void Matches_follows_the_mqtt_wildcard_rule(string filter, string topic, bool expected)
    {
        MqttAddressRenderer.Matches(filter, topic).ShouldBe(expected);
    }
}
