using MicroDCS.Core.Messaging;
using MicroDCS.Core.Transport;

namespace MicroDCS.Transport.Nats.Tests;

public sealed class NatsAddressRendererTests
{
    private readonly NatsAddressRenderer _renderer = new();

    [Fact]
    public void A_publish_subject_joins_prefix_scope_discriminator_and_intent_with_dots()
    {
        var address = new LogicalAddress("dcs/jobs", ["site", "area"], "orders", Intent.Command);

        _renderer.RenderPublish(address).ShouldBe("dcs.jobs.site.area.orders.commands");
    }

    [Fact]
    public void Subscriptions_use_a_star_per_scope_level_from_zero_to_the_depth()
    {
        var filters = _renderer.RenderSubscriptions(new AddressPattern("dcs", 2, null, Intent.Event, null));

        filters.ShouldBe(["dcs.events", "dcs.*.events", "dcs.*.*.events"]);
    }

    [Fact]
    public void A_shared_group_does_not_change_the_subject()
    {
        var plain = _renderer.RenderSubscriptions(new AddressPattern("dcs", 1, "d", Intent.Data, null));
        var shared = _renderer.RenderSubscriptions(new AddressPattern("dcs", 1, "d", Intent.Data, "workers"));

        shared.ShouldBe(plain);
    }

    [Fact]
    public void A_received_subject_is_parsed_back_with_the_discriminator()
    {
        NatsAddressRenderer.TryParse("dcs/jobs", "orders", "dcs.jobs.site.area.orders.events", out var address).ShouldBeTrue();

        address.ScopePath.ShouldBe(["site", "area"]);
        address.Discriminator.ShouldBe("orders");
        address.Intent.ShouldBe(Intent.Event);
    }

    [Theory]
    [InlineData("other.site.events")]
    [InlineData("dcs.site.unknown")]
    [InlineData("dcs.si te.events")]
    [InlineData("dcs.>.events")]
    public void A_subject_outside_the_prefix_or_with_an_invalid_segment_is_rejected(string subject)
    {
        NatsAddressRenderer.TryParse("dcs", null, subject, out _).ShouldBeFalse();
    }

    [Fact]
    public void A_wrong_discriminator_is_rejected()
    {
        NatsAddressRenderer.TryParse("dcs", "orders", "dcs.site.other.events", out _).ShouldBeFalse();
    }

    [Theory]
    [InlineData("dcs.*.events", "dcs.site.events", true)]
    [InlineData("dcs.*.events", "dcs.site.area.events", false)]
    [InlineData("dcs.events", "dcs.events", true)]
    [InlineData("dcs.*.events", "dcs.site.data", false)]
    public void Matching_supports_the_single_level_wildcard(string filter, string subject, bool expected)
    {
        NatsAddressRenderer.Matches(filter, subject).ShouldBe(expected);
    }

    [Theory]
    [InlineData("dcs", true)]
    [InlineData("dcs/jobs", true)]
    [InlineData("dcs.jobs", true)]
    [InlineData("dcs/", false)]
    [InlineData("dcs.*", false)]
    [InlineData("dcs jobs", false)]
    public void Prefixes_follow_the_segment_rule_with_either_separator(string prefix, bool valid)
    {
        NatsAddressRenderer.IsValidPrefix(prefix).ShouldBe(valid);
    }

    [Fact]
    public void An_instance_id_with_dots_becomes_a_single_token()
    {
        NatsAddressRenderer.InstanceToken("pod-0.svc.cluster").ShouldBe("pod-0_svc_cluster");
    }
}
