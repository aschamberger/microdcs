using System.Collections.Immutable;
using MicroDCS.Core.Messaging;
using MicroDCS.Core.Transport;
using Microsoft.Extensions.Time.Testing;

namespace MicroDCS.Core.Tests;

public sealed class InMemoryTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Dedupe_claim_lease_and_retention_follow_the_clock()
    {
        var time = new FakeTimeProvider();
        var store = new InMemoryDedupeStore(time, TimeSpan.FromSeconds(30), TimeSpan.FromMinutes(10));

        (await store.ClaimAsync("s", "1", "a", Ct)).ShouldBe(ClaimState.Claimed);
        (await store.ClaimAsync("s", "1", "a", Ct)).ShouldBe(ClaimState.Claimed); // same token: a retry is safe
        (await store.ClaimAsync("s", "1", "b", Ct)).ShouldBe(ClaimState.Busy);
        (await store.LeaseRemainingAsync("s", "1", Ct)).ShouldBe(TimeSpan.FromSeconds(30));

        time.Advance(TimeSpan.FromSeconds(31));
        (await store.ClaimAsync("s", "1", "b", Ct)).ShouldBe(ClaimState.Claimed); // lease lapsed

        await store.MarkDoneAsync("s", "1", Ct);
        (await store.ClaimAsync("s", "1", "c", Ct)).ShouldBe(ClaimState.Done);
        (await store.LeaseRemainingAsync("s", "1", Ct)).ShouldBe(TimeSpan.Zero);

        time.Advance(TimeSpan.FromMinutes(11));
        (await store.ClaimAsync("s", "1", "c", Ct)).ShouldBe(ClaimState.Claimed); // retention over
    }

    [Fact]
    public async Task State_topic_keeps_the_last_value_until_it_expires_or_is_deleted()
    {
        var time = new FakeTimeProvider();
        var port = new InMemoryStateTopicPort(time, new SlashAddressRenderer());
        var address = new LogicalAddress("dcs", ["plant1"], null, Intent.Data);

        await port.PublishAsync(address, "order/1", new byte[] { 1 }, TimeSpan.FromHours(1), Ct);
        port.Get(address, "order/1")!.Value.ToArray().ShouldBe(new byte[] { 1 });

        await port.PublishAsync(address, "order/1", new byte[] { 2 }, TimeSpan.FromHours(1), Ct);
        port.Get(address, "order/1")!.Value.ToArray().ShouldBe(new byte[] { 2 });

        time.Advance(TimeSpan.FromHours(2));
        port.Get(address, "order/1").HasValue.ShouldBeFalse();

        await port.PublishAsync(address, "order/2", new byte[] { 3 }, TimeSpan.FromHours(1), Ct);
        await port.DeleteAsync(address, "order/2", Ct);
        port.Get(address, "order/2").HasValue.ShouldBeFalse();
    }

    [Fact]
    public void Renderer_renders_and_parses_addresses()
    {
        var renderer = new SlashAddressRenderer();
        var address = new LogicalAddress("dcs/jobs", ["plant1", "line2"], null, Intent.Command);

        renderer.RenderPublish(address).ShouldBe("dcs/jobs/plant1/line2/commands");
        renderer.TryParse("dcs/jobs", "dcs/jobs/plant1/line2/commands", out var parsed).ShouldBeTrue();
        parsed.ScopePath.ShouldBe(address.ScopePath);
        parsed.Intent.ShouldBe(Intent.Command);
        renderer.TryParse("dcs/jobs", "other/plant1/commands", out _).ShouldBeFalse();
        renderer.TryParse("dcs/jobs", "dcs/jobs/pla+nt/commands", out _).ShouldBeFalse();
        renderer.TryParse("dcs/jobs", "dcs/jobs/plant1/unknown", out _).ShouldBeFalse();
    }

    [Fact]
    public void Renderer_renders_one_subscription_per_scope_depth_with_the_discriminator()
    {
        var subscriptions = new SlashAddressRenderer().RenderSubscriptions(new AddressPattern("dcs", 2, "jobs", Intent.Data, null));

        subscriptions.ShouldBe(["dcs/jobs/data", "dcs/+/jobs/data", "dcs/+/+/jobs/data"]);
    }

    [Fact]
    public async Task Transport_rejects_publish_after_stop_and_inject_when_not_receiving()
    {
        var transport = new InMemoryTransport(DeliveryGuarantee.AtMostOnce);
        var message = new InboundMessage { BindingId = "b", Kind = InboundKind.Request, Envelope = new EventEnvelope { Id = "1", Source = "s", Type = "t" } };

        await Should.ThrowAsync<InvalidOperationException>(async () => await transport.InjectAsync(message, Ct));
        transport.Profile.Guarantee.ShouldBe(DeliveryGuarantee.AtMostOnce);

        await transport.StartAsync(new NoopHandler(), Ct);
        (await transport.InjectAsync(message, Ct)).ShouldBe(DeliveryOutcome.Ack);
        await transport.StopReceivingAsync(Ct);
        await Should.ThrowAsync<InvalidOperationException>(async () => await transport.InjectAsync(message, Ct));

        await transport.StopAsync(Ct);
        await Should.ThrowAsync<InvalidOperationException>(async () =>
            await transport.PublishAsync(new OutboundMessage { BindingId = "b", Envelope = message.Envelope }, Ct));
        transport.Published.ShouldBe(ImmutableArray<OutboundMessage>.Empty);
    }

    private sealed class NoopHandler : IInboundHandler
    {
        public ValueTask<DeliveryOutcome> HandleAsync(InboundMessage message, CancellationToken cancellationToken) => ValueTask.FromResult(DeliveryOutcome.Ack);
    }
}
