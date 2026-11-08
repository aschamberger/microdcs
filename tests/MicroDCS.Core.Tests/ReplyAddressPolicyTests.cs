using MicroDCS.Core.Hosting;
using MicroDCS.Core.Transport;

namespace MicroDCS.Core.Tests;

/// <summary>D-48 (RV-18): the reply address of a peer before the instance publishes to it.</summary>
public sealed class ReplyAddressPolicyTests
{
    private static readonly ReplyAddressRules _nats = new('.', "*>", ["dcs", "plant.a"], ["dcs-reply"]);
    private static readonly ReplyAddressRules _mqtt = new('/', "+#", ["dcs"], ["dcs/reply"]);

    [Theory]
    [InlineData("_INBOX.abc.1")]
    [InlineData("mes.replies.line1")]
    [InlineData("dcs-reply.i1")]
    [InlineData("dcsx.other")]
    public void NATS_addresses_outside_the_own_space_are_allowed(string reply) =>
        ReplyAddressPolicy.Check(reply, _nats, new BindingOptions()).ShouldBeNull();

    [Theory]
    [InlineData("$JS.API.STREAM.DELETE.MDCS_dcs", "system namespace")]
    [InlineData("$KV.microdcs-state.dcs.p1.state", "system namespace")]
    [InlineData("mes.*", "wildcard")]
    [InlineData("mes.>", "wildcard")]
    [InlineData("mes..x", "empty segment")]
    [InlineData("mes x", "empty or white space")]
    [InlineData("", "empty or white space")]
    [InlineData("dcs.p1.evt", "below a binding prefix of this instance")]
    [InlineData("plant.a", "below a binding prefix of this instance")]
    public void NATS_addresses_in_system_or_own_space_are_rejected(string reply, string reason) =>
        ReplyAddressPolicy.Check(reply, _nats, new BindingOptions()).ShouldBe(reason);

    [Theory]
    [InlineData("dcs/p1/state/job", "below a binding prefix of this instance")]
    [InlineData("mes/+/x", "wildcard")]
    [InlineData("$SYS/broker", "system namespace")]
    public void MQTT_rules_use_its_separator_and_wildcards(string reply, string reason) =>
        ReplyAddressPolicy.Check(reply, _mqtt, new BindingOptions()).ShouldBe(reason);

    [Fact]
    public void An_own_reply_base_below_an_own_prefix_stays_allowed() =>
        ReplyAddressPolicy.Check("dcs/reply/i1", _mqtt, new BindingOptions()).ShouldBeNull();

    [Fact]
    public void AllowedReplyPrefixes_restrict_further_but_not_the_own_reply_base()
    {
        var binding = new BindingOptions { AllowedReplyPrefixes = "mes/replies/, _INBOX/" };

        ReplyAddressPolicy.Check("mes/replies/line1", _mqtt, binding).ShouldBeNull();
        ReplyAddressPolicy.Check("other/replies", _mqtt, binding).ShouldBe("not in AllowedReplyPrefixes");
        ReplyAddressPolicy.Check("dcs/reply/i1", _mqtt, binding).ShouldBeNull();
    }
}

/// <summary>D-49: a credential file as it is at the moment of the connect.</summary>
public sealed class SecretFileTests : IDisposable
{
    private readonly string _file = Path.GetTempFileName();

    public void Dispose() => File.Delete(_file);

    [Theory]
    [InlineData("secret", "secret")]
    [InlineData("secret\n", "secret")]
    [InlineData("secret\r\n", "secret")]
    [InlineData("secret\n\n", "secret\n")]
    public async Task One_trailing_line_break_is_removed(string content, string expected)
    {
        await File.WriteAllTextAsync(_file, content, TestContext.Current.CancellationToken);

        SecretFile.Read(_file).ShouldBe(expected);
    }

    [Fact]
    public async Task Every_read_sees_the_current_content()
    {
        await File.WriteAllTextAsync(_file, "first", TestContext.Current.CancellationToken);
        SecretFile.Read(_file).ShouldBe("first");

        await File.WriteAllTextAsync(_file, "rotated", TestContext.Current.CancellationToken);
        SecretFile.Read(_file).ShouldBe("rotated");
    }
}
