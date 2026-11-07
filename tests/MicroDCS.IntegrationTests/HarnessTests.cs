using System.Net.Sockets;
using System.Text;

namespace MicroDCS.IntegrationTests;

/// <summary>
/// Proves the compose path (D-15): the tests container reaches Valkey and the MQTT broker over the
/// compose network. Raw protocol frames on purpose; the client libraries arrive in S04 and later.
/// </summary>
[Trait("Category", "Integration")]
public sealed class HarnessTests
{
    [Fact(SkipUnless = nameof(TestEndpoints.ValkeyConfigured), SkipType = typeof(TestEndpoints),
        Skip = $"{TestEndpoints.ValkeyVariable} is not set")]
    public async Task ValkeyAnswersPing()
    {
        var ct = TestContext.Current.CancellationToken;
        var (host, port) = TestEndpoints.Valkey!.Value; // SkipUnless guarantees a value.

        using var client = new TcpClient();
        await client.ConnectAsync(host, port, ct);
        var stream = client.GetStream();
        await stream.WriteAsync("*1\r\n$4\r\nPING\r\n"u8.ToArray(), ct);

        var buffer = new byte[64];
        var read = await stream.ReadAsync(buffer, ct);
        Encoding.ASCII.GetString(buffer, 0, read).ShouldBe("+PONG\r\n");
    }

    [Fact(SkipUnless = nameof(TestEndpoints.MqttConfigured), SkipType = typeof(TestEndpoints),
        Skip = $"{TestEndpoints.MqttVariable} is not set")]
    public async Task MqttBrokerAcceptsMqtt5Connect()
    {
        var ct = TestContext.Current.CancellationToken;
        var (host, port) = TestEndpoints.Mqtt!.Value; // SkipUnless guarantees a value.

        using var client = new TcpClient();
        await client.ConnectAsync(host, port, ct);
        var stream = client.GetStream();

        // MQTT 5 CONNECT: protocol "MQTT", level 5, clean start, keep alive 10 s, no properties,
        // client id "s02-harness".
        byte[] connect =
        [
            0x10, 0x18,
            0x00, 0x04, (byte)'M', (byte)'Q', (byte)'T', (byte)'T',
            0x05, 0x02, 0x00, 0x0A, 0x00,
            0x00, 0x0B, .. "s02-harness"u8,
        ];
        await stream.WriteAsync(connect, ct);

        var buffer = new byte[64];
        var read = await stream.ReadAsync(buffer, ct);
        read.ShouldBeGreaterThanOrEqualTo(4);
        buffer[0].ShouldBe((byte)0x20, "packet type CONNACK");
        buffer[3].ShouldBe((byte)0x00, "reason code Success");

        await stream.WriteAsync(new byte[] { 0xE0, 0x00 }, ct); // DISCONNECT, normal
    }
}
