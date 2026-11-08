namespace MicroDCS.App.Tests;

public sealed class SmokeTests
{
    [Fact]
    public async Task Valkey_registration_smoke_check_passes_without_a_server()
    {
        var output = new StringWriter();

        var result = await ValkeySmoke.RunAsync(output);

        result.ShouldBe(0);
        output.ToString().ShouldContain("ok   valkey-registration");
    }

    [Fact]
    public async Task Host_smoke_check_passes()
    {
        var output = new StringWriter();

        var result = await HostSmoke.RunAsync(output);

        result.ShouldBe(0);
    }

    [Fact]
    public async Task Inspection_smoke_check_passes()
    {
        var output = new StringWriter();

        var result = await InspectionSmoke.RunAsync(output);

        result.ShouldBe(0);
        output.ToString().ShouldContain("ok   inspection-mcp");
    }
}
