using Microsoft.Extensions.Options;

namespace MicroDCS.Maintenance.Tests;

public sealed class OptionsTests
{
    private static ValidateOptionsResult Validate(string environment, MaintenanceOptions options) =>
        new MaintenanceOptionsValidator(new FakeEnvironment(environment)).Validate(null, options);

    [Theory]
    [InlineData("Production")]
    [InlineData("Staging")]
    [InlineData("")]
    public void EnablingOutsideDevelopmentAndTestFailsTheStart(string environment)
    {
        var result = Validate(environment, new MaintenanceOptions { Enabled = true });

        result.Failed.ShouldBeTrue();
        result.FailureMessage.ShouldContain("Development and Test");
    }

    [Theory]
    [InlineData("Development")]
    [InlineData("Test")]
    [InlineData("test")]
    public void EnablingInDevelopmentAndTestIsAllowed(string environment) =>
        Validate(environment, new MaintenanceOptions { Enabled = true }).Succeeded.ShouldBeTrue();

    [Fact]
    public void DisabledIsAllowedEverywhere() =>
        Validate("Production", new MaintenanceOptions()).Succeeded.ShouldBeTrue();

    [Fact]
    public void DefaultsMatchTheDecision()
    {
        var options = new MaintenanceOptions();

        options.ItemCap.ShouldBe(100);
        options.SnapshotRetention.ShouldBe(20);
        options.SnapshotMaxBytes.ShouldBe(5 * 1024 * 1024);
        options.Enabled.ShouldBeFalse();
        options.RequireEnvironmentMarker.ShouldBeFalse();
    }

    [Theory]
    [InlineData(0, 20, 1024)]
    [InlineData(100, 0, 1024)]
    [InlineData(100, 20, 10)]
    public void LimitsMustBePositive(int cap, int retention, long bytes) =>
        Validate("Development", new MaintenanceOptions { ItemCap = cap, SnapshotRetention = retention, SnapshotMaxBytes = bytes }).Failed.ShouldBeTrue();
}
