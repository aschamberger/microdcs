using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace MicroDCS.Inspection.Tests;

/// <summary>The management listener holds ASP.NET Core logging at Warning unless the configuration says otherwise (S22).</summary>
public class ManagementLoggerFactoryTests
{
    /// <summary>A host factory whose loggers have every level on, so the listener's own floor is what the tests see.</summary>
    private static ILoggerFactory Inner()
    {
        var logger = Substitute.For<ILogger>();
        logger.IsEnabled(Arg.Any<LogLevel>()).Returns(true);
        var factory = Substitute.For<ILoggerFactory>();
        factory.CreateLogger(Arg.Any<string>()).Returns(logger);
        return factory;
    }

    [Fact]
    public void AspNetCoreCategoriesStayAtWarningByDefault()
    {
        using var inner = Inner();
        var factory = new ManagementLoggerFactory(inner, configuration: null);

        factory.CreateLogger("Microsoft.AspNetCore.Hosting.Diagnostics").IsEnabled(LogLevel.Information).ShouldBeFalse();
        factory.CreateLogger("Microsoft.AspNetCore.Hosting.Diagnostics").IsEnabled(LogLevel.Warning).ShouldBeTrue();
        factory.CreateLogger("MicroDCS.Security").IsEnabled(LogLevel.Information).ShouldBeTrue();
    }

    [Fact]
    public void TheConfiguredLevelOfAspNetCoreWins()
    {
        using var inner = Inner();
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?> { ["Logging:LogLevel:Microsoft.AspNetCore"] = "Information" })
            .Build();
        var factory = new ManagementLoggerFactory(inner, configuration);

        factory.CreateLogger("Microsoft.AspNetCore.Routing.EndpointMiddleware").IsEnabled(LogLevel.Information).ShouldBeTrue();
        factory.CreateLogger("Microsoft.AspNetCore.Routing.EndpointMiddleware").IsEnabled(LogLevel.Debug).ShouldBeFalse();
    }
}
