using System.Runtime.CompilerServices;

namespace Akka.CustomJobScheduling.Actors.Tests;

internal static class TestEnvironmentInitializer
{
    /// <summary>
    /// Akka.Hosting.TestKit spins up a real <c>IHost</c> per test class, and each one enables a
    /// configuration file watcher by default. Enough of them at once exhausts the inotify watch
    /// limit on Linux CI and the tests fail with something that looks nothing like the real cause.
    /// </summary>
    [ModuleInitializer]
    internal static void Initialize() =>
        Environment.SetEnvironmentVariable("DOTNET_HOSTBUILDER__RELOADCONFIGONCHANGE", "false");
}
