using System.Net;
using System.Net.Sockets;

namespace MicroDCS.IntegrationTests.Sfc;

/// <summary>Kills and starts compose services through the Docker Engine API, so a replica test sees a real process death (S20, D-53).</summary>
internal sealed class DockerEngine : IDisposable
{
    private readonly HttpClient _client;

    public DockerEngine(string socketPath)
    {
        var handler = new SocketsHttpHandler
        {
            ConnectCallback = async (_, ct) =>
            {
                var socket = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
                await socket.ConnectAsync(new UnixDomainSocketEndPoint(socketPath), ct);
                return new NetworkStream(socket, ownsSocket: true);
            },
        };
        _client = new HttpClient(handler) { BaseAddress = new Uri("http://docker") };
    }

    /// <summary>Sends SIGKILL. A container that does not run is left as it is.</summary>
    public Task KillAsync(string container, CancellationToken ct) => PostAsync($"/containers/{container}/kill", [HttpStatusCode.NoContent, HttpStatusCode.Conflict], ct);

    /// <summary>Starts the container. One that already runs is left as it is.</summary>
    public Task StartAsync(string container, CancellationToken ct) => PostAsync($"/containers/{container}/start", [HttpStatusCode.NoContent, HttpStatusCode.NotModified], ct);

    public void Dispose() => _client.Dispose();

    private async Task PostAsync(string path, HttpStatusCode[] accepted, CancellationToken ct)
    {
        using var response = await _client.PostAsync(path, null, ct);
        accepted.ShouldContain(response.StatusCode, $"POST {path}");
    }
}
