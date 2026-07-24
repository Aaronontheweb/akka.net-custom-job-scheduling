using System.Net;
using System.Net.Http.Json;
using System.Text.Json;

namespace Akka.CustomJobScheduling.Api.Tests;

/// <summary>
/// The HTTP surface, end to end, against a self-contained in-process scheduler.
/// </summary>
public class JobApiTests : IClassFixture<JobApiFactory>
{
    private readonly JobApiFactory _factory;

    public JobApiTests(JobApiFactory factory)
    {
        _factory = factory;
    }

    private HttpClient Client() => _factory.CreateClient();

    private static async Task<JsonElement> JsonOf(HttpResponseMessage response) =>
        JsonDocument.Parse(await response.Content.ReadAsStringAsync()).RootElement;

    [Fact]
    public async Task Root_responds()
    {
        // Sounds trivial, and is the single most valuable test here: a minimal-API route that
        // fails to build takes down every endpoint in the app, including this one.
        var response = await Client().GetAsync("/");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task Queue_reports_the_local_worker_nodes()
    {
        var queue = await JsonOf(await Client().GetAsync("/cluster/queue"));

        Assert.Equal(2, queue.GetProperty("nodes").GetArrayLength());
        Assert.Equal(200u, queue.GetProperty("totalCapacity").GetUInt32());
    }

    [Fact]
    public async Task Submitting_a_job_returns_202_with_a_location()
    {
        var client = Client();

        var response = await client.PostAsJsonAsync(
            "/jobs",
            new { id = "http-job-1", size = 20, submitterId = "tester" });

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        Assert.Equal("/jobs/http-job-1", response.Headers.Location?.ToString());

        var status = await JsonOf(await client.GetAsync("/jobs/http-job-1"));
        Assert.Equal("http-job-1", status.GetProperty("id").GetString());
        Assert.Equal("tester", status.GetProperty("submitterId").GetString());
    }

    [Fact]
    public async Task A_job_without_an_id_gets_one()
    {
        var response = await Client().PostAsJsonAsync("/jobs", new { size = 5 });

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        Assert.False(string.IsNullOrWhiteSpace((await JsonOf(response)).GetProperty("id").GetString()));
    }

    [Theory]
    [InlineData(0, HttpStatusCode.BadRequest)]              // size must be positive
    [InlineData(999999, HttpStatusCode.UnprocessableEntity)] // larger than any node
    public async Task Invalid_submissions_are_refused(uint size, HttpStatusCode expected)
    {
        var response = await Client().PostAsJsonAsync("/jobs", new { size, submitterId = "tester" });

        Assert.Equal(expected, response.StatusCode);
    }

    [Fact]
    public async Task Duplicate_submission_conflicts()
    {
        var client = Client();
        var body = new { id = "dupe-job", size = 5, submitterId = "tester" };

        await client.PostAsJsonAsync("/jobs", body);
        var second = await client.PostAsJsonAsync("/jobs", body);

        Assert.Equal(HttpStatusCode.Conflict, second.StatusCode);
    }

    [Fact]
    public async Task Unknown_job_is_404()
    {
        Assert.Equal(HttpStatusCode.NotFound, (await Client().GetAsync("/jobs/nope")).StatusCode);
    }

    [Fact]
    public async Task Cancelling_requires_a_submitter_and_rejects_the_wrong_one()
    {
        var client = Client();
        await client.PostAsJsonAsync("/jobs", new { id = "cancel-job", size = 60, submitterId = "owner" });

        Assert.Equal(
            HttpStatusCode.BadRequest,
            (await client.DeleteAsync("/jobs/cancel-job")).StatusCode);

        Assert.Equal(
            HttpStatusCode.Forbidden,
            (await client.DeleteAsync("/jobs/cancel-job?submitterId=someone-else")).StatusCode);

        Assert.Equal(
            HttpStatusCode.Accepted,
            (await client.DeleteAsync("/jobs/cancel-job?submitterId=owner&reason=done")).StatusCode);
    }

    [Fact]
    public async Task Streaming_a_job_emits_status_frames_and_closes_when_it_finishes()
    {
        var client = Client();
        await client.PostAsJsonAsync("/jobs", new { id = "stream-job", size = 4, submitterId = "tester" });

        using var request = new HttpRequestMessage(HttpMethod.Get, "/jobs/stream-job/events");
        using var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("text/event-stream", response.Content.Headers.ContentType?.MediaType);

        var statuses = new List<string>();

        using var reader = new StreamReader(await response.Content.ReadAsStreamAsync());

        // Bounded on purpose. The stream must terminate on its own once the job reaches a terminal
        // state; without a deadline a regression here hangs the whole test run instead of failing,
        // which is exactly what happened when nothing was announcing the local worker nodes.
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        while (await reader.ReadLineAsync(deadline.Token) is { } line)
        {
            if (!line.StartsWith("data: "))
                continue;

            var frame = JsonDocument.Parse(line[6..]).RootElement;
            statuses.Add(frame.GetProperty("status").GetString()!);
        }

        Assert.NotEmpty(statuses);
        Assert.Equal("Completed", statuses[^1]);
    }
}
