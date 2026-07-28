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
    public async Task Root_serves_the_dashboard()
    {
        // Doubles as the canary: a minimal-API route that fails to build takes down every endpoint
        // in the app, so a 500 here means something far away is broken.
        var response = await Client().GetAsync("/");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("text/html", response.Content.Headers.ContentType?.MediaType);

        var html = await response.Content.ReadAsStringAsync();
        Assert.Contains("/cluster/events", html);
    }

    [Fact]
    public async Task Job_list_reports_submitted_jobs()
    {
        var client = Client();
        await client.PostAsJsonAsync("/jobs", new { id = "listed-1", size = 5, submitterId = "tester" });
        await client.PostAsJsonAsync("/jobs", new { id = "listed-2", size = 5, submitterId = "tester" });

        var list = await JsonOf(await client.GetAsync("/jobs"));

        var ids = list.GetProperty("jobs").EnumerateArray()
            .Select(j => j.GetProperty("id").GetString())
            .ToList();

        Assert.Contains("listed-1", ids);
        Assert.Contains("listed-2", ids);
        Assert.True(list.GetProperty("total").GetInt32() >= 2);
    }

    [Fact]
    public async Task Job_list_can_exclude_finished_work()
    {
        var client = Client();
        await client.PostAsJsonAsync("/jobs", new { id = "filtered", size = 5, submitterId = "tester" });
        await client.DeleteAsync("/jobs/filtered?submitterId=tester");

        var active = await JsonOf(await client.GetAsync("/jobs?includeFinished=false"));

        Assert.DoesNotContain(
            "filtered",
            active.GetProperty("jobs").EnumerateArray().Select(j => j.GetProperty("id").GetString()));
    }

    [Fact]
    public async Task Queue_stream_opens_with_a_snapshot_then_pushes_transitions()
    {
        var client = Client();

        using var request = new HttpRequestMessage(HttpMethod.Get, "/cluster/events");
        using var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead);

        Assert.Equal("text/event-stream", response.Content.Headers.ContentType?.MediaType);

        using var reader = new StreamReader(await response.Content.ReadAsStreamAsync());
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        // Submitting after the stream is open proves this is a push, not a snapshot poll: the
        // "status" frame can only arrive because the tracker sent it.
        await client.PostAsJsonAsync("/jobs", new { id = "streamed", size = 3, submitterId = "tester" });

        var seen = new List<string>();

        while (await reader.ReadLineAsync(deadline.Token) is { } line)
        {
            if (line.StartsWith("event: "))
                seen.Add(line[7..]);

            // Opening snapshot is jobs+queue; a transition for the job we just submitted proves
            // the live half works.
            if (seen.Contains("jobs") && seen.Contains("queue") && seen.Contains("status"))
                return;
        }

        Assert.Fail($"stream ended before all frame types arrived; saw: {string.Join(", ", seen)}");
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

    [Fact]
    public async Task A_zero_size_submission_is_refused()
    {
        // Size must be positive — rejected at the DTO layer before it reaches the tracker. A job
        // larger than any node is *not* refused any more: it's accepted and run on a whole node.
        var response = await Client().PostAsJsonAsync("/jobs", new { size = 0, submitterId = "tester" });

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
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
