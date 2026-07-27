using Aaron.Akka.Aspire.Hosting;

var builder = DistributedApplication.CreateBuilder(args);

var redis = builder.AddRedis("akka-discovery");

var akkaCluster = builder.AddAkka("custom-job-scheduling")
    .WithClustering(redis);

builder.AddProject<Projects.Akka_CustomJobScheduling>("worker")
    .WithHttpEndpoint(name: "http")
    .WithReplicas(3)
    .WithReference(akkaCluster);

// The API joins the same cluster under a different role. It runs no jobs — it reaches the tracker
// singleton and the submitter shard region through proxies, and terminates SSE connections.
builder.AddProject<Projects.Akka_CustomJobScheduling_Api>("api")
    .WithHttpEndpoint(name: "http")
    .WithReference(akkaCluster);

builder.Build().Run();
