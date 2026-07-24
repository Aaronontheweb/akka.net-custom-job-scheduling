using Aaron.Akka.Aspire.Hosting;

var builder = DistributedApplication.CreateBuilder(args);

var redis = builder.AddRedis("akka-discovery");

var akkaCluster = builder.AddAkka("custom-job-scheduling")
    .WithClustering(redis);

builder.AddProject<Projects.Akka_CustomJobScheduling>("worker")
    .WithHttpEndpoint(name: "http")
    .WithReplicas(3)
    .WithReference(akkaCluster);

builder.Build().Run();
