using BigDataProject;
using BigDataProject.DataHandling;
using BigDataProject.Services;

var builder = new HostBuilder();

var config = new ConfigurationBuilder()
    .SetBasePath(Directory.GetCurrentDirectory())
    .AddJsonFile("appsettings.json", optional: true)
    .Build();

builder.ConfigureServices(services =>
{
services.Configure<Config>(opts => config.GetSection(nameof(Config)).Bind(opts));
services.AddSingleton<DatabaseControl>();
services.AddSingleton<DownloadManager>();
services.AddSingleton<DatabaseControl>();
services.AddSingleton<DataTransformationManager>();
    services.AddHostedService<DatasetsCollectingService>();
});

var app = builder.Build();

app.Run();
