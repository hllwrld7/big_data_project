
using BigDataProject.DataHandling;

namespace BigDataProject.Services
{
    public class DatasetsCollectingService : IHostedService
    {
        private readonly DownloadManager _downloadManager;
        private readonly DataTransformationManager _dataTransformationManager;
        private readonly IHostApplicationLifetime _lifeTime;

        public DatasetsCollectingService(DownloadManager downloadManager, DataTransformationManager dataTransformationManager, IHostApplicationLifetime lifeTime)
        {
            _downloadManager = downloadManager;
            _dataTransformationManager = dataTransformationManager;
            _lifeTime = lifeTime;
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            Console.WriteLine("Downloading datasets...");
            await _downloadManager.DownloadDatasets();
            Console.WriteLine("Datasets downloaded!");

            Console.WriteLine("Loading datasets to database...");
            await _downloadManager.LoadDatasetsToDatabase();
            Console.WriteLine("Loaded datasets to database!");

            Console.WriteLine("Joining tables by genre...");
            await _dataTransformationManager.JoinAllTablesByColumn("genre");
            Console.WriteLine("Joined tables by genre");

            Console.WriteLine();
            Console.WriteLine("Press enter to exit");
            Console.ReadLine();
            _lifeTime.StopApplication();
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            Console.WriteLine("Service stopped");
            return Task.CompletedTask;
        }
    }
}
