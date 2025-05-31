using KaggleAPI.Web;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using System.Data;
using static KaggleAPI.Web.KaggleEnum;

namespace BigDataProject.DataHandling
{
    public class DownloadManager
    {
        private readonly DatabaseControl _databaseControl;
        private readonly DataTransformationManager _dataTransformationManager;
        private readonly string _kaggleAPICredentialsPath;
        private readonly string _csvFilesDirectory;

        public DownloadManager(DatabaseControl databaseControl, IOptions<Config> config, DataTransformationManager dataTransformationManager)
        {
            _databaseControl = databaseControl;
            _kaggleAPICredentialsPath = config.Value.KaggleAPICredentialsPath;
            _csvFilesDirectory = config.Value.CsvFilesDirectory;
            _dataTransformationManager = dataTransformationManager;
        }

        public async Task DownloadDatasets()
        {
            var kaggleConfig = LoadJson<KaggleConfiguration>(_kaggleAPICredentialsPath);
            var datasetRefs = await GetDatasetRefs(kaggleConfig);

            using (var kaggle = new KaggleClient())
            {
                kaggle.Authenticate(
                    kaggleConfig,
                    method: AuthenticationMethod.Direct
                );

                if (!Directory.Exists(_csvFilesDirectory))
                    Directory.CreateDirectory(_csvFilesDirectory);

                var downloadedDatasets =  Directory.GetFiles(_csvFilesDirectory);

                foreach (var dataset in datasetRefs)
                {
                    try
                    {
                        await kaggle.DatasetDownload(dataset.ToString(), path: _csvFilesDirectory, unzip: true);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.ToString());
                    }
                    Console.WriteLine($"Downloaded files for {dataset.ToString()} dataset");
                }
            }
        }

        // I have to get datasets in this way because the library method for it stopped working halfway through the project (deserialization error).
        internal async Task<IEnumerable<dynamic>> GetDatasetRefs(KaggleConfiguration kaggleConfig)
        {
            string baseUrl = "https://www.kaggle.com/api/v1";
            HttpClient client = new HttpClient();
            string accept = "application/json";
            UriBuilder url = new UriBuilder(baseUrl);
            url.Path += string.Format("/datasets/list");
            url.Query = new FormUrlEncodedContent(new Dictionary<string, string> { { "search", "video games" } }).ReadAsStringAsync().Result;

            HttpRequestMessage request = new HttpRequestMessage(new HttpMethod("GET"), url.Uri);

            request.Headers.TryAddWithoutValidation("Authorization", $"Basic {kaggleConfig.key}");
            request.Headers.TryAddWithoutValidation("User-Agent", "Swagger-Codegen/1/python");
            if (accept != string.Empty)
                request.Headers.TryAddWithoutValidation("Accept", "application/json");

            var response = await client.SendAsync(request);

            var json = await response.Content.ReadAsStringAsync();
            var datasetInformation = JsonConvert.DeserializeObject<IEnumerable<dynamic>>(json);

            if (datasetInformation == null)
                return new List<dynamic>();

            return datasetInformation.Select(x => x.@ref);
        }

        public async Task LoadDatasetsToDatabase()
        {
            foreach (var file in Directory.GetFiles(_csvFilesDirectory).Where(x => x.Split('.')[1] == "csv"))
            {
                var dataTable = _dataTransformationManager.GetDataTableFromCSVFile(file);
                Console.WriteLine($"Creating datatable from csv for {file}");

                if (dataTable == null || String.IsNullOrEmpty(dataTable.TableName))
                {
                    Console.WriteLine($"File {file} skipped before writing to database");
                    continue;
                }
                try
                {
                    await _databaseControl.CreateTableFromDataTable(dataTable);
                    await _databaseControl.InsertDataIntoSQLServerFast(dataTable);
                    Console.WriteLine($"Inserted data to the {dataTable.TableName} table");
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                    Console.WriteLine($"Data table {dataTable.TableName} skipped while writing to database");
                }
            }
        }

        private T LoadJson<T>(string path)
        {
            T items;
            using (StreamReader r = new StreamReader(path))
            {
                string json = r.ReadToEnd();
                items = JsonConvert.DeserializeObject<T>(json);
            }
            return items;
        }
    }
}
