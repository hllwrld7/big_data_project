namespace BigDataProject
{
    public class Config
    {
        public string DatabaseConnectionString { get; set; } = "Server=localhost;Database=big_data_project;Uid=root;Pwd=";
        public string CsvFilesDirectory { get; set; } = "data";
        public string KaggleAPICredentialsPath { get; set; } = "kaggle.json";
        public string SchemaName { get; set; } = "big_data_project";
        public int MaxStringLengthSQL { get; set; } = 255;
    }
}
