namespace BigDataProject
{
    public class Config
    {
        public string DatabaseConnectionString { get; set; }
        public string CsvFilesDirectory { get; set; }
        public string KaggleAPICredentialsPath { get; set; }
        public string SchemaName {  get; set; }
        public int MaxStringLengthSQL { get; set; }
    }
}
