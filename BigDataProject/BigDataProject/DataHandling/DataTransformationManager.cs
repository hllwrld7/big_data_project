using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.Options;
using Microsoft.VisualBasic.FileIO;
using Mysqlx.Crud;
using System.Data;
using System.Diagnostics;
using System.Text.RegularExpressions;

namespace BigDataProject.DataHandling
{
    public class DataTransformationManager
    {
        private List<string> _usedTableNames = new List<string>();
        private readonly DatabaseControl _databaseControl;
        private readonly int _maxStringLength = 255;

        public DataTransformationManager(DatabaseControl databaseControl, IOptions<Config> config)
        {
            _databaseControl = databaseControl;
            _maxStringLength = config.Value.MaxStringLengthSQL;
        }

        public DataTable GetDataTableFromCSVFile(string csvFilePath)
        {
            DataTable csvData = new DataTable();

            try
            {
                using (TextFieldParser csvReader = new TextFieldParser(csvFilePath))
                {
                    Console.WriteLine(csvFilePath);
                    csvReader.SetDelimiters(new string[] { "," });
                    csvReader.HasFieldsEnclosedInQuotes = true;
                    string[] colFields = csvReader.ReadFields();
                    var fieldData = new List<string[]>();
                    while (!csvReader.EndOfData)
                    {
                        try
                        {
                            string[] fields = csvReader.ReadFields();
                            fieldData.Add(fields);
                        }
                        catch
                        {
                            Console.WriteLine("field skipped :(");
                        }
                    }

                    var dateTimeColumns = new List<int>();
                    var doubleColumns = new List<int>();
                    var stringColumns = new List<int>();

                    for (int i = 0; i < colFields.Length; i++)
                    {
                        DataColumn dataColumn = new DataColumn(colFields[i]);
                        dataColumn.DataType = GetColumnType(fieldData, i);

                        if (dataColumn.ColumnName.IndexOf("sale", StringComparison.OrdinalIgnoreCase) >= 0)
                            dataColumn.DataType = typeof(double);

                        if (dataColumn.DataType == typeof(DateTime))
                            dateTimeColumns.Add(i);

                        if (dataColumn.DataType == typeof(double))
                            doubleColumns.Add(i);

                        if (dataColumn.DataType == typeof(string))
                        {
                            dataColumn.MaxLength = _maxStringLength;
                            stringColumns.Add(i);
                        }

                        dataColumn.AllowDBNull = true;
                        dataColumn.ColumnName = dataColumn.ColumnName.Replace(' ', '_').Replace('(', '_').Replace(')', '_').Replace('-', '_');

                        if (string.Equals(dataColumn.ColumnName, "index", StringComparison.OrdinalIgnoreCase))
                            dataColumn.ColumnName = "id";

                        if (!IsColumnNameValid(dataColumn.ColumnName))
                            return csvData;

                        // capitalise the 1st letter
                        if (!string.IsNullOrEmpty(dataColumn.ColumnName))
                            dataColumn.ColumnName = dataColumn.ColumnName.ToLower();

                        csvData.Columns.Add(dataColumn);
                    }

                    var fieldDataList = fieldData.ToList();
                    for (int i = 0; i < fieldDataList.Count; i++)
                    {
                        //Making empty value as null
                        for (int j = 0; j < fieldDataList[i].Length; j++)
                        {
                            if (fieldDataList[i][j] == "")
                            {
                                fieldData[i][j] = null;
                            }

                            foreach (var dateTimeColumn in dateTimeColumns)
                            {
                                if (fieldData[i][dateTimeColumn] == null)
                                    continue;

                                var value = fieldData[i][dateTimeColumn].ToString();
                                if (DateTime.TryParse(value, out var dateTimeValue))
                                    fieldData[i][dateTimeColumn] = dateTimeValue.ToString("yyyy-MM-dd HH:mm:ss");
                            }

                            foreach (var doubleColumn in doubleColumns)
                            {
                                if (fieldData[i][doubleColumn] == null)
                                    continue;
                                var value = fieldData[i][doubleColumn].ToString();
                                fieldData[i][doubleColumn] = value.Replace('.', ',');
                            }

                            foreach (var stringColumn in stringColumns)
                            {
                                var value = fieldData[i][stringColumn];
                                if (value != null && value.Length >= _maxStringLength)
                                    fieldData[i][stringColumn] = value.Substring(0, _maxStringLength-1);
                            }
                        }
                        csvData.Rows.Add(fieldData[i]);
                    }
                    Console.WriteLine(Path.GetFileName(csvFilePath));
                    csvData.TableName = GetTableName(csvFilePath);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
            return csvData;
        }

        private bool IsColumnNameValid(string columnName)
        {
            // if the column name only consists of numbers then the csv file probably doesn't have a proper header
            var numericRegex = new Regex("^[0-9]*$");
            return !numericRegex.IsMatch(columnName) || String.IsNullOrEmpty(columnName);
        }

        private string GetTableName(string csvFilePath)
        {
            var tableName = Path.GetFileName(csvFilePath).Replace(' ', '_').Replace('(', '_').Replace(')', '_').Replace('-', '_').Split('.')[0].ToString();
            var i = 0;
            while (_usedTableNames.Any(x => x == tableName))
            {
                i++;
                tableName += $"_{i}";
            }
            _usedTableNames.Add(tableName);
            return tableName;
        }

        private static System.Type GetColumnType(List<string[]> rows, int columnIndex)
        {
            var isColumnInt = true;
            var isColumnDouble = false;
            var isColumnDate = true;
            var isColumnBool = false;

            foreach (var row in rows)
            {
                if (!isColumnInt && !isColumnDouble && !isColumnDate)
                    return typeof(string);

                var value = row[columnIndex];

                if (value.Any(x => x == ',' || x == '.') || isColumnDouble)
                {
                    var potentialDouble = value.Replace('.', ',');
                    isColumnDouble = double.TryParse(potentialDouble, out var b);
                }

                isColumnBool = bool.TryParse(value, out var h);
                    
                isColumnInt = int.TryParse(value, out var c);

                if (isColumnDate && !isColumnDouble)
                {
                    if (!DateTime.TryParse(value, out var a))
                        isColumnDate = false;
                }
            }
            if (isColumnBool)
                return typeof(bool);

            if (isColumnDouble)
                return typeof(double);

            if (isColumnDate)
                return typeof(DateTime);

            return isColumnInt ? typeof(int) : typeof(string);
        }

        public async Task JoinAllTablesByColumn(string column)
        {
            var tablesNames = _databaseControl.GetAllTableNamesWithSpecificColumn(column);
            var dataTable = _databaseControl.MergeTablesWithCommonColumn(tablesNames, column, $"video_games_joinedby_{column}");
            await _databaseControl.CreateTableFromDataTable(dataTable);
            await _databaseControl.InsertDataIntoSQLServerByRow(dataTable);
        }
    }
}
