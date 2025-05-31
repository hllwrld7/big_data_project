using BigDataProject.SqlQueries;
using Microsoft.Extensions.Options;
using MySql.Data.MySqlClient;
using System.Data;
using System.Data.Common;
using System.Reflection.PortableExecutable;
using System.Text;

namespace BigDataProject.DataHandling
{
    public class DatabaseControl
    {
        private readonly string _connectionString;
        private readonly string _schemaName;

        public DatabaseControl(IOptions<Config> config)
        {
            _connectionString = config.Value.DatabaseConnectionString;
            _schemaName = config.Value.SchemaName;
        }

        public List<string> GetAllTableNamesWithSpecificColumn(string column)
        {
            var tables = new List<string>();
            using (var dbConnection = new MySqlConnection(_connectionString))
            {
                dbConnection.Open();
                var query = SqlQueryHelper.GetAllTableNamesWithSpecificColumnQuery(column, _schemaName);
                LogQuery(query);
                MySqlCommand cmd = new MySqlCommand(query, dbConnection);
                var dataReader = cmd.ExecuteReader();
                while (dataReader.Read())
                {
                    var value = dataReader["table_name"];
                    if (value == null)
                        continue;
                    tables.Add(value.ToString());
                }
                dbConnection.Close();
            }
            return tables;
        }

        private void LogQuery(string query)
        {
            Console.WriteLine($"Query executed: {query}");
        }

        public DataTable MergeTablesWithCommonColumn(List<string> tablesToJoin, string commonColumn, string resultTableName)
        {
            DataTable dataTable = new DataTable();
            
            using (var dbConnection = new MySqlConnection(_connectionString))
            {
                dbConnection.Open();

                dataTable = LoadDataTableFromSql(tablesToJoin[0], dbConnection);
                List<string> actualJoinedTables = new List<string>() { dataTable.TableName };

                for (int i = 1; i < tablesToJoin.Count; i++)
                {
                    DataTable tempTable = new DataTable();
                    tempTable = LoadDataTableFromSql(tablesToJoin[i], dbConnection);
                    try
                    {
                        dataTable.Merge(tempTable);
                        actualJoinedTables.Add(tempTable.TableName);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
                dbConnection.Close();
            }
            dataTable.TableName = resultTableName;
            return dataTable;
        }

        private DataTable LoadDataTableFromSql(string tableName, MySqlConnection dbConnection)
        {
            var dataTable = new DataTable();
            var query = SqlQueryHelper.SelectAllFromTableQuery(tableName, false);
            LogQuery(query);
            var cmd = new MySqlCommand(query, dbConnection);
            try
            {
                dataTable.Load(cmd.ExecuteReader());
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            return dataTable;
        }

        public async Task CreateTableFromDataTable(DataTable dataTable)
        {
            using (var dbConnection = new MySqlConnection(_connectionString))
            {
                try
                {
                    dbConnection.Open();
                    var query = SqlQueryHelper.CreateTableQuery(dataTable);
                    var command = new MySqlCommand(query, dbConnection);
                    LogQuery(command.CommandText);
                    await command.ExecuteNonQueryAsync();
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                }
                finally
                {
                    dbConnection.Close();
                }
            }
        }

        public async Task InsertDataIntoSQLServerFast(DataTable dataTable)
        {
            using (var dbConnection = new MySqlConnection(_connectionString))
            {
                try
                {
                    dbConnection.Open();

                    using (MySqlTransaction tran = dbConnection.BeginTransaction(IsolationLevel.Serializable))
                    {
                        using (MySqlCommand cmd = new MySqlCommand())
                        {
                            cmd.Connection = dbConnection;
                            cmd.Transaction = tran;
                            cmd.CommandText = SqlQueryHelper.SelectAllFromTableQuery(dataTable.TableName, true);

                            using (MySqlDataAdapter adapter = new MySqlDataAdapter(cmd))
                            {
                                adapter.UpdateBatchSize = 10000;
                                using (MySqlCommandBuilder cb = new MySqlCommandBuilder(adapter))
                                {
                                    cb.SetAllValues = true;
                                    await adapter.UpdateAsync(dataTable);
                                    tran.Commit();
                                }
                            };
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                }
                finally
                {
                    dbConnection.Close();
                }
            }
        }

        // the method above does not seem to be able to handle really big data tables, so I have to use this one
        public async Task InsertDataIntoSQLServerByRow(DataTable dataTable)
        {
            using (var dbConnection = new MySqlConnection(_connectionString))
            {
                try
                {
                    var rowIndex = 1;
                    var maxRowsCount = 100000;
                    dbConnection.Open();

                    for (int i = 0; i < dataTable.Rows.Count; i++)
                    {
                        try
                        { 
                            var cmd = new MySqlCommand(SqlQueryHelper.InsertRowQuery(ref dataTable, dataTable.TableName, i), dbConnection);
                            LogQuery(cmd.CommandText);
                            await cmd.ExecuteNonQueryAsync();
                            rowIndex += maxRowsCount;
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex.ToString());
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                }
                finally
                {
                    dbConnection.Close();
                }
            }

        }
    }
}
