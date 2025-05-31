using MySql.Data.MySqlClient;
using System.Data;
using System.Diagnostics;
using System.Text;

namespace BigDataProject.SqlQueries
{
    public static class SqlQueryHelper
    {
        public static string GetColumnsForTableCommand(string tableName, string tableSchema)
        {
            return $"SELECT COLUMN_NAME FROM information_schema.columns WHERE TABLE_NAME='{tableName}' AND TABLE_SCHEMA='{tableSchema}';";
        }

        public static string GetAllTableNamesWithSpecificColumnQuery(string column, string tableSchema)
        {
            {
                return $"SELECT DISTINCT table_name FROM information_schema.columns WHERE COLUMN_NAME IN ('{column}') AND TABLE_SCHEMA='{tableSchema}';";
            }
        }

        public static string SelectAllFromTableQuery(string tableName, bool addLimit)
        {
            var limit0 = addLimit ? "LIMIT 0" : "";
            return $"SELECT * FROM {tableName} {limit0};";
        }

        public static string ModifyColumnQuery(string tableName, string columnName, string newType)
        {
            return $"ALTER TABLE {tableName} MODIFY COLUMN {columnName} {newType};";
        }

        public static string CreateTableQuery(DataTable table)
        {
            StringBuilder sql = new StringBuilder();
            StringBuilder alterSql = new StringBuilder();

            sql.AppendFormat("CREATE TABLE IF NOT EXISTS {0} (", table.TableName);

            for (int i = 0; i < table.Columns.Count; i++)
            {
                bool isNumeric = false;
                bool usesColumnDefault = true;

                sql.AppendFormat("\n\t{0}", table.Columns[i].ColumnName);

                switch (table.Columns[i].DataType.ToString().ToUpper())
                {
                    case "SYSTEM.INT32":
                        sql.Append(" int(11)");
                        isNumeric = true;
                        break;
                    case "SYSTEM.INT64":
                        sql.Append(" bigint(11)");
                        isNumeric = true;
                        break;
                    case "SYSTEM.DATETIME":
                        sql.Append(" datetime(6)");
                        usesColumnDefault = false;
                        break;
                    case "SYSTEM.STRING":
                        sql.AppendFormat(" varchar(255)", table.Columns[i].MaxLength);
                        break;
                    case "SYSTEM.DOUBLE":
                        sql.Append(" double(40,2)");
                        isNumeric = true;
                        break;
                    case "SYSTEM.BOOL":
                        sql.Append(" tinyint(1)");
                        isNumeric = true;
                        break;
                    default:
                        sql.AppendFormat(" varchar(255)", table.Columns[i].MaxLength);
                        break;
                }

                if (!table.Columns[i].AllowDBNull)
                {
                    sql.Append(" NOT NULL");
                }

                sql.Append(",");
            }

            if (table.PrimaryKey.Length > 0)
            {
                StringBuilder primaryKeySql = new StringBuilder();

                primaryKeySql.AppendFormat("\n\tCONSTRAINT PK_{0} PRIMARY KEY (", table.TableName);

                for (int i = 0; i < table.PrimaryKey.Length; i++)
                {
                    primaryKeySql.AppendFormat("{0},", table.PrimaryKey[i].ColumnName);
                }

                primaryKeySql.Remove(primaryKeySql.Length - 1, 1);
                primaryKeySql.Append(")");

                sql.Append(primaryKeySql);
            }
            else
            {
                sql.Remove(sql.Length - 1, 1);
            }

            sql.AppendFormat("\n);\n{0}", alterSql.ToString());

            return sql.ToString();
        }

        public static string InsertRowQuery(ref DataTable table, String table_name, int row)
        {
            try
            {
                StringBuilder queryBuilder = new StringBuilder();
                DateTime dt;

                queryBuilder.AppendFormat("INSERT INTO `{0}` (", table_name);

                // more than 1 column required and 1 or more rows
                if (table.Columns.Count > 1 && table.Rows.Count > 0)
                {
                    // build all columns
                    queryBuilder.AppendFormat("`{0}`", table.Columns[0].ColumnName);

                    if (table.Columns.Count > 1)
                    {
                        for (int i = 1; i < table.Columns.Count; i++)
                        {
                            queryBuilder.AppendFormat(", `{0}` ", table.Columns[i].ColumnName);
                        }
                    }

                    queryBuilder.AppendFormat(") VALUES (", table_name);

                    if (table.Rows.Count > 1)
                    {

                        // escape String & Datetime values!
                        if (table.Columns[0].DataType == typeof(String))
                        {
                            queryBuilder.AppendFormat("'{0}'", MySqlHelper.EscapeString(table.Rows[row][table.Columns[0].ColumnName].ToString()));
                        }
                        else if (table.Columns[0].DataType == typeof(DateTime))
                        {
                            dt = (DateTime)table.Rows[row][table.Columns[0].ColumnName];
                            queryBuilder.AppendFormat("'{0}'", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                        }
                        else if (table.Columns[0].DataType == typeof(Int32))
                        {
                            queryBuilder.AppendFormat("{0}", table.Rows[row].Field<Int32?>(table.Columns[0].ColumnName) ?? 0);
                        }
                        else if (table.Columns[0].DataType == typeof(Double))
                        {
                            var value = table.Rows[row].Field<string?>(table.Columns[0].ColumnName) ?? "";
                            queryBuilder.AppendFormat(", {0}", value.Replace(',', '.'));
                        }
                        else
                        {
                            queryBuilder.AppendFormat(", {0}", table.Rows[row][table.Columns[0].ColumnName].ToString());
                        }

                        for (int col = 1; col < table.Columns.Count; col++)
                        {
                            // escape String & Datetime values!
                            if (table.Columns[col].DataType == typeof(String))
                            {
                                queryBuilder.AppendFormat(", '{0}'", MySqlHelper.EscapeString(table.Rows[row][table.Columns[col].ColumnName].ToString()));
                            }
                            else if (table.Columns[col].DataType == typeof(DateTime))
                            {
                                dt = (DateTime)table.Rows[row][table.Columns[col].ColumnName];
                                queryBuilder.AppendFormat(", '{0}'", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                            }
                            else if (table.Columns[col].DataType == typeof(Int32))
                            {
                                queryBuilder.AppendFormat(", {0}", table.Rows[row].Field<Int32?>(table.Columns[col].ColumnName) ?? 0);
                            }
                            else if (table.Columns[col].DataType == typeof(Double))
                            {
                                var value = table.Rows[row].Field<Double?>(table.Columns[col].ColumnName) ?? 0;
                                queryBuilder.AppendFormat(", {0}", value.ToString().Replace(',', '.'));
                            }
                            else
                            {
                                queryBuilder.AppendFormat(", {0}", table.Rows[row][table.Columns[col].ColumnName].ToString());
                            }
                        } // end for (int i = 1; i < table.Columns.Count; i++)

                        // close value block
                        queryBuilder.Append(")");
                        queryBuilder.AppendLine();

                        // sql delimiter =)
                        queryBuilder.Append(";");

                    } // end if (table.Rows.Count > 1)

                    return queryBuilder.ToString();
                }
                else
                {
                    return "";
                } // end if(table.Columns.Count > 1 && table.Rows.Count > 0)
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            return "";
        }
    }
}
