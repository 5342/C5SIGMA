/*
C5 SIGMA -- Copyright (C) Command Five Pty Ltd 2011
<http://www.commandfive.com/>

C5 SIGMA is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

C5 SIGMA is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with C5 SIGMA. If not, see <http://www.gnu.org/licenses/>.
*/

using System;
using System.Collections.Generic;
using System.Data;
using System.Net;
using System.Text;
using System.Threading;
using MySql.Data.MySqlClient;
using Sigma.Common;
using Sigma.Engine.TShark;

namespace Sigma.Engine.Database
{

    /// <summary>
    /// Data writer that emits rows to a MySQL database.
    /// </summary>
    /// <remarks>
    /// Limitations: This class stores DateTime values as VarChar(27) and Guid values as VarChar(36). 
    /// All 8-bit strings are stored as 16-bit strings.
    /// </remarks>
    internal class MySqlDataWriter : AsyncDataWriter
    {

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="connectionString">The database connection string.</param>
        /// <param name="disableForeignKeys">True to disable generation of foreign keys, false otherwise.</param>
        public MySqlDataWriter(string connectionString, bool disableForeignKeys)
            : base(DataWriterType.MySql, connectionString, disableForeignKeys)
        {
        }

        /// <summary>
        /// Connects to the database.
        /// </summary>
        /// <param name="connectionString">Connection string.</param>
        protected override void Connect(string connectionString)
        {
            Log.WriteInfo("Connecting to database.");

            MySqlConnectionStringBuilder builder = new MySqlConnectionStringBuilder(connectionString);
            string catalogEscaped = EscapeName(builder.Database);
            builder.Database = string.Empty;

            catalogName = catalogEscaped;
            Connection = new MySqlConnection(builder.ToString());
            Connection.Open();

            Log.WriteInfo("Looking for database: {0}", catalogEscaped);
            int exists = ExecuteScalar<int>(string.Format("SELECT COUNT(`SCHEMA_NAME`) FROM `information_schema`.`SCHEMATA` WHERE `SCHEMA_NAME` = N'{0}'", catalogEscaped));

            if (exists == 0)
            {
                Log.WriteInfo("Creating new database.");
                ExecuteNonQuery(string.Format("CREATE DATABASE `{0}`", catalogEscaped));
            }
            else
            {
                Log.WriteInfo("Found existing database.");
            }

            ExecuteNonQuery(string.Format("USE `{0}`", catalogEscaped));
            ExecuteNonQuery(string.Format("SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED", catalogEscaped));
        }

        /// <summary>
        /// The target database's name.
        /// </summary>
        private string catalogName;

        /// <summary>
        /// Inserts a single row into the database by generating a SQL command and attaching appropriate parameters.
        /// </summary>
        /// <param name="tableDefinition">The table definition for the table to modify.</param>
        /// <param name="columnNames">List of column names.</param>
        /// <param name="columnValues">List of column values.</param>
        /// <returns>The identity column value of the inserted row.</returns>
        protected override long InsertRow(TableDefinition tableDefinition, List<string> columnNames, List<object> columnValues)
        {
            using (MySqlCommand command = (MySqlCommand)Connection.CreateCommand())
            {
                StringBuilder insertCommand = new StringBuilder();
                insertCommand.AppendFormat("INSERT INTO `{0}` (", tableDefinition.Name);
                for (int i = 0; i < columnNames.Count; i++)
                {
                    if (i > 0)
                        insertCommand.Append(',');
                    insertCommand.AppendFormat("`{0}`", columnNames[i]);
                }
                insertCommand.Append(") VALUES (");
                for (int i = 0; i < columnNames.Count; i++)
                {
                    string parameterName = string.Format("@{0}", i);

                    if (i > 0)
                        insertCommand.Append(',');
                    insertCommand.AppendFormat(parameterName);

                    command.Parameters.AddWithValue(parameterName, columnValues[i]);
                }
                insertCommand.Append("); SELECT @@IDENTITY");

                command.CommandText = insertCommand.ToString();
                return Convert.ToInt64(command.ExecuteScalar());
            }
        }

        /// <summary>
        /// Creates a new table using the given table definition.
        /// </summary>
        /// <remarks>
        /// This method generates a table creation SQL statement and executes it.
        /// </remarks>
        /// <param name="tableDefinition">Table definition for the table to create.</param>
        protected override void CreateTable(TableDefinition tableDefinition)
        {
            tableDefinition.Committed = true;

            //Log.WriteInfo("Creating table: {0}", tableDefinition.Name);

            StringBuilder commandText = new StringBuilder();
            commandText.AppendFormat("CREATE TABLE `{0}` (", tableDefinition.Name);
            commandText.Append("`_id` BIGINT AUTO_INCREMENT PRIMARY KEY");

            foreach (KeyValuePair<string, ColumnDefinition> column in tableDefinition.Columns)
            {
                commandText.AppendFormat(", `{0}` {1} NULL", column.Key, ConvertToString(column.Value.SqlType, column.Value.SqlPrecision));
                column.Value.Committed = true;
            }
            commandText.Append(") AUTO_INCREMENT=1");

            ExecuteNonQuery(commandText.ToString());
        }

        /// <summary>
        /// Loads a table definition from the database.
        /// </summary>
        /// <param name="tableName">Name of the table for which a definition is to be loaded.</param>
        /// <returns>The table definition for the named table.</returns>
        protected override TableDefinition LoadTableDefinition(string tableName)
        {
            TableDefinition tableDefinition = new TableDefinition(tableName);

            //Log.WriteInfo("Looking for table: {0}", tableName);

            int exists = ExecuteScalar<int>(string.Format("SELECT COUNT(*) FROM `information_schema`.`TABLES` WHERE `TABLE_NAME` = N'{0}' AND `TABLE_SCHEMA` =  N'{1}'", tableName, catalogName));
            if (exists == 1)
            {
                tableDefinition.Committed = true;

                //Log.WriteInfo("Loading schema from existing table.");

                string commandText = string.Format(
@"SELECT 
    `COLUMN_NAME`, `DATA_TYPE`, `CHARACTER_MAXIMUM_LENGTH`, `NUMERIC_PRECISION`
FROM
    `information_schema`.`COLUMNS`
WHERE 
	`TABLE_NAME` = N'{0}' AND `TABLE_SCHEMA` = N'{1}'", tableName, catalogName);

                using (IDataReader reader = ExecuteReader(commandText))
                {
                    bool hasIdentityColumn = false;
                    while (reader.Read())
                    {
                        string columnName = reader.GetString(0);
                        string typeName = reader.GetString(1);
                        
                        int maxLength;
                        if (!reader.IsDBNull(2))
                        {
                            maxLength = reader.GetInt32(2);
                        }
                        else
                        {
                            maxLength = 0;
                        }

                        int numPrecision;
                        if (!reader.IsDBNull(3))
                        {
                            numPrecision = reader.GetInt32(3);
                        }
                        else
                        {
                            numPrecision = 0;
                        }

                        if (!columnName.Equals("_id", StringComparison.OrdinalIgnoreCase))
                        {
                            MySqlDbType sqlDbType;
                            int sqlPrecision;

                            switch (typeName.ToLowerInvariant())
                            {
                                case "bigint":
                                    sqlDbType = MySqlDbType.Int64;
                                    sqlPrecision = 0;
                                    break;
                                case "bit":
                                    sqlDbType = MySqlDbType.Bit;
                                    sqlPrecision = 0;
                                    break;
                                case "numeric":
                                    sqlDbType = MySqlDbType.Decimal;
                                    sqlPrecision = 0;
                                    break;
                                case "float":
                                    sqlDbType = MySqlDbType.Float;
                                    sqlPrecision = 0;
                                    break;
                                case "int":
                                    sqlDbType = MySqlDbType.Int32;
                                    sqlPrecision = 0;
                                    break;
                                case "varchar":
                                    sqlDbType = MySqlDbType.VarChar;
                                    sqlPrecision = maxLength;
                                    break;
                                default:
                                    throw new Exception(string.Format("Unexpected column value type: {0}", typeName));
                            }

                            ColumnType typeOut;
                            int precisionOut;
                            ConvertColumnType(sqlDbType, sqlPrecision, out typeOut, out precisionOut);

                            ColumnDefinition columnDefinition = new ColumnDefinition(columnName, typeOut, precisionOut);
                            columnDefinition.Committed = true;

                            tableDefinition.Columns[columnName] = columnDefinition;
                        }
                        else
                        {
                            hasIdentityColumn = true;
                        }
                    }

                    if (!hasIdentityColumn)
                    {
                        throw new Exception(string.Format("Missing identity column on table: {0}", tableName));
                    }
                }
            }
            else
            {
                //Log.WriteInfo("Table does not exist.");
            }

            return tableDefinition;
        }

        /// <summary>
        /// Creates a SQL parameter and attaches it to the specified SQL command.
        /// </summary>
        /// <param name="command">Command to attach a new parameter to.</param>
        /// <param name="parameterName">The parameter name.</param>
        /// <param name="parameterValue">The parameter value.</param>
        protected override void CreateParameter(IDbCommand command, string parameterName, object parameterValue)
        {
            MySqlCommand sqlCommand = (MySqlCommand)command;
            sqlCommand.Parameters.AddWithValue(parameterName, parameterValue);
        }

        /// <summary>
        /// Creates a foreign key in the database from a specified table/column pair to a specified table's "_id" column.
        /// </summary>
        /// <param name="fromTable">Table to create a foreign key from.</param>
        /// <param name="fromColumnName">Column to create a foreign key from.</param>
        /// <param name="toTable">Table to create a foreign key to.</param>
        protected override void CreateForeignKey(string fromTable, string fromColumnName, string toTable)
        {
            string commandText = string.Format("ALTER TABLE `{0}` ADD CONSTRAINT `fk_{3}` FOREIGN KEY (`{1}`) REFERENCES `{2}`(`_id`)", fromTable, fromColumnName, toTable, Guid.NewGuid().ToString("N"));
            ExecuteNonQuery(commandText);
        }

        /// <summary>
        /// Generates a column altering SQL command and executes it.
        /// </summary>
        /// <param name="tableDefinition">Table definition for the table to modify.</param>
        /// <param name="columnDefinition">Column definition for the column to modify.</param>
        /// <param name="sqlType">New column type.</param>
        /// <param name="sqlPrecision">New column precision.</param>
        protected override void AlterColumn(TableDefinition tableDefinition, ColumnDefinition columnDefinition, ColumnType sqlType, int sqlPrecision)
        {
            string tableName = tableDefinition.Name;
            string columnName = columnDefinition.Name;

            if (columnDefinition.Committed)
            {
                string commandText = string.Format("ALTER TABLE `{0}` CHANGE COLUMN `{1}` `{1}` {2} NULL", tableName, columnName, ConvertToString(sqlType, sqlPrecision));
                ExecuteNonQuery(commandText);
            }
            else if (tableDefinition.Committed)
            {
                //Log.WriteInfo("Adding column.\nTable: {0}\nColumn: {1}", tableName, columnName);

                string commandText = string.Format("ALTER TABLE `{0}` ADD `{1}` {2} NULL", tableName, columnName, ConvertToString(sqlType, sqlPrecision));
                ExecuteNonQuery(commandText);

                columnDefinition.Committed = true;
            }

            columnDefinition.SqlType = sqlType;
            columnDefinition.SqlPrecision = sqlPrecision;
        }

        /// <summary>
        /// Applies any necessary filtering to the given SQL type and precision.
        /// </summary>
        /// <param name="sqlType">SQL type to filter.</param>
        /// <param name="sqlPrecision">SQL precision to filter.</param>
        protected override void FilterSqlType(ref ColumnType sqlType, ref int sqlPrecision)
        {
            if (sqlType == ColumnType.DateTime)
            {
                sqlType = ColumnType.String16Bit;
                sqlPrecision = 27;
            }
            else if (sqlType == ColumnType.Guid)
            {
                sqlType = ColumnType.String16Bit;
                sqlPrecision = 36;
            }
            else if (sqlType == ColumnType.String8Bit)
            {
                sqlType = ColumnType.String16Bit;
            }
        }

        /// <summary>
        /// Converts a ColumnType and precision to a MySqlDbType and precision.
        /// </summary>
        /// <param name="typeIn">ColumnType to convert.</param>
        /// <param name="precisionIn">Precision to convert.</param>
        /// <param name="typeOut">Converted MySqlDbType.</param>
        /// <param name="precisionOut">Converted precision.</param>
        private static void ConvertColumnType(ColumnType typeIn, int precisionIn, out MySqlDbType typeOut, out int precisionOut)
        {
            switch (typeIn)
            {
                case ColumnType.Boolean:
                    precisionOut = 0;
                    typeOut = MySqlDbType.Bit;
                    break;
                case ColumnType.BigInteger:
                    precisionOut = 20;
                    typeOut = MySqlDbType.Decimal;
                    break;
                case ColumnType.Float:
                    precisionOut = 0;
                    typeOut = MySqlDbType.Float;
                    break;
                case ColumnType.Integer32Bit:
                    precisionOut = 0;
                    typeOut = MySqlDbType.Int32;
                    break;
                case ColumnType.Integer64Bit:
                    precisionOut = 0;
                    typeOut = MySqlDbType.Int64;
                    break;
                case ColumnType.String16Bit:
                    precisionOut = precisionIn;
                    typeOut = MySqlDbType.VarChar;
                    break;

                case ColumnType.String8Bit: // not supported
                    precisionOut = precisionIn;
                    typeOut = MySqlDbType.VarChar;
                    break;
                case ColumnType.DateTime: // not supported
                    precisionOut = 27;
                    typeOut = MySqlDbType.VarChar;
                    break;
                case ColumnType.Guid: // not supported
                    precisionOut = 36;
                    typeOut = MySqlDbType.VarChar;
                    break;

                default:
                    throw new Exception(string.Format("Unexpected column value type: {0}", typeIn));
            }
        }

        /// <summary>
        /// Converts a MySqlDbType and precision to a ColumnType and precision.
        /// </summary>
        /// <param name="typeIn">MySqlDbType to convert.</param>
        /// <param name="precisionIn">Precision to convert.</param>
        /// <param name="typeOut">Converted ColumnType.</param>
        /// <param name="precisionOut">Converted precision.</param>
        private static void ConvertColumnType(MySqlDbType typeIn, int precisionIn, out ColumnType typeOut, out int precisionOut)
        {
            switch (typeIn)
            {
                case MySqlDbType.Bit:
                    precisionOut = 0;
                    typeOut = ColumnType.Boolean;
                    break;
                case MySqlDbType.Decimal:
                    precisionOut = 0;
                    typeOut = ColumnType.BigInteger;
                    break;
                case MySqlDbType.Float:
                    precisionOut = 0;
                    typeOut = ColumnType.Float;
                    break;
                case MySqlDbType.Int32:
                    precisionOut = 0;
                    typeOut = ColumnType.Integer32Bit;
                    break;
                case MySqlDbType.Int64:
                    precisionOut = 0;
                    typeOut = ColumnType.Integer64Bit;
                    break;
                case MySqlDbType.VarChar:
                    precisionOut = precisionIn;
                    typeOut = ColumnType.String16Bit;
                    break;
                default:
                    throw new Exception(string.Format("Unexpected column value type: {0}", typeIn));
            }
        }

        /// <summary>
        /// Converts a SQL type and precision to its string representation for inclusion in a SQL command.
        /// </summary>
        /// <param name="sqlType">Type name.</param>
        /// <param name="sqlPrecision">Type precision.</param>
        /// <returns>The string representation of the given type and precision.</returns>
        private static string ConvertToString(ColumnType sqlType, int sqlPrecision)
        {
            MySqlDbType typeOut;
            int precisionOut;
            ConvertColumnType(sqlType, sqlPrecision, out typeOut, out precisionOut);

            return ConvertToString(typeOut, precisionOut);
        }

        /// <summary>
        /// Converts a SQL type and precision to its string representation for inclusion in a SQL command.
        /// </summary>
        /// <param name="sqlType">Type name.</param>
        /// <param name="sqlPrecision">Type precision.</param>
        /// <returns>The string representation of the given type and precision.</returns>
        private static string ConvertToString(MySqlDbType sqlType, int sqlPrecision)
        {
            switch (sqlType)
            {
                case MySqlDbType.Int64:
                    return "BIGINT(8)";
                case MySqlDbType.Bit:
                    return "BIT";
                case MySqlDbType.Decimal:
                    return "DECIMAL(20,0)";
                case MySqlDbType.Float:
                    return "FLOAT";
                case MySqlDbType.Int32:
                    return "INT(4)";
                case MySqlDbType.VarChar:
                    return string.Format("VARCHAR({0})", sqlPrecision);
                default:
                    throw new Exception(string.Format("Unexpected column value type: {0}", sqlType));
            }
        }

        /// <summary>
        /// Converts an object to a type compatible with the given column type information.
        /// </summary>
        /// <param name="valueIn">Value to convert.</param>
        /// <param name="sqlType">Column type.</param>
        /// <param name="sqlPrecision">Column precision.</param>
        /// <param name="truncated">The number of characters truncated off the result.</param>
        /// <returns>Converted value.</returns>
        protected override object ConvertColumnValue(object valueIn, ColumnType sqlType, int sqlPrecision, out int truncated)
        {
            switch (sqlType)
            {
                case ColumnType.Boolean:
                    truncated = 0;
                    return Convert.ChangeType(valueIn, typeof(bool));
                case ColumnType.BigInteger:
                    truncated = 0;
                    return Convert.ChangeType(valueIn, typeof(ulong));
                case ColumnType.Float:
                    if (valueIn is TimeSpan)
                    {
                        truncated = 0;
                        return Convert.ChangeType(((TimeSpan)valueIn).TotalSeconds, typeof(float));
                    }
                    else
                    {
                        truncated = 0;
                        return Convert.ChangeType(valueIn, typeof(float));
                    }
                case ColumnType.Integer32Bit:
                    truncated = 0;
                    return Convert.ChangeType(valueIn, typeof(int));
                case ColumnType.Integer64Bit:
                    truncated = 0;
                    return Convert.ChangeType(valueIn, typeof(long));
                case ColumnType.String16Bit:
                    return ConvertToString(valueIn, out truncated);

                case ColumnType.String8Bit: // not supported
                    return ConvertToString(valueIn, out truncated);
                case ColumnType.DateTime: // not supported
                    return ConvertToString(valueIn, out truncated);
                case ColumnType.Guid: // not supported
                    return ConvertToString(valueIn, out truncated);

                default:
                    throw new Exception(string.Format("Unexpected column value type: {0}", sqlType));
            }
        }
    }
}
