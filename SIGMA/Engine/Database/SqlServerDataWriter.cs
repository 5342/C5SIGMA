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
using System.Data.SqlClient;
using System.Net;
using System.Text;
using System.Threading;
using Sigma.Common;
using Sigma.Engine.TShark;

namespace Sigma.Engine.Database
{

    /// <summary>
    /// Data writer that emits rows to a SQL Server database.
    /// </summary>
    internal class SqlServerDataWriter : AsyncDataWriter
    {

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="connectionString">The database connection string.</param>
        /// <param name="disableForeignKeys">True to disable generation of foreign keys, false otherwise.</param>
        public SqlServerDataWriter(string connectionString, bool disableForeignKeys)
            : base(DataWriterType.SqlServer, connectionString, disableForeignKeys)
        {
        }
        
        /// <summary>
        /// Connects to the database.
        /// </summary>
        /// <param name="connectionString">Connection string.</param>
        protected override void Connect(string connectionString)
        {
            Log.WriteInfo("Connecting to database.");

            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(connectionString);
            string catalogEscaped = EscapeName(builder.InitialCatalog);
            builder.InitialCatalog = string.Empty;

            Connection = new SqlConnection(builder.ToString());
            Connection.Open();

            Log.WriteInfo("Looking for database: {0}", catalogEscaped);
            int exists = ExecuteScalar<int>(string.Format("IF EXISTS (SELECT name FROM sys.databases WHERE name = N'{0}') SELECT 1 ELSE SELECT 0", catalogEscaped));

            if (exists == 0)
            {
                Log.WriteInfo("Creating new database.");
                ExecuteNonQuery(string.Format("CREATE DATABASE [{0}]", catalogEscaped));
            }
            else
            {
                Log.WriteInfo("Found existing database.");
            }

            ExecuteNonQuery(string.Format("USE [{0}]", catalogEscaped));
        }

        /// <summary>
        /// Inserts a single row into the database by generating a SQL command and attaching appropriate parameters.
        /// </summary>
        /// <param name="tableDefinition">The table definition for the table to modify.</param>
        /// <param name="columnNames">List of column names.</param>
        /// <param name="columnValues">List of column values.</param>
        /// <returns>The identity column value of the inserted row.</returns>
        protected override long InsertRow(TableDefinition tableDefinition, List<string> columnNames, List<object> columnValues)
        {
            using (SqlCommand command = (SqlCommand)Connection.CreateCommand())
            {
                StringBuilder insertCommand = new StringBuilder();
                insertCommand.AppendFormat("INSERT INTO [dbo].[{0}] (", tableDefinition.Name);
                for (int i = 0; i < columnNames.Count; i++)
                {
                    if (i > 0)
                        insertCommand.Append(',');
                    insertCommand.AppendFormat("[{0}]", columnNames[i]);
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
            commandText.AppendFormat("CREATE TABLE [dbo].[{0}] (", tableDefinition.Name);
            commandText.Append("[_id] [BIGINT] IDENTITY(1,1) PRIMARY KEY");
            
            foreach (KeyValuePair<string, ColumnDefinition> column in tableDefinition.Columns)
            {
                commandText.AppendFormat(", [{0}] {1} NULL", column.Key, ConvertToString(column.Value.SqlType, column.Value.SqlPrecision));
                column.Value.Committed = true;
            }
            commandText.Append(")");

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

            int exists = ExecuteScalar<int>(string.Format("IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[{0}]') AND type in (N'U')) SELECT 1 ELSE SELECT 0", tableName));
            if (exists == 1)
            {
                tableDefinition.Committed = true;

                //Log.WriteInfo("Loading schema from existing table.");

                string commandText = string.Format(
@"select 
	c.name, t.name, c.max_length
from 
	sys.columns c join 
	sys.types t 
on 
	c.user_type_id = t.user_type_id
where 
	object_id = OBJECT_ID(N'[dbo].[{0}]')", tableName);

                using (IDataReader reader = ExecuteReader(commandText))
                {
                    bool hasIdentityColumn = false;
                    while (reader.Read())
                    {
                        string columnName = reader.GetString(0);
                        string typeName = reader.GetString(1);
                        int maxLength = reader.GetInt16(2);

                        if (!columnName.Equals("_id", StringComparison.OrdinalIgnoreCase))
                        {
                            SqlDbType sqlDbType;
                            int sqlPrecision;

                            switch (typeName.ToLowerInvariant())
                            {
                                case "bigint":
                                    sqlDbType = SqlDbType.BigInt;
                                    sqlPrecision = 0;
                                    break;
                                case "bit":
                                    sqlDbType = SqlDbType.Bit;
                                    sqlPrecision = 0;
                                    break;
                                case "datetime2":
                                    sqlDbType = SqlDbType.DateTime2;
                                    sqlPrecision = 7;
                                    break;
                                case "numeric":
                                    sqlDbType = SqlDbType.Decimal;
                                    sqlPrecision = maxLength;
                                    break;
                                case "float":
                                    sqlDbType = SqlDbType.Float;
                                    sqlPrecision = 0;
                                    break;
                                case "int":
                                    sqlDbType = SqlDbType.Int;
                                    sqlPrecision = 0;
                                    break;
                                case "nvarchar":
                                    sqlDbType = SqlDbType.NVarChar;
                                    sqlPrecision = maxLength / 2;
                                    break;
                                case "uniqueidentifier":
                                    sqlDbType = SqlDbType.UniqueIdentifier;
                                    sqlPrecision = 0;
                                    break;
                                case "varchar":
                                    sqlDbType = SqlDbType.VarChar;
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
            SqlCommand sqlCommand = (SqlCommand)command;
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
            string commandText = string.Format("ALTER TABLE [dbo].[{0}] ADD CONSTRAINT [fk_{3}] FOREIGN KEY ([{1}]) REFERENCES [dbo].[{2}]([_id])", fromTable, fromColumnName, toTable, Guid.NewGuid().ToString("N"));
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
                if (sqlType == columnDefinition.SqlType)
                {
                    //Log.WriteInfo("Altering column precision.\nTable: {0}\nColumn: {1}", tableName, columnName);

                    string commandText = string.Format("ALTER TABLE [dbo].[{0}] ALTER COLUMN [{1}] {2} NULL", tableName, columnName, ConvertToString(sqlType, sqlPrecision));
                    ExecuteNonQuery(commandText);
                }
                else
                {
                    //Log.WriteInfo("Altering column type.\nTable: {0}\nColumn: {1}", tableName, columnName);

                    string commandText;
                    string typeAndPrecision = ConvertToString(sqlType, sqlPrecision);
                    
                    commandText = string.Format("SELECT [_id], [{0}] INTO #temp FROM [dbo].[{1}]", columnName, tableName);
                    ExecuteNonQuery(commandText);

                    commandText = string.Format("ALTER TABLE [dbo].[{0}] DROP COLUMN [{1}]", tableName, columnName);
                    ExecuteNonQuery(commandText);

                    commandText = string.Format("ALTER TABLE [dbo].[{0}] ADD [{1}] {2} NULL", tableName, columnName, typeAndPrecision);
                    ExecuteNonQuery(commandText);

                    commandText = string.Format("UPDATE [dbo].[{0}] SET [dbo].[{0}].[{1}] = CONVERT({2}, [#temp].[{1}]) FROM #temp WHERE [#temp].[_id] = [dbo].[{0}].[_id]", tableName, columnName, typeAndPrecision);
                    ExecuteNonQuery(commandText);

                    commandText = string.Format("DROP TABLE #temp");
                    ExecuteNonQuery(commandText);
                }
            }
            else if (tableDefinition.Committed)
            {
                //Log.WriteInfo("Adding column.\nTable: {0}\nColumn: {1}", tableName, columnName);

                string commandText = string.Format("ALTER TABLE [dbo].[{0}] ADD [{1}] {2} NULL", tableName, columnName, ConvertToString(sqlType, sqlPrecision));
                ExecuteNonQuery(commandText);

                columnDefinition.Committed = true;
            }

            columnDefinition.SqlType = sqlType;
            columnDefinition.SqlPrecision = sqlPrecision;
        }

        /// <summary>
        /// Converts a ColumnType and precision to a SqlDbType and precision.
        /// </summary>
        /// <param name="typeIn">ColumnType to convert.</param>
        /// <param name="precisionIn">Precision to convert.</param>
        /// <param name="typeOut">Converted SqlDbType.</param>
        /// <param name="precisionOut">Converted precision.</param>
        private static void ConvertColumnType(ColumnType typeIn, int precisionIn, out SqlDbType typeOut, out int precisionOut)
        {
            switch (typeIn)
            {
                case ColumnType.Boolean:
                    precisionOut = 0;
                    typeOut = SqlDbType.Bit;
                    break;
                case ColumnType.DateTime:
                    precisionOut = 7;
                    typeOut = SqlDbType.DateTime2;
                    break;
                case ColumnType.BigInteger:
                    precisionOut = 20;
                    typeOut = SqlDbType.Decimal;
                    break;
                case ColumnType.Float:
                    precisionOut = 0;
                    typeOut = SqlDbType.Float;
                    break;
                case ColumnType.Guid:
                    precisionOut = 0;
                    typeOut = SqlDbType.UniqueIdentifier;
                    break;
                case ColumnType.Integer32Bit:
                    precisionOut = 0;
                    typeOut = SqlDbType.Int;
                    break;
                case ColumnType.Integer64Bit:
                    precisionOut = 0;
                    typeOut = SqlDbType.BigInt;
                    break;
                case ColumnType.String16Bit:
                    precisionOut = precisionIn;
                    typeOut = SqlDbType.NVarChar;
                    break;
                case ColumnType.String8Bit:
                    precisionOut = precisionIn;
                    typeOut = SqlDbType.VarChar;
                    break;
                default:
                    throw new Exception(string.Format("Unexpected column value type: {0}", typeIn));
            }
        }

        /// <summary>
        /// Converts a SqlDbType and precision to a ColumnType and precision.
        /// </summary>
        /// <param name="typeIn">SqlDbType to convert.</param>
        /// <param name="precisionIn">Precision to convert.</param>
        /// <param name="typeOut">Converted ColumnType.</param>
        /// <param name="precisionOut">Converted precision.</param>
        private static void ConvertColumnType(SqlDbType typeIn, int precisionIn, out ColumnType typeOut, out int precisionOut)
        {
            switch (typeIn)
            {
                case SqlDbType.Bit:
                    precisionOut = 0;
                    typeOut = ColumnType.Boolean;
                    break;
                case SqlDbType.DateTime2:
                    precisionOut = 0;
                    typeOut = ColumnType.DateTime;
                    break;
                case SqlDbType.Decimal:
                    precisionOut = 0;
                    typeOut = ColumnType.BigInteger;
                    break;
                case SqlDbType.Float:
                    precisionOut = 0;
                    typeOut = ColumnType.Float;
                    break;
                case SqlDbType.UniqueIdentifier:
                    precisionOut = 0;
                    typeOut = ColumnType.Guid;
                    break;
                case SqlDbType.Int:
                    precisionOut = 0;
                    typeOut = ColumnType.Integer32Bit;
                    break;
                case SqlDbType.BigInt:
                    precisionOut = 0;
                    typeOut = ColumnType.Integer64Bit;
                    break;
                case SqlDbType.NVarChar:
                    precisionOut = precisionIn;
                    typeOut = ColumnType.String16Bit;
                    break;
                case SqlDbType.VarChar:
                    precisionOut = precisionIn;
                    typeOut = ColumnType.String8Bit;
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
            SqlDbType typeOut;
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
        private static string ConvertToString(SqlDbType sqlType, int sqlPrecision)
        {
            switch (sqlType)
            {
                case SqlDbType.BigInt:
                    return "BIGINT";
                case SqlDbType.Bit:
                    return "BIT";
                case SqlDbType.DateTime2:
                    return "DATETIME2(7)";
                case SqlDbType.Decimal:
                    return string.Format("NUMERIC({0},0)", sqlPrecision);
                case SqlDbType.Float:
                    return "FLOAT";
                case SqlDbType.Int:
                    return "INT";
                case SqlDbType.NVarChar:
                    return string.Format("NVARCHAR({0})", sqlPrecision);
                case SqlDbType.UniqueIdentifier:
                    return "UNIQUEIDENTIFIER";
                case SqlDbType.VarChar:
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
            return ConvertToString(valueIn, out truncated);
        }
    }
}
