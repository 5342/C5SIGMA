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
    /// Data writer that emits rows to a database asynchronously.
    /// </summary>
    internal abstract class AsyncDataWriter : DataWriter
    {

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="type">This instance's type.</param>
        /// <param name="connectionString">The database connection string.</param>
        /// <param name="disableForeignKeys">True to disable generation of foreign keys, false otherwise.</param>
        public AsyncDataWriter(DataWriterType type, string connectionString, bool disableForeignKeys)
            : base(type)
        {
            this.connectionString = connectionString;
            this.disableForeignKeys = disableForeignKeys;
        }

        /// <summary>
        /// The database connection string.
        /// </summary>
        private string connectionString;

        /// <summary>
        /// True to disable generation of foreign keys, false otherwise.
        /// </summary>
        private bool disableForeignKeys;

        /// <summary>
        /// Connects to the database.
        /// </summary>
        /// <remarks>
        /// Implementations of this method should update the Connection property.
        /// </remarks>
        /// <param name="connectionString">Connection string.</param>
        protected abstract void Connect(string connectionString);

        /// <summary>
        /// Escapes a string identifier for direct inclusion in SQL.
        /// </summary>
        /// <remarks>
        /// Any character in the input that is not a letter or a digit is replaced by an underscore.
        /// Names are truncated to an arbitrary length (96) by removing characters from near the middle.
        /// </remarks>
        /// <param name="id">Identifier to escape.</param>
        /// <returns>Escaped string.</returns>
        protected virtual string EscapeName(string id)
        {
            const int MaxLength = 128 - 32;

            StringBuilder builder = new StringBuilder(id.Length);
            for (int i = 0; i < id.Length; i++)
            {
                char c = id[i];
                if (!char.IsLetterOrDigit(c))
                {
                    builder.Append('_');
                }
                else
                {
                    builder.Append(c);
                }
            }

            string result;
            if (builder.Length > MaxLength)
            {
                string truncated = builder.ToString();

                int toRemove = builder.Length - MaxLength + 3;
                int leftPad = (builder.Length - toRemove) / 4;

                result = string.Concat(truncated.Substring(0, leftPad), "___", truncated.Substring(leftPad + toRemove));

                Log.WriteWarning("Truncated table name.\nFrom: {0}\nTo: {1}", truncated, result);
            }
            else
            {
                result = builder.ToString();
            }

            return result;
        }

        /// <summary>
        /// Escapes the given row recursively (child rows are also escaped).
        /// </summary>
        /// <remarks>
        /// After calling this method it is safe to use row and column names directly in SQL.
        /// </remarks>
        /// <param name="row">Row to escape recursively.</param>
        private void EscapeRow(CapDataRow row)
        {
            row.Table = EscapeName(row.Table);

            if (row.Columns.Count > 0)
            {
                Dictionary<string, object> escapedColumns = new Dictionary<string, object>();
                foreach (KeyValuePair<string, object> column in row.Columns)
                {
                    escapedColumns[EscapeName(column.Key)] = column.Value;
                }
                row.Columns = escapedColumns;
            }

            foreach (CapDataRow childRow in row.ChildRows)
            {
                EscapeRow(childRow);
            }
        }

        /// <summary>
        /// The active database connection.
        /// </summary>
        protected IDbConnection Connection { get; set; }

        /// <summary>
        /// Writes a single data row.
        /// </summary>
        /// <remarks>
        /// This method queues the given row to be written asynchronously.
        /// </remarks>
        /// <param name="row">Row to write.</param>
        public override void WriteRow(CapDataRow row)
        {
            while (true)
            {
                EnsureWriteAsyncActive();
                if (allowEnqueue.Wait(500))
                {
                    break;
                }
            }

            lock (backlog)
            {
                backlog.Enqueue(row);
                if (backlog.Count >= MaxBacklog)
                {
                    allowEnqueue.Reset();
                }
                Monitor.PulseAll(backlog);
            }
        }

        /// <summary>
        /// Ensures that the asynchronous row writing thread is active.
        /// </summary>
        private void EnsureWriteAsyncActive()
        {
            lock (backlog)
            {
                if (writeAsyncInactive.Wait(0))
                {
                    if (asyncFailures >= 3)
                    {
                        throw new Exception("Too many asynchronous failures.");
                    }

                    writeAsyncInactive.Reset();
                    writeAsyncThread = new Thread(WriteAsync);
                    writeAsyncThread.IsBackground = true;
                    writeAsyncThread.Start();
                }
            }
        }

        /// <summary>
        /// Dictionary of source file paths and their corresponding identifiers.
        /// </summary>
        /// <remarks>
        /// This dictionary is constructed as rows are written to the "geninfo" table.
        /// </remarks>
        private Dictionary<string, long> files = new Dictionary<string, long>();

        /// <summary>
        /// Maximum number of rows to queue for asynchronous write before the calling thread is blocked.
        /// </summary>
        private const int MaxBacklog = 1000;

        /// <summary>
        /// Asynchronous write thread, or null.
        /// </summary>
        private Thread writeAsyncThread = null;

        /// <summary>
        /// This event is set when no asynchronous write thread is active.
        /// </summary>
        private ManualResetEventSlim writeAsyncInactive = new ManualResetEventSlim(true);

        /// <summary>
        /// This event is set when rows can be enqueued for asynchronous write without blocking.
        /// </summary>
        private ManualResetEventSlim allowEnqueue = new ManualResetEventSlim(true);

        /// <summary>
        /// Queue of rows waiting to be written asynchronously.
        /// </summary>
        private Queue<CapDataRow> backlog = new Queue<CapDataRow>();

        /// <summary>
        /// Tracks the number of failures that have occurred asynchronously.
        /// </summary>
        private int asyncFailures = 0;

        /// <summary>
        /// Main body/loop of the asynchronous row writing thread.
        /// </summary>
        private void WriteAsync()
        {
            try
            {
                Connect(connectionString);

                while (true)
                {
                    CapDataRow next;
                    lock (backlog)
                    {
                        while (true)
                        {
                            if (backlog.Count > 0)
                            {
                                next = backlog.Dequeue();
                                if (backlog.Count <= MaxBacklog / 2)
                                {
                                    allowEnqueue.Set();
                                }
                                break;
                            }

                            Monitor.Wait(backlog, 500);
                        }
                    }

                    if (next != null)
                    {
                        EscapeRow(next);
                        WriteRowInternal(null, next, null);
                    }
                    else
                    {
                        break;
                    }
                }
            }
            catch (Exception ex)
            {
                Interlocked.Increment(ref asyncFailures);
                Log.WriteError("Asynchronous write failure. Error: {0}", ex.Message);
            }
            finally
            {
                writeAsyncInactive.Set();

                if (Connection != null)
                {
                    Log.WriteInfo("Disconnecting from database.");

                    try
                    {
                        Connection.Close();
                    }
                    catch
                    {
                    }
                    Connection = null;
                }
            }
        }

        /// <summary>
        /// Called internally by the asynchronous row writing thread to recursively insert a row and all its children into the database.
        /// </summary>
        /// <param name="parent">Parent row, or null if none.</param>
        /// <param name="row">Row to insert.</param>
        /// <param name="rowInfo">Additional row information accumulated during recursion, or null if none.</param>
        private void WriteRowInternal(CapDataRow parent, CapDataRow row, RowInfo rowInfo)
        {
            bool includeRowInfo = (rowInfo != null);
            if (rowInfo == null && row.Table.Equals("geninfo", StringComparison.OrdinalIgnoreCase))
            {
                string path = Convert.ToString(row.Columns["file"]);
                long number = Convert.ToInt64(row.Columns["num"]);
                DateTime timestamp = TSharkTypeParser.ParseDateTime(Convert.ToString(row.Columns["timestamp"]));

                rowInfo = CreateRowInfo(path, number, timestamp);
            }

            if (Filter != null)
            {
                if (Filter.FilterTable(row.Table) == DataFilterType.Deny)
                {
                    foreach (CapDataRow childRow in row.ChildRows)
                    {
                        WriteRowInternal(parent, childRow, rowInfo);
                    }
                    return;
                }
            }

            TableDefinition tableDefinition = GetTableDefinition(row.Table);

            List<string> columnNames = new List<string>();
            List<object> columnValues = new List<object>();

            string parentColumnName = null;
            bool createForeignKey = false;
            bool createSourceFileForeignKey = false;

            int generatedColumnCount = 0;
            
            int truncated;
            List<string> truncatedColumns = null;

            if (includeRowInfo)
            {
                createSourceFileForeignKey = !tableDefinition.Columns.ContainsKey("_sourcefileid");

                columnNames.Add("_sourcefileid");
                columnNames.Add("_number");
                columnNames.Add("_timestamp");

                ColumnDefinition columnInfo;

                columnInfo = CreateOrUpdateColumnDefinition(tableDefinition, "_sourcefileid", ColumnType.Integer64Bit, 0);
                columnValues.Add(ConvertColumnValue(rowInfo.SourceFileID, columnInfo.SqlType, columnInfo.SqlPrecision, out truncated));

                columnInfo = CreateOrUpdateColumnDefinition(tableDefinition, "_number", ColumnType.Integer64Bit, 0);
                columnValues.Add(ConvertColumnValue(rowInfo.Number, columnInfo.SqlType, columnInfo.SqlPrecision, out truncated));

                columnInfo = CreateOrUpdateColumnDefinition(tableDefinition, "_timestamp", ColumnType.DateTime, 0);
                columnValues.Add(ConvertColumnValue(rowInfo.Timestamp, columnInfo.SqlType, columnInfo.SqlPrecision, out truncated));

                generatedColumnCount += 3;
            }

            if (parent != null)
            {
                parentColumnName = string.Format("parent_{0}", parent.Table);
                createForeignKey = !tableDefinition.Columns.ContainsKey(parentColumnName);

                ColumnDefinition columnInfo = CreateOrUpdateColumnDefinition(tableDefinition, parentColumnName, ColumnType.Integer64Bit, 0);

                columnNames.Add(parentColumnName);
                columnValues.Add(ConvertColumnValue(parent.ID, columnInfo.SqlType, columnInfo.SqlPrecision, out truncated));

                generatedColumnCount++;
            }

            foreach (KeyValuePair<string, object> column in row.Columns)
            {
                bool dropColumn = false;

                // comment out the next if-block to enable storage of binary data in the database, this can be very storage intensive
                // binary data is stored in the database as variable length strings and may be arbitrarily truncated to fit
                if (column.Value is byte[])
                {
                    dropColumn = true;
                }

                if (Filter != null)
                {
                    if (Filter.FilterColumn(row.Table, column.Key) == DataFilterType.Deny)
                    {
                        dropColumn = true;
                    }
                }

                if (!dropColumn)
                {
                    ColumnType sqlType;
                    int sqlPrecision;
                    GetSqlTypeAndPrecision(column.Value, out sqlType, out sqlPrecision);
                    ColumnDefinition columnInfo = CreateOrUpdateColumnDefinition(tableDefinition, column.Key, sqlType, sqlPrecision);

                    object convertedColumnValue = ConvertColumnValue(column.Value, columnInfo.SqlType, columnInfo.SqlPrecision, out truncated);

                    if (truncated > 0)
                    {
                        if (truncatedColumns == null)
                            truncatedColumns = new List<string>();
                        truncatedColumns.Add(column.Key);
                    }

                    columnNames.Add(column.Key);
                    columnValues.Add(convertedColumnValue);
                }
            }

            if (!tableDefinition.Committed)
            {
                CreateTable(tableDefinition);
            }

            if (!disableForeignKeys)
            {
                if (createForeignKey)
                {
                    //Log.WriteInfo("Creating foreign key.\nFrom table: {0}\nTo table: {1}", row.Table, parent.Table);

                    CreateForeignKey(row.Table, parentColumnName, parent.Table);
                }

                if (createSourceFileForeignKey)
                {
                    CreateSourceFileForeignKey(row.Table);
                }
            }

            row.ID = InsertRow(tableDefinition, columnNames, columnValues);

            if (truncatedColumns != null)
            {
                foreach (string truncatedColumn in truncatedColumns)
                {
                    Log.WriteWarning("Truncated column value.\nTable: {0}\nID: {1}\nColumn: {2}", tableDefinition.Name, row.ID, truncatedColumn);
                }
            }

            foreach (CapDataRow childRow in row.ChildRows)
            {
                WriteRowInternal(row, childRow, rowInfo);
            }
        }

        /// <summary>
        /// Creates a foreign key linking a table's "_sourcefileid" column to the "sourcefile" table's "_id" column.
        /// </summary>
        /// <param name="table">Table to create the foreign key from.</param>
        private void CreateSourceFileForeignKey(string table)
        {
            CreateForeignKey(table, "_sourcefileid", "sourcefile");
        }

        /// <summary>
        /// Creates a foreign key in the database from a specified table/column pair to a specified table's "_id" column.
        /// </summary>
        /// <param name="fromTable">Table to create a foreign key from.</param>
        /// <param name="fromColumnName">Column to create a foreign key from.</param>
        /// <param name="toTable">Table to create a foreign key to.</param>
        protected abstract void CreateForeignKey(string fromTable, string fromColumnName, string toTable);

        /// <summary>
        /// Inserts a single row into the database by generating a SQL command and attaching appropriate parameters.
        /// </summary>
        /// <param name="tableDefinition">The table definition for the table to modify.</param>
        /// <param name="columnNames">List of column names.</param>
        /// <param name="columnValues">List of column values.</param>
        /// <returns>The identity column value of the inserted row.</returns>
        protected abstract long InsertRow(TableDefinition tableDefinition, List<string> columnNames, List<object> columnValues);

        /// <summary>
        /// Creates a new row information instance.
        /// </summary>
        /// <remarks>
        /// This method may create or modify database tables so that the source file identifier is valid.
        /// </remarks>
        /// <param name="path">Source (packet capture) file path.</param>
        /// <param name="number">Packet number.</param>
        /// <param name="timestamp">Packet timestamp.</param>
        /// <returns>The new RowInfo instance.</returns>
        private RowInfo CreateRowInfo(string path, long number, DateTime timestamp)
        {
            try
            {
                long sourceFileID;
                if (!files.TryGetValue(path, out sourceFileID))
                {
                    TableDefinition filesTableDefinition = GetTableDefinition("sourcefile");
                    if (!filesTableDefinition.Committed)
                    {
                        CreateOrUpdateColumnDefinition(filesTableDefinition, "path", ColumnType.String16Bit, 4000);
                        CreateTable(filesTableDefinition);
                    }

                    List<string> columnNames = new List<string> { "path" };
                    List<object> columnValues = new List<object> { path };

                    sourceFileID = InsertRow(filesTableDefinition, columnNames, columnValues);
                    files[path] = sourceFileID;
                }

                RowInfo rowInfo = new RowInfo();
                rowInfo.SourceFileID = sourceFileID;
                rowInfo.Number = number;
                rowInfo.Timestamp = timestamp;
                return rowInfo;
            }
            catch (Exception ex)
            {
                Log.WriteError("Unable to save source file path.\nPath: {0}\nError: {1}", path, ex.Message);
                return null;
            }
        }

        /// <summary>
        /// Creates or updates a column definition for a given table.
        /// </summary>
        /// <remarks>
        /// This method may interact with the database if a column is added or changed for an existing table.
        /// </remarks>
        /// <param name="tableDefinition">Table definition to modify.</param>
        /// <param name="columnName">Name of the column to create or update.</param>
        /// <param name="sqlType">New column type.</param>
        /// <param name="sqlPrecision">New column precision.</param>
        /// <returns>A new or existing ColumnDefinition instance.</returns>
        private ColumnDefinition CreateOrUpdateColumnDefinition(TableDefinition tableDefinition, string columnName, ColumnType sqlType, int sqlPrecision)
        {
            FilterSqlType(ref sqlType, ref sqlPrecision);

            ColumnDefinition columnDefinition;
            if (!tableDefinition.Columns.TryGetValue(columnName, out columnDefinition))
            {
                columnDefinition = new ColumnDefinition(columnName, sqlType, sqlPrecision);
                tableDefinition.Columns[columnName] = columnDefinition;

                if (tableDefinition.Committed)
                {
                    AlterColumn(tableDefinition, columnDefinition, sqlType, sqlPrecision);
                }
            }
            else
            {
                UpdateColumnDefinition(tableDefinition, columnDefinition, sqlType, sqlPrecision);
            }
            return columnDefinition;
        }

        /// <summary>
        /// Creates a new table using the given table definition.
        /// </summary>
        /// <remarks>
        /// This method generates a table creation SQL statement and executes it.
        /// </remarks>
        /// <param name="tableDefinition">Table definition for the table to create.</param>
        protected abstract void CreateTable(TableDefinition tableDefinition);

        /// <summary>
        /// Flushes this instance.
        /// </summary>
        /// <remarks>
        /// This method signals that the asynchronous write thread should terminate after writing all pending rows, and then blocks waiting for it to do so.
        /// </remarks>
        protected override void Flush()
        {
            Log.WriteInfo("Flushing backlog to database.");

            lock (backlog)
            {
                EnsureWriteAsyncActive();
                backlog.Enqueue(null);   
            }

            writeAsyncInactive.Wait();
        }

        /// <summary>
        /// Table definitions, indexed by name.
        /// </summary>
        private Dictionary<string, TableDefinition> tables = new Dictionary<string, TableDefinition>();

        /// <summary>
        /// Gets the a definition for the named table.
        /// </summary>
        /// <remarks>
        /// If a definition is already cached for the named table this definition is returned, otherwise a definition is loaded from the database.
        /// </remarks>
        /// <param name="tableName">Table name to get a definition for.</param>
        /// <returns>The table definition for the named table.</returns>
        private TableDefinition GetTableDefinition(string tableName)
        {
            TableDefinition result;
            if (!tables.TryGetValue(tableName, out result))
            {
                result = LoadTableDefinition(tableName);
                tables[tableName] = result;
            }
            return result;
        }

        /// <summary>
        /// Loads a table definition from the database.
        /// </summary>
        /// <param name="tableName">Name of the table for which a definition is to be loaded.</param>
        /// <returns>The table definition for the named table.</returns>
        protected abstract TableDefinition LoadTableDefinition(string tableName);

        /// <summary>
        /// Updates a column with a new type and precision.
        /// </summary>
        /// <remarks>
        /// This method determines whether modifications are required and if they are performs appropriate SQL operations.
        /// </remarks>
        /// <param name="tableDefinition">Table definition to modify.</param>
        /// <param name="columnDefinition">Column definition to modify.</param>
        /// <param name="newSqlType">New column type.</param>
        /// <param name="newSqlPrecision">New column precision.</param>
        /// <returns>True if a definition was changed, false otherwise.</returns>
        private bool UpdateColumnDefinition(TableDefinition tableDefinition, ColumnDefinition columnDefinition, ColumnType newSqlType, int newSqlPrecision)
        {
            ColumnType sqlType = columnDefinition.SqlType;
            int sqlPrecision = columnDefinition.SqlPrecision;

            bool? changed = null;
            if (sqlType == ColumnType.String16Bit && sqlPrecision == 4000)
            {
                changed = false;
            }
            else if (newSqlType == sqlType)
            {
                if (newSqlPrecision > sqlPrecision)
                {
                    sqlPrecision = newSqlPrecision;
                    changed = true;
                }
                else
                {
                    changed = false;
                }
            }
            else if (sqlType == ColumnType.Integer32Bit)
            {
                if (newSqlType == ColumnType.Integer64Bit || newSqlType == ColumnType.BigInteger)
                {
                    sqlType = newSqlType;
                    sqlPrecision = newSqlPrecision;
                    changed = true;
                }
            }
            else if (sqlType == ColumnType.Integer64Bit)
            {
                if (newSqlType == ColumnType.BigInteger)
                {
                    sqlType = newSqlType;
                    sqlPrecision = newSqlPrecision;
                    changed = true;
                }
                else if (newSqlType == ColumnType.Integer32Bit)
                {
                    changed = false;
                }
            }
            else if (sqlType == ColumnType.BigInteger)
            {
                if (newSqlType == ColumnType.Integer32Bit || newSqlType == ColumnType.Integer64Bit)
                {
                    changed = false;
                }
            }
            else if (sqlType == ColumnType.String8Bit)
            {
                if (newSqlType == ColumnType.String16Bit)
                {
                    sqlType = ColumnType.String16Bit;
                    sqlPrecision = Math.Min(Math.Max(sqlPrecision, newSqlPrecision), 4000);
                    changed = true;
                }
            }

            if (changed == null)
            {
                Log.WriteWarning("SQL column type incompatibility.\nTable: {0}\nColumn: {1}\nExpected: {2}\nFound: {3}", tableDefinition.Name, columnDefinition.Name, sqlType, newSqlType);

                sqlType = ColumnType.String16Bit;
                sqlPrecision = 4000;
                changed = true;
            }

            if ((bool)changed)
            {
                AlterColumn(tableDefinition, columnDefinition, sqlType, sqlPrecision);
            }

            return (bool)changed;
        }

        /// <summary>
        /// Generates a column altering SQL command and executes it.
        /// </summary>
        /// <param name="tableDefinition">Table definition for the table to modify.</param>
        /// <param name="columnDefinition">Column definition for the column to modify.</param>
        /// <param name="sqlType">New column type.</param>
        /// <param name="sqlPrecision">New column precision.</param>
        protected abstract void AlterColumn(TableDefinition tableDefinition, ColumnDefinition columnDefinition, ColumnType sqlType, int sqlPrecision);

        /// <summary>
        /// Creates a SQL command on the active connection.
        /// </summary>
        /// <remarks>
        /// The given parameters are added to the new command instances using a naming scheme "@0", "@1", ..., "@n".
        /// </remarks>
        /// <param name="query">Command text.</param>
        /// <param name="parameters">Indexed parameter values.</param>
        /// <returns>The new SQL command.</returns>
        protected virtual IDbCommand CreateCommand(string query, params object[] parameters)
        {
            IDbCommand command = Connection.CreateCommand();
            command.CommandText = query;
            if (parameters != null && parameters.Length > 0)
            {
                for (int i = 0; i < parameters.Length; i++)
                {
                    CreateParameter(command, string.Format("@{0}", i), parameters[i]);
                }
            }
            return command;
        }

        /// <summary>
        /// Creates a SQL parameter and attaches it to the specified SQL command.
        /// </summary>
        /// <param name="command">Command to attach a new parameter to.</param>
        /// <param name="parameterName">The parameter name.</param>
        /// <param name="parameterValue">The parameter value.</param>
        protected abstract void CreateParameter(IDbCommand command, string parameterName, object parameterValue);

        /// <summary>
        /// Executes a SQL command on the active connection and returns a data reader for the results.
        /// </summary>
        /// <remarks>
        /// The given parameters are added to the new command instances using a naming scheme "@0", "@1", ..., "@n".
        /// </remarks>
        /// <param name="query">Command text.</param>
        /// <param name="parameters">Indexed parameter values.</param>
        /// <returns>A new data reader.</returns>
        protected virtual IDataReader ExecuteReader(string query, params object[] parameters)
        {
            try
            {
                using (IDbCommand command = CreateCommand(query, parameters))
                {
                    return command.ExecuteReader();
                }
            }
            catch (Exception ex)
            {
                Log.WriteError("Error executing SQL command.\nCommand: {0}\nError: {1}", query, ex.Message);
                throw new Exception("Database failure.", ex);
            }
        }

        /// <summary>
        /// Executes a SQL command on the active connection and returns the scalar result.
        /// </summary>
        /// <typeparam name="T">Scalar result type.</typeparam>
        /// <remarks>
        /// The given parameters are added to the new command instances using a naming scheme "@0", "@1", ..., "@n".
        /// </remarks>
        /// <param name="query">Command text.</param>
        /// <param name="parameters">Indexed parameter values.</param>
        /// <returns>A new data reader.</returns>
        protected virtual T ExecuteScalar<T>(string query, params object[] parameters)
        {
            try
            {
                using (IDbCommand command = CreateCommand(query, parameters))
                {
                    return (T)Convert.ChangeType(command.ExecuteScalar(), typeof(T));
                }
            }
            catch (Exception ex)
            {
                Log.WriteError("Error executing SQL command.\nCommand: {0}\nError: {1}", query, ex.Message);
                throw new Exception("Database failure.", ex);
            }
        }

        /// <summary>
        /// Executes a SQL command on the active connection.
        /// </summary>
        /// <remarks>
        /// The given parameters are added to the new command instances using a naming scheme "@0", "@1", ..., "@n".
        /// </remarks>
        /// <param name="query">Command text.</param>
        /// <param name="parameters">Indexed parameter values.</param>
        protected virtual void ExecuteNonQuery(string query, params object[] parameters)
        {
            try
            {
                using (IDbCommand command = CreateCommand(query, parameters))
                {
                    command.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                Log.WriteError("Error executing SQL command.\nCommand: {0}\nError: {1}", query, ex.Message);
                throw new Exception("Database failure.", ex);
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
        protected abstract object ConvertColumnValue(object valueIn, ColumnType sqlType, int sqlPrecision, out int truncated);

        /// <summary>
        /// Applies any necessary filtering to the given SQL type and precision.
        /// </summary>
        /// <param name="sqlType">SQL type to filter.</param>
        /// <param name="sqlPrecision">SQL precision to filter.</param>
        protected virtual void FilterSqlType(ref ColumnType sqlType, ref int sqlPrecision)
        {
        }

        /// <summary>
        /// Determines an appropriate SQL type and precision for the given column value.
        /// </summary>
        /// <param name="value">Column value to examine.</param>
        /// <param name="sqlType">Receives the SQL type.</param>
        /// <param name="sqlPrecision">Receives the SQL type precision.</param>
        protected virtual void GetSqlTypeAndPrecision(object value, out ColumnType sqlType, out int sqlPrecision)
        {
            if (value == null)
            {
                sqlType = ColumnType.String16Bit;
                sqlPrecision = 1;
            }
            else
            {
                Type runtimeType = value.GetType();
                if (runtimeType == typeof(object[]))
                {
                    sqlType = ColumnType.String16Bit;
                    sqlPrecision = 4000;
                }
                else if (runtimeType == typeof(string))
                {
                    string stringValue = (string)value;
                    if (stringValue.Length > 2000)
                    {
                        sqlType = ColumnType.String16Bit;
                        sqlPrecision = 4000;
                    }
                    else if (stringValue.Length > 1000)
                    {
                        sqlType = ColumnType.String16Bit;
                        sqlPrecision = 2000;
                    }
                    else if (stringValue.Length > 500)
                    {
                        sqlType = ColumnType.String16Bit;
                        sqlPrecision = 1000;
                    }
                    else if (stringValue.Length > 250)
                    {
                        sqlType = ColumnType.String16Bit;
                        sqlPrecision = 500;
                    }
                    else
                    {
                        sqlType = ColumnType.String16Bit;
                        sqlPrecision = 250;
                    }
                }
                else if (runtimeType == typeof(sbyte) || runtimeType == typeof(short) || runtimeType == typeof(int) || runtimeType == typeof(byte) || runtimeType == typeof(ushort))
                {
                    sqlType = ColumnType.Integer32Bit;
                    sqlPrecision = 0;
                }
                else if (runtimeType == typeof(long) || runtimeType == typeof(uint))
                {
                    sqlType = ColumnType.Integer64Bit;
                    sqlPrecision = 0;
                }
                else if (runtimeType == typeof(ulong))
                {
                    sqlType = ColumnType.BigInteger;
                    sqlPrecision = 20;
                }
                else if (runtimeType == typeof(bool))
                {
                    sqlType = ColumnType.Boolean;
                    sqlPrecision = 0;
                }
                else if (runtimeType == typeof(Guid))
                {
                    sqlType = ColumnType.Guid;
                    sqlPrecision = 0;
                }
                else if (runtimeType == typeof(byte[]))
                {
                    sqlType = ColumnType.String8Bit;

                    byte[] binaryValue = (byte[])value;
                    sqlPrecision = Math.Min(2 * 64 * (binaryValue.Length + 63) / 64, 8000);
                }
                else if (runtimeType == typeof(DateTime))
                {
                    sqlType = ColumnType.DateTime;
                    sqlPrecision = 0;
                }
                else if (runtimeType == typeof(float) || runtimeType == typeof(double) || runtimeType == typeof(TimeSpan))
                {
                    sqlType = ColumnType.Float;
                    sqlPrecision = 0;
                }
                else if (runtimeType == typeof(IPAddress))
                {
                    sqlType = ColumnType.String8Bit;
                    sqlPrecision = 64;
                }
                else
                {
                    throw new Exception(string.Format("Unexpected column value type: {0}", runtimeType));
                }
            }
        }

        /// <summary>
        /// Converts a value to its string representation for use as a parameter in a SQL command.
        /// </summary>
        /// <remarks>
        /// WARNING: The string returned by this method is NOT escaped. Do not include it directly in SQL. 
        /// </remarks>
        /// <param name="value">Value to convert to a string.</param>
        /// <param name="truncated">The number of characters truncated off the result string.</param>
        /// <returns>The converted string value.</returns>
        protected static string ConvertToString(object value, out int truncated)
        {
            truncated = 0;
            if (value == null)
            {
                return string.Empty;
            }
            else
            {
                Type runtimeType = value.GetType();
                if (runtimeType == typeof(string))
                {
                    string result = (string)value;

                    // loss of precision
                    if (result.Length > 4000)
                    {
                        truncated = result.Length - 4000;
                        result = result.Substring(0, 4000);
                    }
                    return result;
                }
                else if (
                    runtimeType == typeof(sbyte) || runtimeType == typeof(short) || runtimeType == typeof(int) || runtimeType == typeof(byte) ||
                    runtimeType == typeof(ushort) || runtimeType == typeof(long) || runtimeType == typeof(uint) || runtimeType == typeof(ulong) ||
                    runtimeType == typeof(float) || runtimeType == typeof(double) || runtimeType == typeof(IPAddress))
                {
                    return value.ToString();
                }
                else if (runtimeType == typeof(bool))
                {
                    if ((bool)value)
                    {
                        return "1";
                    }
                    else
                    {
                        return "0";
                    }
                }
                else if (runtimeType == typeof(Guid))
                {
                    return ((Guid)value).ToString("D");
                }
                else if (runtimeType == typeof(byte[]))
                {
                    string result = TSharkTypeParser.ConstructHexBytes((byte[])value);

                    // loss of precision
                    if (result.Length > 8000)
                    {
                        truncated = result.Length - 8000;
                        result = result.Substring(0, 8000);
                    }
                    return result;
                }
                else if (runtimeType == typeof(DateTime))
                {
                    return ((DateTime)value).ToString("yyyy-MM-dd HH:mm:ss.FFFFFFF");
                }
                else if (runtimeType == typeof(TimeSpan))
                {
                    return ((TimeSpan)value).TotalSeconds.ToString();
                }
                else if (runtimeType == typeof(object[]))
                {
                    object[] valueArray = (object[])value;
                    StringBuilder builder = new StringBuilder("{ ");

                    truncated = 0;
                    for (int i = 0; i < valueArray.Length; i++)
                    {
                        if (i > 0)
                        {
                            builder.Append(", ");
                        }

                        int truncatedTemp;
                        builder.Append(ConvertToString(valueArray[i], out truncatedTemp));

                        truncated += truncatedTemp;
                    }
                    builder.Append(" }");

                    // loss of precision
                    if (builder.Length > 4000)
                    {
                        truncated += builder.Length - 4000;
                        builder.Length = 4000;
                    }

                    return builder.ToString();
                }
                else
                {
                    throw new Exception(string.Format("Unexpected column value type: {0}", runtimeType));
                }
            }
        }

        /// <summary>
        /// Encapsulates a table definition.
        /// </summary>
        protected class TableDefinition
        {

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="name">Table name.</param>
            public TableDefinition(string name)
            {
                this.Name = name;
                this.Columns = new Dictionary<string, ColumnDefinition>();
            }

            /// <summary>
            /// Gets the table name.
            /// </summary>
            public string Name { get; private set; }

            /// <summary>
            /// Gets the dictionary of named columns.
            /// </summary>
            public IDictionary<string, ColumnDefinition> Columns { get; private set; }

            /// <summary>
            /// Gets or sets a value indicating whether this table exists in the database.
            /// </summary>
            public bool Committed { get; set; }
        }

        /// <summary>
        /// Encapsulates a column definition.
        /// </summary>
        protected class ColumnDefinition
        {

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="name">Column name.</param>
            /// <param name="sqlType">Column type.</param>
            /// <param name="sqlPrecision">Column precision.</param>
            public ColumnDefinition(string name, ColumnType sqlType, int sqlPrecision)
            {
                this.Name = name;
                this.SqlType = sqlType;
                this.SqlPrecision = sqlPrecision;
            }

            /// <summary>
            /// Gets the column name.
            /// </summary>
            public string Name { get; private set; }

            /// <summary>
            /// Gets the column type.
            /// </summary>
            public ColumnType SqlType { get; set; }

            /// <summary>
            /// Gets the column precision.
            /// </summary>
            public int SqlPrecision { get; set; }

            /// <summary>
            /// Gets or sets a value indicating whether this table exists in the database.
            /// </summary>
            public bool Committed { get; set; }
        }

        /// <summary>
        /// Enumeration of supported database column types.
        /// </summary>
        protected enum ColumnType
        {

            /// <summary>
            /// Boolean ("BIT" on SQL Server).
            /// </summary>
            Boolean,

            /// <summary>
            /// Date and Time ("DATETIME2(7)" on SQL Server).
            /// </summary>
            DateTime,
            
            /// <summary>
            /// Big Integer ("NUMERIC(20,0)" on SQL Server).
            /// </summary>
            BigInteger,

            /// <summary>
            /// Float ("FLOAT" on SQL Server).
            /// </summary>
            Float,

            /// <summary>
            /// 32-bit Integer ("INT" on SQL Server).
            /// </summary>
            Integer32Bit,

            /// <summary>
            /// 64-bit Integer ("BIGINT" on SQL Server).
            /// </summary>
            Integer64Bit,

            /// <summary>
            /// String of 8-bit characters ("VARCHAR(p)" on SQL Server).
            /// </summary>
            /// <remarks>
            /// The precision value affects this type.
            /// </remarks>
            String8Bit,

            /// <summary>
            /// String of 16-bit characters ("NVARCHAR(p)" on SQL Server).
            /// </summary>
            /// <remarks>
            /// The precision value affects this type.
            /// </remarks>
            String16Bit,

            /// <summary>
            /// GUID ("UNIQUEIDENTIFIER" on SQL Server).
            /// </summary>
            Guid
        }

        /// <summary>
        /// Holds additional row information.
        /// </summary>
        private class RowInfo
        {

            /// <summary>
            /// Identifier of the source file.
            /// </summary>
            public long SourceFileID { get; set; }

            /// <summary>
            /// Packet number.
            /// </summary>
            public long Number { get; set; }

            /// <summary>
            /// Packet timestamp.
            /// </summary>
            public DateTime Timestamp { get; set; }
        }
    }
}
