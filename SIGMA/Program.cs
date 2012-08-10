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
using System.Data.SqlClient;
using System.IO;
using System.Reflection;
using MySql.Data.MySqlClient;
using Sigma.Common;
using Sigma.Engine;
using Sigma.Engine.Database;

namespace Sigma
{

    /// <summary>
    /// Main application entry point.
    /// </summary>
    public class Program : ProgramBase<Arguments>
    {

        /// <summary>
        /// Main application entry point.
        /// </summary>
        /// <param name="args">Command line arguments.</param>
        /// <returns>Non-zero if an error occurs.</returns>
        public static int Main(string[] args)
        {
            return new Program().Run(args);
        }

        /// <summary>
        /// Called to perform the main body of work for the application after command line arguments have been parsed.
        /// </summary>
        /// <param name="commandLine">Command line arguments.</param>
        protected override void Run(Arguments commandLine)
        {
            Log.WriteInfo("Starting up.\nUTC time: {0}\nLocal time: {1}\nVersion: {2}", DateTime.UtcNow, DateTime.Now, Assembly.GetExecutingAssembly().GetName().Version);
            Log.WriteInfo("Input directory: {0}", commandLine.InputPath);
            Log.WriteInfo("Output directory: {0}", commandLine.OutputPath);
            Log.WriteInfo("TShark path: {0}", commandLine.TShark);
            Log.WriteInfo("TShark parameters: {0}", commandLine.TSharkParams);

            DataWriterType dataWriterType;
            string connectionString;
            if (commandLine.DBSqlServer)
            {
                SqlConnectionStringBuilder sqlStringBuilder = new SqlConnectionStringBuilder();
                sqlStringBuilder.DataSource = commandLine.DBHostname;
                sqlStringBuilder.IntegratedSecurity = commandLine.DBIntegrated;
                if (!commandLine.DBIntegrated)
                {
                    sqlStringBuilder.UserID = commandLine.DBUsername;
                    sqlStringBuilder.Password = "********";
                }
                sqlStringBuilder.InitialCatalog = commandLine.DBCatalog;

                Log.WriteInfo("Using SQL Server database: {0}", sqlStringBuilder.ToString());

                if (!commandLine.DBIntegrated)
                {
                    sqlStringBuilder.Password = commandLine.DBPassword;
                }

                connectionString = sqlStringBuilder.ToString();
                dataWriterType = DataWriterType.SqlServer;
            }
            else
            {
                MySqlConnectionStringBuilder sqlStringBuilder = new MySqlConnectionStringBuilder();
                sqlStringBuilder.Server = commandLine.DBHostname;
                sqlStringBuilder.IntegratedSecurity = commandLine.DBIntegrated;
                if (!commandLine.DBIntegrated)
                {
                    sqlStringBuilder.UserID = commandLine.DBUsername;
                    sqlStringBuilder.Password = "********";
                }
                sqlStringBuilder.Database = commandLine.DBCatalog;

                Log.WriteInfo("Using MySQL database: {0}", sqlStringBuilder.ToString());

                if (!commandLine.DBIntegrated)
                {
                    sqlStringBuilder.Password = commandLine.DBPassword;
                }

                connectionString = sqlStringBuilder.ToString();
                dataWriterType = DataWriterType.MySql;
            }

            DataPump dataPump = new DataPump();
            dataPump.TSharkPath = Path.GetFullPath(commandLine.TShark);
            dataPump.TSharkParams = commandLine.TSharkParams;
            dataPump.InputPath = Path.GetFullPath(commandLine.InputPath);
            dataPump.OutputPath = Path.GetFullPath(commandLine.OutputPath);
            dataPump.ConnectionString = connectionString;
            dataPump.DataWriterType = dataWriterType;
            dataPump.DisableForeignKeys = commandLine.DBForeignKeys;

            if (!string.IsNullOrEmpty(commandLine.FixupsPath))
            {
                dataPump.FixupsPath = Path.GetFullPath(commandLine.FixupsPath);
            }

            if (!string.IsNullOrEmpty(commandLine.DataFilterPath))
            {
                dataPump.DataFilterPath = Path.GetFullPath(commandLine.DataFilterPath);
            }

            dataPump.DataFilterPreset = commandLine.DataFilterPreset;

            dataPump.Run();
        }
    }
}
