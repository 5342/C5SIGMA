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
using System.IO;
using Sigma.Common;
using Sigma.Common.IO;

namespace Sigma
{

    /// <summary>
    /// Encapsulates command line arguments.
    /// </summary>
    public class Arguments : ArgumentsBase
    {

        /// <summary>
        /// Input directory.
        /// </summary>
        [CommandLine("in", "inputpath", "Path to a directory containing network capture files.")]
        public string InputPath { get; set; }

        /// <summary>
        /// Output directory.
        /// </summary>
        [CommandLine("out", "outputpath", "Path to a directory that will receive processed data files.")]
        public string OutputPath { get; set; }

        /// <summary>
        /// Database hostname.
        /// </summary>
        [CommandLine("dbh", "dbhostname", "Database hostname.", true)]
        public string DBHostname { get; set; }

        /// <summary>
        /// Database schema name.
        /// </summary>
        [CommandLine("dbc", "dbcatalog", "Database catalog/schema name.")]
        public string DBCatalog { get; set; }

        /// <summary>
        /// Database authentication type.
        /// </summary>
        [CommandLine("dbi", "dbintegrated", "Use integrated Windows authentication with the database.")]
        public bool DBIntegrated { get; set; }

        /// <summary>
        /// Use MySQL database flag.
        /// </summary>
        [CommandLine("dbmy", "dbmysql", "Use MySQL database.")]
        public bool DBMySql { get; set; }

        /// <summary>
        /// Use SQL Server database flag (default).
        /// </summary>
        [CommandLine("dbms", "dbsqlserver", "Use SQL Server database. Default.")]
        public bool DBSqlServer { get; set; }

        /// <summary>
        /// Database username.
        /// </summary>
        [CommandLine("dbu", "dbusername", "Database username. Not valid with '--dbintegrated'.")]
        public string DBUsername { get; set; }

        /// <summary>
        /// Database password.
        /// </summary>
        [CommandLine("dbp", "dbpassword", "Database password. Not valid with '--dbintegrated'.")]
        public string DBPassword { get; set; }

        /// <summary>
        /// Disable generation of foreign keys in the database.
        /// </summary>
        [CommandLine("dbfk", "dbforeignkeys", "Disable generation of foreign keys in the database.")]
        public bool DBForeignKeys { get; set; }

        /// <summary>
        /// Path to the TShark executable file.
        /// </summary>
        [CommandLine("ts", "tshark", "Path to 'tshark.exe' (including the filename).")]
        public string TShark { get; set; }

        /// <summary>
        /// Additional parameters to pass to the TShark executable.
        /// </summary>
        [CommandLine("tsp", "tsharkparams", "Additional parameters to pass to 'tshark.exe'.")]
        public string TSharkParams { get; set; }

        /// <summary>
        /// Path to an XML formatted fixups file.
        /// </summary>
        [CommandLine("fix", "fixups", "Path to an XML formatted fixups file.")]
        public string FixupsPath { get; set; }

        /// <summary>
        /// Path to an XML formatted data filter file.
        /// </summary>
        [CommandLine("fil", "datafilter", "Path to an XML formatted data filter file.")]
        public string DataFilterPath { get; set; }

        /// <summary>
        /// Name of a preset (built-in) data filter.
        /// </summary>
        [CommandLine("pre", "datafilterpreset", "Name of a preset (built-in) data filter. Presets: Basic.")]
        public string DataFilterPreset { get; set; }

        /// <summary>
        /// Initializes the data object.
        /// </summary>
        public override void Initialize()
        {
            DBHostname = "localhost";
            DBCatalog = "SIGMA";
            TShark = "C:\\Program Files\\Wireshark\\tshark.exe";
            TSharkParams = string.Empty;
            InputPath = ".\\Input";
            OutputPath = ".\\Output";
            FixupsPath = null;
            DataFilterPath = null;
            DataFilterPreset = null;
            DBSqlServer = true;
        }

        /// <summary>
        /// Validates arguments after parsing.
        /// </summary>
        public override void Validate()
        {
            if (Help)
                return;

            if (DBMySql)
            {
                DBSqlServer = false;
            }

            if (DBIntegrated)
            {
                if (!string.IsNullOrEmpty(DBUsername))
                    throw new Exception("The option '--dbusername' is not valid with '--dbintegrated'.");
                if (!string.IsNullOrEmpty(DBPassword))
                    throw new Exception("The option '--dbpassword' is not valid with '--dbintegrated'.");
            }
            else
            {
                if (string.IsNullOrEmpty(DBUsername))
                    throw new Exception("The option '--dbusername' is required unless '--dbintegrated' is specified.");
                if (string.IsNullOrEmpty(DBPassword))
                    throw new Exception("The option '--dbpassword' is required unless '--dbintegrated' is specified.");
            }

            if (!File.Exists(TShark))
            {
                throw new Exception(string.Format("File not found for option '--tshark': {0}", TShark));
            }
            
            if (!Directory.Exists(InputPath))
            {
                throw new Exception(string.Format("Directory not found for option '--input': {0}", InputPath));
            }

            if (!Directory.Exists(OutputPath))
            {
                throw new Exception(string.Format("Directory not found for option '--output': {0}", OutputPath));
            }

            if (!string.IsNullOrEmpty(FixupsPath) && !File.Exists(FixupsPath))
            {
                throw new Exception(string.Format("File not found for option '--fixups': {0}", FixupsPath));
            }
        }
    }
}
