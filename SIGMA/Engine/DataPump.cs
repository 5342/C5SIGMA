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

using System.IO;
using Sigma.Common;
using Sigma.Common.Support;
using Sigma.Engine.Database;
using Sigma.Engine.TShark;
using System;

namespace Sigma.Engine
{

    /// <summary>
    /// Pumps data from raw input files, into TShark, and then into the database.
    /// </summary>
    internal class DataPump
    {

        /// <summary>
        /// Constructor.
        /// </summary>
        public DataPump()
        {
        }

        /// <summary>
        /// Gets or sets the path to the input directory.
        /// </summary>
        public string InputPath { get; set; }

        /// <summary>
        /// Gets or sets the path to the output directory.
        /// </summary>
        public string OutputPath { get; set; }

        /// <summary>
        /// Gets or sets the database connection string.
        /// </summary>
        public string ConnectionString { get; set; }

        /// <summary>
        /// Gets or sets the path to the TShark executable.
        /// </summary>
        public string TSharkPath { get; set; }

        /// <summary>
        /// Gets or sets any additional parameters to pass to TShark.
        /// </summary>
        public string TSharkParams { get; set; }

        /// <summary>
        /// Gets or sets the path to a file containing XML formatted fixups to use.
        /// </summary>
        public string FixupsPath { get; set; }

        /// <summary>
        /// Gets or sets the path to an XML formatted data filter file to use.
        /// </summary>
        public string DataFilterPath { get; set; }

        /// <summary>
        /// Gets or sets the name of a preset (built-in) data filter to use.
        /// </summary>
        public string DataFilterPreset { get; set; }

        /// <summary>
        /// Gets or sets the type of data writer to be used.
        /// </summary>
        public DataWriterType DataWriterType { get; set; }

        /// <summary>
        /// Gets or sets whether the generation of foreign keys in the database schema is disabled.
        /// </summary>
        public bool DisableForeignKeys { get; set; }

        /// <summary>
        /// Runs the data pump.
        /// </summary>
        public void Run()
        {
            string protocolsFile = Path.Combine(OutputPath, "TShark.protocols");
            string fieldsFile = Path.Combine(OutputPath, "TShark.fields");
            string valuesFile = Path.Combine(OutputPath, "TShark.values");
            string decodesFile = Path.Combine(OutputPath, "TShark.decodes");

            Log.WriteInfo("Generated schema file paths.\nProtocols: {0}\nFields: {1}\nValues: {2}\nDecodes: {3}", protocolsFile, fieldsFile, valuesFile, decodesFile);

            Log.WriteInfo("Writing protocols file.");
            TSharkInvoker.Invoke(TSharkPath, string.Format("-G protocols {0}", TSharkParams), protocolsFile);

            Log.WriteInfo("Writing fields file.");
            TSharkInvoker.Invoke(TSharkPath, string.Format("-G fields3 {0}", TSharkParams), fieldsFile);

            Log.WriteInfo("Writing values file.");
            TSharkInvoker.Invoke(TSharkPath, string.Format("-G values {0}", TSharkParams), valuesFile);

            Log.WriteInfo("Writing decodes file.");
            TSharkInvoker.Invoke(TSharkPath, string.Format("-G decodes {0}", TSharkParams), decodesFile);

            TSharkDataSchema schema;

            Log.WriteInfo("Reading TShark schema.");
            schema = TSharkSchemaReader.Read(protocolsFile, fieldsFile, valuesFile, decodesFile);

            Log.WriteInfo("Installing fixups.");
            TSharkFixups fixups = new TSharkFixups();
            if (!string.IsNullOrEmpty(FixupsPath))
            {
                fixups.LoadExternalFixups(FixupsPath);
            }

            using (DataWriter writer = CreateDataWriter())
            {
                DataFilter filter = new DataFilter(DataFilterPreset);
                if (!string.IsNullOrEmpty(DataFilterPath))
                {
                    filter.LoadExternalFilter(DataFilterPath);
                }
                writer.Filter = filter;

                TSharkDataReaderCallback callback = writer.WriteRow;

                Log.WriteInfo("Starting data pump.");
                foreach (string file in FileSystemSupport.RecursiveScan(InputPath, null))
                {
                    Log.WriteInfo("Processing: {0}", file);

                    string dataFile = GenerateOutputFile(file);
                    TSharkInvoker.Invoke(TSharkPath, string.Format("-r \"{0}\" -n -T pdml -V {1}", file.Replace("\"", "\\\""), TSharkParams), dataFile);

                    Log.WriteInfo("Reading TShark data file: {0}", dataFile);
                    TSharkDataReader.Read(file, schema, dataFile, callback, fixups);
                }
            }
        }

        /// <summary>
        /// Creates the DataWriter to be used by this instance.
        /// </summary>
        /// <returns>A DataWriter instance.</returns>
        private DataWriter CreateDataWriter()
        {
            switch (DataWriterType)
            {
                case DataWriterType.SqlServer:
                    return new SqlServerDataWriter(ConnectionString, DisableForeignKeys);
                case DataWriterType.MySql:
                    return new MySqlDataWriter(ConnectionString, DisableForeignKeys);
                default:
                    throw new Exception(string.Format("Unsupported data writer type: {0}", DataWriterType));
            }
        }

        /// <summary>
        /// Generates an output file name for the given input file.
        /// </summary>
        /// <param name="inputFile">Input file path.</param>
        /// <returns>The output file name.</returns>
        private string GenerateOutputFile(string inputFile)
        {
            string filename = Path.GetFileName(inputFile);
            string outputFile = Path.Combine(OutputPath, string.Concat(filename, ".data"));

            Log.WriteInfo("Generated output file path.\nInput file: {0}\nOutput file: {1}", inputFile, outputFile);

            return outputFile;
        }
    }
}
