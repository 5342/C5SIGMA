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

namespace Sigma.Compress
{

    /// <summary>
    /// Encapsulates command line arguments.
    /// </summary>
    public class Arguments : ArgumentsBase
    {

        /// <summary>
        /// Input file.
        /// </summary>
        [CommandLine("in", "inputpath", "Path to an input file.", true)]
        public string InputPath { get; set; }

        /// <summary>
        /// Output file.
        /// </summary>
        [CommandLine("out", "outputpath", "Path to an output file.", true)]
        public string OutputPath { get; set; }

        /// <summary>
        /// Force overwrite.
        /// </summary>
        [CommandLine("f", "force", "Force overwrite of existing output file.", true)]
        public bool Overwrite { get; set; }

        /// <summary>
        /// Initializes the data object.
        /// </summary>
        public override void Initialize()
        {
        }

        /// <summary>
        /// Validates arguments after parsing.
        /// </summary>
        public override void Validate()
        {
            if (Help)
                return;

            if (!File.Exists(InputPath))
            {
                throw new Exception(string.Format("File not found for option '--input': {0}", InputPath));
            }

            if (File.Exists(OutputPath) && !Overwrite)
            {
                throw new Exception(string.Format("File already exists for option '--output': {0}", OutputPath));
            }
        }
    }
}
