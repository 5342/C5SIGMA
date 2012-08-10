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

namespace Sigma.SourceScan
{

    /// <summary>
    /// Encapsulates command line arguments.
    /// </summary>
    public class Arguments : ArgumentsBase
    {

        /// <summary>
        /// Input directory.
        /// </summary>
        [CommandLine("in", "inputpath", "Path to a directory containing the Wireshark source code.", true)]
        public string InputPath { get; set; }

        /// <summary>
        /// Output directory.
        /// </summary>
        [CommandLine("out", "outputpath", "Path to a directory that will receive fixup XML files.", true)]
        public string OutputPath { get; set; }

        /// <summary>
        /// Initializes the data object.
        /// </summary>
        public override void Initialize()
        {
            InputPath = ".\\src\\epan\\dissectors";
            OutputPath = ".\\";
        }

        /// <summary>
        /// Validates arguments after parsing.
        /// </summary>
        public override void Validate()
        {
            if (Help)
                return;

            if (!Directory.Exists(InputPath))
            {
                throw new Exception(string.Format("Directory not found for option '--input': {0}", InputPath));
            }

            if (!Directory.Exists(OutputPath))
            {
                throw new Exception(string.Format("Directory not found for option '--output': {0}", OutputPath));
            }
        }
    }
}
