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
using System.Reflection;
using Sigma.Common;
using Sigma.Common.Support;

namespace Sigma.Compress
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
            Log.WriteInfo("Input file: {0}", commandLine.InputPath);
            Log.WriteInfo("Output file: {0}", commandLine.OutputPath);

            CompressionSupport.CompressFile(commandLine.InputPath, commandLine.OutputPath);
        }
    }
}
