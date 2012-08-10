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

using System.Diagnostics;
using System.IO;
using System.Text;
using Sigma.Common;

namespace Sigma.Engine.TShark
{

    /// <summary>
    /// Provides utility methods to invoke the 'tshark.exe' executable.
    /// </summary>
    internal static class TSharkInvoker
    {

        /// <summary>
        /// Invokes the 'tshark.exe' executable.
        /// </summary>
        /// <param name="tsharkPath">Path to 'tshark.exe' including the file name.</param>
        /// <param name="parameters">Parameters to pass to the external program.</param>
        /// <param name="outputFilePath">Path to a file into which standard output should be placed, or null to discard.</param>
        public static void Invoke(string tsharkPath, string parameters, string outputFilePath)
        {
            Log.WriteInfo("Executing TShark with parameters: {0}", parameters);

            ProcessStartInfo startInfo = new ProcessStartInfo(tsharkPath, parameters);
            startInfo.CreateNoWindow = true;
            startInfo.RedirectStandardOutput = (outputFilePath != null);
            startInfo.UseShellExecute = false;

            Process tshark = Process.Start(startInfo);
            if (outputFilePath != null)
            {
                int lineCount = 0;

                StreamReader reader = tshark.StandardOutput;
                using (StreamWriter writer = new StreamWriter(outputFilePath, false, Encoding.UTF8))
                {
                    while (true)
                    {
                        string line = reader.ReadLine();
                        if (line == null)
                            break;

                        writer.WriteLine(line);
                        lineCount++;
                    }
                }

                Log.WriteInfo("TShark output lines captured: {0}", lineCount);
            }

            while (true)
            {
                tshark.Refresh();
                if (tshark.HasExited || tshark.WaitForExit(500))
                {
                    break;
                }
            }
        }
    }
}
