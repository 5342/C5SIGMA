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
using System.Threading;
using Sigma.Common.IO;

namespace Sigma.Common
{

    /// <summary>
    /// Base class for main application entry points.
    /// </summary>
    /// <remarks>
    /// Derived classes should implement a Main method that calls the Run method exposed by this class.
    /// </remarks>
    public abstract class ProgramBase<T> where T : ArgumentsBase, new()
    {

        /// <summary>
        /// Called to perform the main body of work for the application after command line arguments have been parsed.
        /// </summary>
        /// <param name="commandLine">Command line arguments.</param>
        protected abstract void Run(T commandLine);

        /// <summary>
        /// Runs this program.
        /// </summary>
        /// <param name="args">Command line arguments.</param>
        /// <returns>Non-zero if an error occurs.</returns>
        protected int Run(string[] args)
        {
            try
            {
                ConsoleWriter.PrintLine("+----------------------------------------------------------------------+");
                ConsoleWriter.PrintLine("| C5 SIGMA. Copyright (C) Command Five Pty Ltd 2011.                   |");
                ConsoleWriter.PrintLine("| <http://www.commandfive.com/>                                        |");
                ConsoleWriter.PrintLine("|                                                                      |");
                ConsoleWriter.PrintLine("| C5 SIGMA is free software: you can redistribute it and/or modify     |");
                ConsoleWriter.PrintLine("| it under the terms of the GNU General Public License as published by |");
                ConsoleWriter.PrintLine("| the Free Software Foundation, either version 3 of the License, or    |");
                ConsoleWriter.PrintLine("| (at your option) any later version.                                  |");
                ConsoleWriter.PrintLine("|                                                                      |");
                ConsoleWriter.PrintLine("| C5 SIGMA is distributed in the hope that it will be useful,          |");
                ConsoleWriter.PrintLine("| but WITHOUT ANY WARRANTY; without even the implied warranty of       |");
                ConsoleWriter.PrintLine("| MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the         |");
                ConsoleWriter.PrintLine("| GNU General Public License for more details.                         |");
                ConsoleWriter.PrintLine("|                                                                      |");
                ConsoleWriter.PrintLine("| You should have received a copy of the GNU General Public License    |");
                ConsoleWriter.PrintLine("| along with C5 SIGMA. If not, see <http://www.gnu.org/licenses/>.     |");
                ConsoleWriter.PrintLine("+----------------------------------------------------------------------+");
                ConsoleWriter.PrintLine();

                Thread.Sleep(1000);

                T commandLine = ParseCommandLine(args);
                if (commandLine == null)
                {
#if DEBUG
                    try
                    {
                        ConsoleWriter.PrintLine("Press any key to continue.");
                        Console.ReadKey();
                    }
                    catch
                    {
                    }
#endif
                    return 1;
                }

                Run(commandLine);
            }
            catch (Exception ex)
            {
                Log.WriteError(ex.Message);
                return 1;
            }

            Log.WriteInfo("Shutting down.\nWarnings: {0}\nErrors: {1}", Log.WarningCount, Log.ErrorCount);

            ConsoleWriter.PrintLine();

#if DEBUG
            try
            {
                ConsoleWriter.PrintLine("Press any key to continue.");
                Console.ReadKey();
            }
            catch
            {
            }
#endif

            return 0;
        }

        /// <summary>
        /// Parses the command line.
        /// </summary>
        /// <param name="args">Raw command line arguments.</param>
        /// <returns>Parsed command line arguments.</returns>
        private T ParseCommandLine(string[] args)
        {
            T commandLine = new T();

            bool showHelp;
            if (args == null || args.Length == 0)
            {
                showHelp = true;
            }
            else
            {
                CommandLineParser.Parse(commandLine, args);
                showHelp = commandLine.Help;
            }

            if (showHelp)
            {
                Console.WriteLine(CommandLineParser.HelpText(commandLine, "SIGMA.exe"));
                return default(T);
            }

            return commandLine;
        }
    }
}
