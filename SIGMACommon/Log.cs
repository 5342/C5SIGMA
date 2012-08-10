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

using Sigma.Common.IO;

namespace Sigma.Common
{

    /// <summary>
    /// Provides logging utility methods.
    /// </summary>
    public static class Log
    {

        /// <summary>
        /// Writes a debug log message.
        /// </summary>
        /// <param name="format">Message format.</param>
        /// <param name="args">Message arguments.</param>
        public static void WriteDebug(string format, params object[] args)
        {
            Write(LogLevel.Debug, format, args);
        }

        /// <summary>
        /// Writes an informational log message.
        /// </summary>
        /// <param name="format">Message format.</param>
        /// <param name="args">Message arguments.</param>
        public static void WriteInfo(string format, params object[] args)
        {
            Write(LogLevel.Info, format, args);
        }

        /// <summary>
        /// Writes a warning log message.
        /// </summary>
        /// <param name="format">Message format.</param>
        /// <param name="args">Message arguments.</param>
        public static void WriteWarning(string format, params object[] args)
        {
            Write(LogLevel.Warning, format, args);
        }

        /// <summary>
        /// Writes an error log message.
        /// </summary>
        /// <param name="format">Message format.</param>
        /// <param name="args">Message arguments.</param>
        public static void WriteError(string format, params object[] args)
        {
            Write(LogLevel.Error, format, args);
        }

        /// <summary>
        /// Writes a log message.
        /// </summary>
        /// <param name="level">Log level.</param>
        /// <param name="format">Message format.</param>
        /// <param name="args">Message arguments.</param>
        public static void Write(LogLevel level, string format, params object[] args)
        {
            lock (syncRoot)
            {
                string prefix;
                switch (level)
                {
                    case LogLevel.Debug:
                        prefix = "DEBUG";
                        DebugCount++;
                        break;
                    default:
                    case LogLevel.Info:
                        prefix = "INFO";
                        InfoCount++;
                        break;
                    case LogLevel.Warning:
                        prefix = "WARNING";
                        WarningCount++;
                        break;
                    case LogLevel.Error:
                        prefix = "ERROR";
                        ErrorCount++;
                        break;
                }

                ConsoleWriter.Print("[{0}] - ", prefix);
                ConsoleWriter.TabIn(); ConsoleWriter.PrintLine(format, args); ConsoleWriter.TabOut();
            }
        }

        /// <summary>
        /// Resets the message counters.
        /// </summary>
        public static void ResetCounters()
        {
            lock (syncRoot)
            {
                DebugCount = 0;
                InfoCount = 0;
                WarningCount = 0;
                ErrorCount = 0;
            }
        }

        /// <summary>
        /// Synchronization root object.
        /// </summary>
        private static object syncRoot = new object();

        /// <summary>
        /// Gets the Debug message counter.
        /// </summary>
        public static int DebugCount { get; private set; }

        /// <summary>
        /// Gets the Info message counter.
        /// </summary>
        public static int InfoCount { get; private set; }

        /// <summary>
        /// Gets the Warning message counter.
        /// </summary>
        public static int WarningCount { get; private set; }

        /// <summary>
        /// Gets the Error message counter.
        /// </summary>
        public static int ErrorCount { get; private set; }
    }

    /// <summary>
    /// Enumeration of support log levels.
    /// </summary>
    public enum LogLevel
    {

        /// <summary>
        /// Debug log level.
        /// </summary>
        Debug,

        /// <summary>
        /// Informational log level.
        /// </summary>
        Info,

        /// <summary>
        /// Warning log level.
        /// </summary>
        Warning,

        /// <summary>
        /// Error log level.
        /// </summary>
        Error
    }
}
