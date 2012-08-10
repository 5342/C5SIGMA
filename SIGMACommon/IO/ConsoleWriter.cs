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

namespace Sigma.Common.IO
{

    /// <summary>
    /// Provides application Console output methods.
    /// </summary>
    public static class ConsoleWriter
    {

        /// <summary>
        /// True if window operations are disabled, false otherwise.
        /// </summary>
        /// <remarks>
        /// This field is set when the current application doesn't have a console window.
        /// </remarks>
        private static volatile bool disableWindowOperations;

        /// <summary>
        /// Prints text to the console.
        /// </summary>
        /// <param name="text">Text to print to the console.</param>
        public static void Print(string text)
        {
            if (text == null)
                text = string.Empty;

            if (disableWindowOperations)
            {
                Console.Write(text);
            }
            else
            {
                try
                {
                    int width = Console.WindowWidth;
                    int leftPadding = Indent;
                    int maxWidth = width - 2;

                    int startOffset = 0;
                    while (startOffset < text.Length)
                    {
                        int left = Console.CursorLeft;

                        if (left < leftPadding)
                        {
                            Console.Write(new string(' ', leftPadding - left));
                            left = leftPadding;
                        }

                        string line;
                        bool newLine;

                        int endOffset = text.IndexOf('\n', startOffset);
                        if (endOffset < 0)
                        {
                            line = text.Substring(startOffset);

                            endOffset = text.Length;
                            newLine = false;
                        }
                        else
                        {
                            if (endOffset > 0 && text[endOffset - 1] == '\r')
                            {
                                line = text.Substring(startOffset, endOffset - startOffset - 1);
                            }
                            else
                            {
                                line = text.Substring(startOffset, endOffset - startOffset);
                            }

                            newLine = true;
                        }

                        startOffset = endOffset + 1;

                        int splitOffset = 0;
                        while (splitOffset < line.Length)
                        {
                            int maxLineWidth = maxWidth - left;
                            if (line.Length - splitOffset > maxLineWidth)
                            {
                                Console.WriteLine(line.Substring(splitOffset, maxLineWidth));
                                Console.Write(new string(' ', leftPadding));
                                splitOffset += maxLineWidth;
                            }
                            else
                            {
                                Console.Write(line.Substring(splitOffset));
                                break;
                            }
                        }

                        if (newLine)
                        {
                            Console.Write(Environment.NewLine);
                        }
                    }
                }
                catch
                {
                    disableWindowOperations = true;
                    Console.Write(text);
                }
            }
        }

        /// <summary>
        /// Prints text to the console.
        /// </summary>
        /// <param name="format">Format of the text to print (passed to string.Format).</param>
        /// <param name="args">Format arguments (passed to string.Format).</param>
        public static void Print(string format, params object[] args)
        {
            if (format == null)
                format = string.Empty;

            if (args == null || args.Length == 0)
            {
                Print(format);
            }
            else
            {
                Print(string.Format(format, args));
            }
        }

        /// <summary>
        /// Prints a blank line to the console.
        /// </summary>
        public static void PrintLine()
        {
            Print(Environment.NewLine);
        }

        /// <summary>
        /// Prints a line of text to the console.
        /// </summary>
        /// <param name="text">Text to print to the console.</param>
        public static void PrintLine(string text)
        {
            if (text == null)
                text = string.Empty;

            Print(text);
            Print(Environment.NewLine);
        }

        /// <summary>
        /// Prints a line of text to the console.
        /// </summary>
        /// <param name="format">Format of the text to print (passed to string.Format).</param>
        /// <param name="args">Format arguments (passed to string.Format).</param>
        public static void PrintLine(string format, params object[] args)
        {
            if (format == null)
                format = string.Empty;

            if (args == null || args.Length == 0)
            {
                PrintLine(format);
            }
            else
            {
                PrintLine(string.Format(format, args));
            }
        }

        /// <summary>
        /// Gets or sets the current indent level.
        /// </summary>
        public static int Indent { get; set; }

        /// <summary>
        /// Increases the current indent to the next multiple of 4.
        /// </summary>
        public static void TabIn()
        {
            try
            {
                if (Indent < Console.WindowWidth - 4)
                {
                    Indent += 4;
                    Indent -= Indent % 4;
                }
            }
            catch
            {
                disableWindowOperations = true;
            }
        }

        /// <summary>
        /// Decreases the current indent to the previous multiple of 4.
        /// </summary>
        public static void TabOut()
        {
            if (Indent > 4)
            {
                Indent -= 4;
            }
            else
            {
                Indent = 0;
            }
            Indent -= Indent % 4;
        }
    }
}
