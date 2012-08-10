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
using System.Text;
using Sigma.Common;

namespace Sigma.Engine.TShark
{

    /// <summary>
    /// Parses TShark formatted schema files into an internal data structure.
    /// </summary>
    internal class TSharkSchemaReader
    {

        /// <summary>
        /// Reads TShark formatted schema files.
        /// </summary>
        /// <param name="protocolsFile">Protocols file.</param>
        /// <param name="fieldsFile">Fields file.</param>
        /// <param name="valuesFile">Values file.</param>
        /// <param name="decodesFile">Decodes file.</param>
        /// <returns>Data structure containing the schema that was read.</returns>
        public static TSharkDataSchema Read(string protocolsFile, string fieldsFile, string valuesFile, string decodesFile)
        {
            TSharkDataSchema schema = new TSharkDataSchema();

            int lineIndex = 0;

            // read protocols
            using (StreamReader reader = new StreamReader(protocolsFile, Encoding.UTF8))
            {
                while (true)
                {
                    try
                    {
                        string line = reader.ReadLine();
                        if (line == null)
                            break;

                        lineIndex++;

                        if (line.Length == 0)
                            continue;

                        string[] parts = SplitParts(line, 3); 
                        if (parts.Length != 3)
                            throw new Exception(string.Format("Badly formatted protocols file. Line: {0}", lineIndex));

                        schema.AddProtocol(parts[0], parts[1], parts[2]);
                    }
                    catch (Exception ex)
                    {
                        Log.WriteWarning(ex.Message);
                    }
                }
            }

            // read fields
            using (StreamReader reader = new StreamReader(fieldsFile, Encoding.UTF8))
            {
                while (true)
                {
                    try
                    {
                        string line = reader.ReadLine();
                        if (line == null)
                            break;

                        lineIndex++;

                        if (line.Length == 0)
                            continue;

                        string[] parts;
                        switch (line[0])
                        {
                            case 'F':
                                parts = SplitParts(line, 8); 
                                if (parts.Length != 8)
                                    throw new Exception(string.Format("Badly formatted fields file. Line: {0}", lineIndex));
                                schema.AddField(parts[1], parts[2], parts[3], parts[4], parts[5], parts[6], parts[7]);
                                break;
                            case 'P':
                                parts = SplitParts(line, 3); 
                                if (parts.Length != 3)
                                    throw new Exception(string.Format("Badly formatted fields file. Line: {0}", lineIndex));
                                if (!schema.Protocols.ContainsKey(parts[2].ToLowerInvariant()))
                                {
                                    schema.AddProtocol(parts[1], parts[2], parts[2].ToLowerInvariant());
                                }
                                break;
                        }
                    }
                    catch (Exception ex)
                    {
                        Log.WriteWarning(ex.Message);
                    }
                }
            }

            // read values
            using (StreamReader reader = new StreamReader(valuesFile, Encoding.UTF8))
            {
                while (true)
                {
                    try
                    {
                        string line = reader.ReadLine();
                        if (line == null)
                            break;

                        lineIndex++;

                        if (line.Length == 0)
                            continue;

                        string[] parts;
                        switch (line[0])
                        {
                            case 'V':
                                parts = SplitParts(line, 4); 
                                long value;
                                if (parts.Length != 4 || !TSharkTypeParser.TryParseInt64(parts[2], out value))
                                    throw new Exception(string.Format("Badly formatted values file. Line: {0}", lineIndex));
                                schema.AddValueString(parts[1], value, parts[3]);
                                break;
                            case 'R':
                                parts = SplitParts(line, 5); 
                                long lowerBound, upperBound;
                                if (parts.Length != 5 || !TSharkTypeParser.TryParseInt64(parts[2], out lowerBound) || !TSharkTypeParser.TryParseInt64(parts[3], out upperBound))
                                    throw new Exception(string.Format("Badly formatted values file. Line: {0}", lineIndex));
                                schema.AddRangeString(parts[1], lowerBound, upperBound, parts[4]);
                                break;
                            case 'T':
                                parts = SplitParts(line, 4); 
                                if (parts.Length != 4)
                                    throw new Exception(string.Format("Badly formatted values file. Line: {0}", lineIndex));
                                schema.AddTrueFalseString(parts[1], parts[2], parts[3]);
                                break;
                        }
                    }
                    catch (Exception ex)
                    {
                        Log.WriteWarning(ex.Message);
                    }
                }
            }

            return schema;
        }

        /// <summary>
        /// Splits a line into the maximum number of parts specified using '\t' as a separator character.
        /// The final part may contain '\t' characters that were not consumed during the split process.
        /// </summary>
        /// <remarks>
        /// Example: The string "a\tb\tc\td" splits into "a", "b", "c\td" when maxCount is 3.
        /// </remarks>
        /// <param name="line">The line to split.</param>
        /// <param name="maxCount">The maximum number of parts to split the line into.</param>
        /// <returns>The parts of the line.</returns>
        private static string[] SplitParts(string line, int maxCount)
        {
            string[] parts = new string[maxCount];

            int startOffset = 0;
            for (int i = 0; i < maxCount && startOffset < line.Length; i++)
            {
                int endOffset;
                if (i == maxCount - 1 || (endOffset = line.IndexOf('\t', startOffset)) < 0)
                {
                    parts[i] = line.Substring(startOffset);
                    while (++i < maxCount)
                    {
                        parts[i] = string.Empty;
                    }
                    startOffset = line.Length;
                }
                else
                {
                    parts[i] = line.Substring(startOffset, endOffset - startOffset);
                    startOffset = endOffset + 1;
                }
            }

            return parts;
        }
    }
}
