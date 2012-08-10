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
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using Sigma.Common;
using Sigma.Common.Support;

namespace Sigma.SourceScan
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
            Log.WriteInfo("Input directory: {0}", commandLine.InputPath);
            Log.WriteInfo("Output directory: {0}", commandLine.OutputPath);
            
            string fixupsFilePath = Path.Combine(commandLine.OutputPath, "Fixups.xml");
            string traceFilePath = Path.Combine(commandLine.OutputPath, "Fixups.trace");

            using (StreamWriter fixups = new StreamWriter(fixupsFilePath, false, Encoding.UTF8))
            using (StreamWriter trace = new StreamWriter(traceFilePath, false, Encoding.UTF8))
            {
                fixups.WriteLine("<?xml version=\"1.0\" encoding=\"utf-8\" ?>");
                fixups.WriteLine("<fixups>");

                HashSet<string> constantFixups = new HashSet<string>();
                HashSet<string> prefixFixups = new HashSet<string>();
                HashSet<string> speculativeFixups = new HashSet<string>();

                string fullPath = Path.GetFullPath(commandLine.InputPath);

                string dissectorPath = Path.Combine(fullPath, @"epan\dissectors");
                string pluginPath = Path.Combine(fullPath, @"plugins");

                if (!Directory.Exists(dissectorPath))
                    throw new Exception(string.Format("Directory not found: {0}", dissectorPath));
                if (!Directory.Exists(pluginPath))
                    throw new Exception(string.Format("Directory not found: {0}", pluginPath));

                List<string> paths = new List<string>();
                paths.Add(dissectorPath);
                paths.Add(pluginPath);

                foreach (string path in paths)
                {
                    foreach (string file in FileSystemSupport.RecursiveScan(path, "*.c"))
                    {
                        ProcessSourceFile(file, fixups, trace, constantFixups, prefixFixups, speculativeFixups);
                    }
                }
                
                fixups.WriteLine("</fixups>");
            }
        }

        /// <summary>
        /// Escapes an XML attribute value that will be enclosed in double quotes.
        /// </summary>
        /// <param name="value">Value to escape.</param>
        /// <returns>Escaped value.</returns>
        private static string XmlAttributeValueEscape(string value)
        {
            // this is obviously not a complete implementation and should not be copied for use elsewhere
            StringBuilder builder = new StringBuilder(value);
            builder.Replace("&", "&amp;");
            builder.Replace("\"", "&quot;");
            builder.Replace("<", "&lt;");
            builder.Replace("<", "&gt;");
            return builder.ToString();
        }

        /// <summary>
        /// Escapes an XML comment that will be enclosed in comment tags.
        /// </summary>
        /// <param name="value">Value to escape.</param>
        /// <returns>Escaped value.</returns>
        private static string XmlCommentEscape(string value)
        {
            // this is obviously not a complete implementation and should not be copied for use elsewhere
            StringBuilder builder = new StringBuilder(value);
            builder.Replace("&", "&amp;");
            builder.Replace("<", "&lt;");
            builder.Replace("<", "&gt;");
            return builder.ToString();
        }

        /// <summary>
        /// Escapes a literal string for insertion into a regular expression.
        /// </summary>
        /// <param name="value">Value to escape.</param>
        /// <returns>Escaped value.</returns>
        private static string RegexEscape(string value)
        {
            StringBuilder builder = new StringBuilder(value.Length);
            for (int i = 0; i < value.Length; i++)
            {
                char c = value[i];
                if (!char.IsLetterOrDigit(c) && c != ' ')
                {
                    if (c < 256)
                    {
                        builder.AppendFormat("\\x{0:X2}", (int)c);
                    }
                    else
                    {
                        builder.AppendFormat("\\u{0:X4}", (int)c);
                    }
                }
                else
                {
                    builder.Append(c);
                }
            }
            return builder.ToString();
        }

        /// <summary>
        /// Regex used to identify calls to proto_tree_add_text.
        /// </summary>
        private static readonly Regex ProtoTreeAddTextRegex = new Regex(@"proto_tree_add_text\s*\(\s*[^,]+,\s*[^,]+,\s*[^,]+,\s*[^,]+,\s*""(?<text>(\\""|[^""])*)""\s*,");

        /// <summary>
        /// Regex used to capture #defined strings.
        /// </summary>
        private static readonly Regex HashDefineRegex = new Regex(@"(#define\s*(?<key>[\w_]+)\s*(?<value>""(\\""|[^""])+""))|(static\s*char\s*(?<key>[\w_]+)[^=]*=\s*(?<value>""(\\""|[^""])+""))");

        /// <summary>
        /// Regex used to capture #include strings.
        /// </summary>
        private static readonly Regex HashIncludeRegex = new Regex(@"#include\s*(<|"")(?<value>[^>""]+)(""|>)");

        /// <summary>
        /// Regex used to identify calls to proto_register_protocol.
        /// </summary>
        private static readonly Regex ProtoRegisterProtocolRegex = new Regex(@"proto_register_protocol\s*\(\s*([\w_]+|""(\\""|[^""])+"")\s*,\s*(/\*([^\*]|\*[^/])*\*/\s*)*([\w_]+|""(\\""|[^""])+"")\s*,\s*(/\*([^\*]|\*[^/])*\*/\s*)*(?<protoname>[\w_]+|""(\\""|[^""])+"")\s*(/\*([^\*]|\*[^/])*\*/\s*)*\)");

        /// <summary>
        /// Regex used to speculatively identify strings that may form part of a text field.
        /// </summary>
        private static readonly Regex SpeculativeConstantRegex = new Regex(@"""(?<text> *[\w\(][\w_\- \(\)']{1,64})(: *)?""");

        /// <summary>
        /// Processes a single source file.
        /// </summary>
        /// <param name="file">Path to the file to process.</param>
        /// <param name="fixups">Receives fixup data.</param>
        /// <param name="trace">Receives trace data.</param>
        /// <param name="constantFixups">Set of constant fixups already added.</param>
        /// <param name="prefixFixups">Set of prefix fixups already added.</param>
        /// <param name="speculativeFixups">Set of speculative fixups already addred.</param>
        private void ProcessSourceFile(string file, StreamWriter fixups, StreamWriter trace, HashSet<string> constantFixups, HashSet<string> prefixFixups, HashSet<string> speculativeFixups)
        {
            Log.WriteInfo("Processing source file: {0}", file);

            fixups.WriteLine(string.Format("<!-- File: {0} -->", XmlCommentEscape(Path.GetFileName(file))));

            try
            {
                string fileContent = File.ReadAllText(file);

                HashSet<string> hashIncludes = new HashSet<string>();
                Dictionary<string, string> hashDefines = new Dictionary<string, string>();
                List<string> protocolNames = new List<string>();

                FindHashIncludes(fileContent, hashIncludes);

                if (hashIncludes.Count > 0)
                {
                    foreach (string hashInclude in hashIncludes)
                    {
                        string headerFile = Path.Combine(Path.GetDirectoryName(file), Path.GetFileName(hashInclude));
                        if (File.Exists(headerFile))
                        {
                            string headerFileContent = File.ReadAllText(headerFile);

                            FindHashDefines(headerFileContent, hashDefines);
                            FindProtocolNames(headerFileContent, hashDefines, protocolNames);
                        }
                    }
                }

                FindHashDefines(fileContent, hashDefines);
                FindProtocolNames(fileContent, hashDefines, protocolNames);

                if (protocolNames.Count == 0)
                {
                    string fileName = Path.GetFileNameWithoutExtension(file);
                    if (fileContent.Contains("proto_register_protocol"))
                    {
                        Log.WriteWarning("Unable to parse protocol name for file: {0}", file);
                    }
                    if (fileName.StartsWith("packet-", StringComparison.OrdinalIgnoreCase))
                    {
                        protocolNames.Add(fileName.Substring(7));
                    }
                }

                if (protocolNames.Count > 0)
                {
                    StringBuilder protocolAttributeBuilder = new StringBuilder();
                    protocolAttributeBuilder.Append("protocol=\"");
                    for (int i = 0; i < protocolNames.Count; i++)
                    {
                        if (i > 0)
                        {
                            protocolAttributeBuilder.Append(',');
                        }

                        // TODO make sure we don't get any commas in the protocol names
                        protocolAttributeBuilder.Append(XmlAttributeValueEscape(protocolNames[i]));
                    }
                    protocolAttributeBuilder.Append("\"");

                    string protocolAttribute = protocolAttributeBuilder.ToString();

                    if (hashIncludes.Count > 0)
                    {
                        foreach (string hashInclude in hashIncludes)
                        {
                            string headerFile = Path.Combine(Path.GetDirectoryName(file), Path.GetFileName(hashInclude));
                            if (File.Exists(headerFile))
                            {
                                string headerFileContent = File.ReadAllText(headerFile);

                                ProcessSourceFileInternal(headerFile, fixups, trace, constantFixups, prefixFixups, speculativeFixups, headerFileContent, protocolAttribute);
                            }
                        }
                    }

                    ProcessSourceFileInternal(file, fixups, trace, constantFixups, prefixFixups, speculativeFixups, fileContent, protocolAttribute);
                }
            }
            catch (Exception ex)
            {
                Log.WriteError("Error processing source file.\nFile: {0}\nError: {1}", file, ex.Message);
            }
        }

        /// <summary>
        /// Called internally by ProcessSourceFile to perform work on each source file.
        /// </summary>
        /// <param name="file">Path to the file to process.</param>
        /// <param name="fixups">Receives fixup data.</param>
        /// <param name="trace">Receives trace data.</param>
        /// <param name="constantFixups">Set of constant fixups already added.</param>
        /// <param name="prefixFixups">Set of prefix fixups already added.</param>
        /// <param name="speculativeFixups">Set of speculative fixups already addred.</param>
        /// <param name="fileContent">File contents.</param>
        /// <param name="protocolAttribute">Formatted protocol attribute to include in output XML.</param>
        private static void ProcessSourceFileInternal(string file, StreamWriter fixups, StreamWriter trace, HashSet<string> constantFixups, HashSet<string> prefixFixups, HashSet<string> speculativeFixups, string fileContent, string protocolAttribute)
        {
            foreach (Match match in ProtoTreeAddTextRegex.Matches(fileContent))
            {
                Group textGroup = match.Groups["text"];
                if (textGroup.Success)
                {
                    string format = textGroup.Value;

                    //Log.WriteInfo("Found text format string: {0}", format);

                    int percentIndex = format.IndexOf('%');
                    int colonIndex = format.IndexOf(':');

                    if (percentIndex < 0 && colonIndex < 0)
                    {
                        string formatInEscaped = XmlAttributeValueEscape(format);
                        if (constantFixups.Add(string.Concat(protocolAttribute, ":", format)))
                        {
                            fixups.WriteLine(string.Format("<constant {1} text=\"{0}\" />", formatInEscaped, protocolAttribute));
                        }

                        trace.WriteLine(string.Format("{0} - {1} - Constant", file, format));
                    }
                    else if (colonIndex > 0 && (percentIndex < 0 || percentIndex > colonIndex))
                    {
                        int secondColonIndex = format.IndexOf(':', colonIndex + 1);
                        if (secondColonIndex < 0)
                        {
                            format = format.Substring(0, colonIndex);

                            string formatInEscaped = XmlAttributeValueEscape(format);
                            if (prefixFixups.Add(string.Concat(protocolAttribute, ":", format)))
                            {
                                fixups.WriteLine(string.Format("<prefix {1} text=\"{0}\" />", formatInEscaped, protocolAttribute));
                            }

                            trace.WriteLine(string.Format("{0} - {1} - Prefix", file, format));
                        }
                        else
                        {
                            // multiple colons - could be multivalue templated text - not a valid fixup
                            trace.WriteLine(string.Format("{0} - {1} - Failed - Multiple colons", file, format));
                        }
                    }
                    else
                    {
                        // no colon before the first templated value - not a valid fixup
                        trace.WriteLine(string.Format("{0} - {1} - Failed - No colon before first percent", file, format));
                    }
                }
            }

            foreach (Match match in SpeculativeConstantRegex.Matches(fileContent))
            {
                Group textGroup = match.Groups["text"];
                if (textGroup.Success)
                {
                    string format = textGroup.Value;

                    //Log.WriteInfo("Found speculative string: {0}", format);
                    string formatInEscaped = XmlAttributeValueEscape(format);
                    if (speculativeFixups.Add(string.Concat(protocolAttribute, ":", format)))
                    {
                        fixups.WriteLine(string.Format("<speculative {1} text=\"{0}\" />", formatInEscaped, protocolAttribute));
                    }

                    trace.WriteLine(string.Format("{0} - {1} - Speculative", file, format));
                }
            }
        }

        /// <summary>
        /// Identifies named protocols in the given source file data.
        /// </summary>
        /// <param name="fileContent">Source file data.</param>
        /// <param name="hashDefines">Dictionary of hash defined values.</param>
        /// <param name="protocolNames">Receives protocol names.</param>
        private static void FindProtocolNames(string fileContent, Dictionary<string, string> hashDefines, List<string> protocolNames)
        {
            foreach (Match match in ProtoRegisterProtocolRegex.Matches(fileContent))
            {
                Group protonameGroup = match.Groups["protoname"];
                if (protonameGroup.Success)
                {
                    string protoname = protonameGroup.Value;
                    if (!protoname.StartsWith("\"") || !protoname.EndsWith("\""))
                    {
                        hashDefines.TryGetValue(protoname, out protoname);
                    }
                    if (!string.IsNullOrEmpty(protoname))
                    {
                        if (protoname.StartsWith("\"") && protoname.EndsWith("\""))
                        {
                            protoname = protoname.Substring(1, protoname.Length - 2);
                        } 
                        protocolNames.Add(protoname);
                    }
                }
            }
        }

        /// <summary>
        /// Identifies hash defined values in the given source file data.
        /// </summary>
        /// <param name="fileContent">Source file data.</param>
        /// <param name="hashDefines">Receives hash defined values.</param>
        private static void FindHashDefines(string fileContent, Dictionary<string, string> hashDefines)
        {
            foreach (Match match in HashDefineRegex.Matches(fileContent))
            {
                Group keyGroup = match.Groups["key"];
                Group valueGroup = match.Groups["value"];
                if (keyGroup.Success && valueGroup.Success)
                {
                    hashDefines[keyGroup.Value] = valueGroup.Value;
                }
            }
        }

        /// <summary>
        /// Identifies hash included files in the given source file data.
        /// </summary>
        /// <remarks>
        /// This method does not recursively search included files.
        /// </remarks>
        /// <param name="fileContent">Source file data.</param>
        /// <param name="hashIncludes">Receives hash included file names.</param>
        private static void FindHashIncludes(string fileContent, HashSet<string> hashIncludes)
        {
            foreach (Match match in HashIncludeRegex.Matches(fileContent))
            {
                Group valueGroup = match.Groups["value"];
                if (valueGroup.Success)
                {
                    hashIncludes.Add(valueGroup.Value);
                }
            }
        }
    }
}
