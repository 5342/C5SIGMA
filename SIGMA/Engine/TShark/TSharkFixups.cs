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
using System.Xml;
using Sigma.Common;
using Sigma.Common.Support;

namespace Sigma.Engine.TShark
{

    /// <summary>
    /// Exposes fixups for the TShark parsing process.
    /// </summary>
    /// <remarks>
    /// Fixups are stored in an embedded resource called Fixups.xml.compressed. Fixups are used to automatically determine, or modify, the name of TShark XML nodes.
    /// This is a compressed copy of the file Fixups.xml, created at build time using a pre-build event that invokes SigmaCompress.exe (another project in the solution).
    /// </remarks>
    internal class TSharkFixups
    {

        /// <summary>
        /// Constructor.
        /// </summary>
        public TSharkFixups()
        {
            try
            {
                LoadInternalFixups();
            }
            catch
            {
            }
        }

        /// <summary>
        /// Loads the embedded fixups resource.
        /// </summary>
        private void LoadInternalFixups()
        {
            string resourceName = null;
            try
            {
                Assembly assembly = Assembly.GetExecutingAssembly();
                foreach (string name in assembly.GetManifestResourceNames())
                {
                    if (name.EndsWith("Fixups.xml.compressed", StringComparison.OrdinalIgnoreCase))
                    {
                        resourceName = name;
                        break;
                    }
                }

                if (resourceName == null)
                {
                    throw new Exception("Missing embedded resource 'Fixups.xml.compressed'.");
                }
            }
            catch (Exception ex)
            {
                Log.WriteError(ex.Message);
                Log.WriteError("Unable to locate embedded fixups.");
                return;
            }

            int installedCount = 0;
            try
            {
                Assembly assembly = Assembly.GetExecutingAssembly();
                using (Stream fixupsStream = assembly.GetManifestResourceStream(resourceName))
                using (Stream decompressionStream = CompressionSupport.DecompressStream(fixupsStream))
                {
                    LoadFixupsStream(ref installedCount, decompressionStream);
                }

                Log.WriteInfo("Installed embedded fixups: {0}", installedCount);
            }
            catch (Exception ex)
            {
                Log.WriteError(ex.Message);
                Log.WriteError("Unable to load embedded fixups: {0}", resourceName);
                return;
            }
        }

        /// <summary>
        /// Loads a file containing fixups.
        /// </summary>
        public void LoadExternalFixups(string path)
        {
            int installedCount = 0;
            try
            {
                using (FileStream fixupsStream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read))
                {
                    LoadFixupsStream(ref installedCount, fixupsStream);
                }

                Log.WriteInfo("Installed external fixups: {0}", installedCount);
            }
            catch (Exception ex)
            {
                Log.WriteError(ex.Message);
                Log.WriteError("Unable to load external fixups: {0}", path);
                return;
            }
        }

        /// <summary>
        /// Loads fixups from a stream.
        /// </summary>
        /// <param name="installedCount">Tracks the number of installed fixups.</param>
        /// <param name="fixupsStream">Stream containing fixups.</param>
        private void LoadFixupsStream(ref int installedCount, Stream fixupsStream)
        {
            XmlDocument fixups = new XmlDocument();
            using (StreamReader reader = new StreamReader(fixupsStream, Encoding.UTF8))
            {
                fixups.Load(reader);
            }

            foreach (XmlNode node in fixups.SelectNodes("/fixups/*"))
            {
                bool installed;
                try
                {
                    installed = InstallFixup(node);
                }
                catch (Exception ex)
                {
                    Log.WriteError(ex.Message);
                    installed = false;
                }

                if (!installed)
                {
                    Log.WriteError("Skipped fixup: {0}", node.OuterXml);
                }
                else
                {
                    //Log.WriteDebug("Fixup installed: {0}", node.OuterXml);
                    installedCount++;
                }
            }
        }

        /// <summary>
        /// Installs a single fixup from its XML representation.
        /// </summary>
        /// <param name="node">Fixup XML node.</param>
        private bool InstallFixup(XmlNode node)
        {
            switch (node.Name.ToLowerInvariant())
            {
                case "template":
                    return InstallTemplateFixup(node);
                case "constant":
                    return InstallStandardFixup(node, true, false);
                case "prefix":
                    return InstallStandardFixup(node, false, true);
                case "speculative":
                    return InstallStandardFixup(node, true, true);
                default:
                    return false;
            }
        }

        /// <summary>
        /// Adds a fixup to a fixup table.
        /// </summary>
        /// <param name="fixupTable">Fixup table to modify.</param>
        /// <param name="protocol">Comma separated list of protocols.</param>
        /// <param name="text">Fixup text.</param>
        /// <param name="name">Fixup name.</param>
        private static void AddFixup(Dictionary<string, Dictionary<string, string>> fixupTable, string protocol, string text, string name)
        {
            foreach (string part in protocol.Split(','))
            {
                Dictionary<string, string> protocolSpecific;
                if (!fixupTable.TryGetValue(part, out protocolSpecific))
                {
                    protocolSpecific = new Dictionary<string, string>();
                    fixupTable[part] = protocolSpecific;
                }
                protocolSpecific[text] = name;
            }
        }

        /// <summary>
        /// Installs a standard fixup.
        /// </summary>
        /// <param name="node">Fixup XML node.</param>
        /// <param name="constant">True to install a constand fixup.</param>
        /// <param name="prefix">True to install a prefix fixup.</param>
        /// <returns>True if a fixup was installed, false otherwise.</returns>
        private bool InstallStandardFixup(XmlNode node, bool constant, bool prefix)
        {
            string text = SafeGetAttributeValue(node, "text");
            string name = SafeGetAttributeValue(node, "name");
            string protocol = SafeGetAttributeValue(node, "protocol");

            if (string.IsNullOrEmpty(text) || string.IsNullOrEmpty("protocol"))
                return false;

            if (string.IsNullOrEmpty(name))
                name = FilterName(text);

            if (constant)
            {
                AddFixup(constantFixups, protocol, text, name);
            }
            if (prefix)
            {
                AddFixup(prefixFixups, protocol, text, name);
            }

            return true;
        }

        /// <summary>
        /// Filters a name string.
        /// </summary>
        /// <param name="value">Name string to filter.</param>
        /// <returns>Filtered name string.</returns>
        private static string FilterName(string value)
        {
            bool hasDot = true;

            StringBuilder builder = new StringBuilder(value.Length);
            for (int i = 0; i < value.Length; i++)
            {
                char c = value[i];
                if (char.IsLetterOrDigit(c))
                {
                    builder.Append(char.ToLowerInvariant(c));
                    hasDot = false;
                }
                else if (c == '.' || c == ' ' || c == '_' || c == '-')
                {
                    if (!hasDot)
                    {
                        builder.Append('.');
                        hasDot = true;
                    }
                }
            }

            while (builder.Length > 0 && builder[builder.Length - 1] == '.')
            {
                builder.Length--;
            }

            return builder.ToString();
        }

        /// <summary>
        /// Installs a single template fixup from its XML representation.
        /// </summary>
        /// <param name="node">Fixup XML node.</param>
        /// <returns>True if a fixup was installed, false otherwise.</returns>
        private bool InstallTemplateFixup(XmlNode node)
        {
            string parentNameRegex = SafeGetAttributeValue(node, "parentName");
            string nameRegex = SafeGetAttributeValue(node, "name");
            string showRegex = SafeGetAttributeValue(node, "show");
            string shownameRegex = SafeGetAttributeValue(node, "showname");
            string valueRegex = SafeGetAttributeValue(node, "value");
            string nameFormat = SafeGetAttributeValue(node, "nameFormat");
            string valueFormat = SafeGetAttributeValue(node, "valueFormat");

            if (string.IsNullOrEmpty(parentNameRegex) && string.IsNullOrEmpty(nameRegex) && string.IsNullOrEmpty(showRegex) && string.IsNullOrEmpty(shownameRegex) && string.IsNullOrEmpty(valueRegex))
                return false;

            templateFixups.Add(new TemplateFixup(parentNameRegex, nameRegex, showRegex, shownameRegex, valueRegex, nameFormat, valueFormat));
            return true;
        }

        /// <summary>
        /// Applies fixups to the given item parameters.
        /// </summary>
        /// <param name="protocolName">Name of the parent protocol.</param>
        /// <param name="parentName">Name of the parent item.</param>
        /// <param name="name">'name' attribute of the item to fixup.</param>
        /// <param name="show">'show' attribute of the item to fixup.</param>
        /// <param name="showname">'showname' attribute of the item to fixup.</param>
        /// <param name="value">'value' attribute of the item to fixup.</param>
        /// <returns>True if a fixup was applied, false otherwise.</returns>
        public bool ApplyFixups(string protocolName, string parentName, ref string name, ref string show, ref string showname, ref string value)
        {
            bool result = false;
            result |= ApplyStandardFixups(protocolName, parentName, ref name, ref show, ref showname, ref value);
            result |= ApplyTemplateFixups(parentName, ref name, ref show, ref showname, ref value);
            return result;
        }

        /// <summary>
        /// Applies fixups stored in the deterministic finite automaton.
        /// </summary>
        /// <param name="protocolName">Name of the parent protocol.</param>
        /// <param name="parentName">Name of the parent item.</param>
        /// <param name="name">'name' attribute of the item to fixup.</param>
        /// <param name="show">'show' attribute of the item to fixup.</param>
        /// <param name="showname">'showname' attribute of the item to fixup.</param>
        /// <param name="value">'value' attribute of the item to fixup.</param>
        /// <returns>True if a fixup was applied, false otherwise.</returns>
        private bool ApplyStandardFixups(string protocolName, string parentName, ref string name, ref string show, ref string showname, ref string value)
        {
            if (string.IsNullOrEmpty(show) || !string.IsNullOrEmpty(name) || string.IsNullOrEmpty(protocolName))
                return false;

            if (constantFixups.Count > 0)
            {
                Dictionary<string, string> protocolSpecific;
                if (constantFixups.TryGetValue(protocolName, out protocolSpecific))
                {
                    string newName;
                    if (protocolSpecific.TryGetValue(show, out newName))
                    {
                        name = newName;
                        showname = show;
                        show = string.Empty;
                        value = string.Empty;
                        return true;
                    }
                }
            }

            if (prefixFixups.Count > 0)
            {
                int colonIndex = show.IndexOf(':');
                if (colonIndex > 0)
                {
                    Dictionary<string, string> protocolSpecific;
                    if (prefixFixups.TryGetValue(protocolName, out protocolSpecific))
                    {
                        string prefix = show.Substring(0, colonIndex);
                        string newName;
                        if (protocolSpecific.TryGetValue(prefix, out newName))
                        {
                            name = newName;
                            showname = show;
                            show = show.Substring(colonIndex + 1).TrimStart();
                            value = show;
                            return true;
                        }
                    }
                }
            }

            return false;
        }

        /// <summary>
        /// Applies fixups to the given item parameters.
        /// </summary>
        /// <param name="parentName">Name of the parent item.</param>
        /// <param name="name">'name' attribute of the item to fixup.</param>
        /// <param name="show">'show' attribute of the item to fixup.</param>
        /// <param name="showname">'showname' attribute of the item to fixup.</param>
        /// <param name="value">'value' attribute of the item to fixup.</param>
        /// <returns>True if a fixup was applied, false otherwise.</returns>
        private bool ApplyTemplateFixups(string parentName, ref string name, ref string show, ref string showname, ref string value)
        {
            if (templateFixups.Count == 0)
                return false;

            string newName = null;
            string newValue = null;

            IList<KeyValuePair<string, string>> accumulator = null;
            foreach (TemplateFixup fixup in templateFixups)
            {
                bool matched = true;
                matched &= RegexSupport.CheckMatch(fixup.ParentNameRegex, parentName, ref accumulator);
                matched &= RegexSupport.CheckMatch(fixup.NameRegex, name, ref accumulator);
                matched &= RegexSupport.CheckMatch(fixup.ShowRegex, show, ref accumulator);
                matched &= RegexSupport.CheckMatch(fixup.ShownameRegex, showname, ref accumulator);
                matched &= RegexSupport.CheckMatch(fixup.ValueRegex, value, ref accumulator);

                if (matched)
                {
                    accumulator.Add(new KeyValuePair<string, string>("parentName", parentName));
                    accumulator.Add(new KeyValuePair<string, string>("parentNamePrefix", !string.IsNullOrEmpty(parentName) ? string.Concat(parentName, ".") : string.Empty));
                    accumulator.Add(new KeyValuePair<string, string>("name", name));
                    accumulator.Add(new KeyValuePair<string, string>("show", show));
                    accumulator.Add(new KeyValuePair<string, string>("showname", showname));
                    accumulator.Add(new KeyValuePair<string, string>("value", value));

                    string newNewName = TemplateValue(fixup.NameFormat, accumulator, true);
                    string newNewValue = TemplateValue(fixup.ValueFormat, accumulator, false);

                    // TODO checking?

                    newName = newNewName;
                    newValue = newNewValue;
                }

                if (accumulator != null)
                {
                    accumulator.Clear();
                }
            }

            if (newName != null || newValue != null)
            {
                name = newName;
                showname = newName;
                show = newValue;
                value = newValue;
                return true;
            }
            else
            {
                return false;
            }
        }

        /// <summary>
        /// Uses the given format string to generate a templated value by inserting text from the capture accumulator.
        /// </summary>
        /// <param name="format">Format string containing "$(key)" sequences to be replaced.</param>
        /// <param name="accumulator">Capture accumulator containg key-value pairs to use when formatting.</param>
        /// <param name="normalize">True to normalize the output by replacing non-letter-digit character runs with a '.' character, false otherwise.</param>
        /// <returns>The templated string.</returns>
        private string TemplateValue(string format, IList<KeyValuePair<string, string>> accumulator, bool normalize)
        {
            if (string.IsNullOrEmpty(format))
                return null;

            StringBuilder formatted = new StringBuilder(format);
            foreach (KeyValuePair<string, string> kvp in accumulator)
            {
                formatted.Replace(string.Concat("$(", kvp.Key, ")"), kvp.Value);
            }

            if (normalize)
            {
                int startRemove = -1;
                for (int i = 0; i < formatted.Length; i++)
                {
                    char c = formatted[i];
                    if (!char.IsLetterOrDigit(c))
                    {
                        if (startRemove < 0)
                        {
                            startRemove = i;
                        }
                    }
                    else
                    {
                        formatted[i] = char.ToLowerInvariant(c);

                        if (startRemove >= 0)
                        {
                            formatted.Remove(startRemove, i - startRemove);
                            formatted.Insert(startRemove, '.');

                            i = startRemove;
                            startRemove = -1;
                        }
                    }
                }

                if (startRemove >= 0)
                {
                    formatted.Length = startRemove;
                }
            }

            return formatted.ToString();
        }


        /// <summary>
        /// Safely gets the value of an XML attributes.
        /// </summary>
        /// <param name="node">The XML node to examine.</param>
        /// <param name="attributeName">The attribute name.</param>
        /// <returns>The attribute value, or null if the attribute was not found.</returns>
        private string SafeGetAttributeValue(XmlNode node, string attributeName)
        {
            XmlAttribute attribute = node.Attributes[attributeName];
            if (attribute == null)
            {
                return null;
            }
            else
            {
                return attribute.Value;
            }
        }

        /// <summary>
        /// List of installed template fixups.
        /// </summary>
        private List<TemplateFixup> templateFixups = new List<TemplateFixup>();

        /// <summary>
        /// Dictionary of constant value fixups.
        /// </summary>
        private Dictionary<string, Dictionary<string, string>> constantFixups = new Dictionary<string, Dictionary<string, string>>();

        /// <summary>
        /// Dictionary of prefix fixups.
        /// </summary>
        private Dictionary<string, Dictionary<string, string>> prefixFixups = new Dictionary<string, Dictionary<string, string>>();

        /// <summary>
        /// Represents a single template fixup.
        /// </summary>
        private class TemplateFixup
        {

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="parentNameRegex">Regex to match against the node's parent's 'name' attribute.</param>
            /// <param name="nameRegex">Regex to match against the node's 'name' attribute.</param>
            /// <param name="showRegex">Regex to match against the node's 'show' attribute.</param>
            /// <param name="shownameRegex">Regex to match against the node's 'showname' attribute.</param>
            /// <param name="valueRegex">Regex to match against the node's 'value' attribute.</param>
            /// <param name="nameFormat">Format string used to generate a new node 'name' attribute.</param>
            /// <param name="valueFormat">Format string used to generate new node 'value' attribute.</param>
            public TemplateFixup(string parentNameRegex, string nameRegex, string showRegex, string shownameRegex, string valueRegex, string nameFormat, string valueFormat)
            {
                this.ParentNameRegex = RegexSupport.GetRegex(parentNameRegex);
                this.NameRegex = RegexSupport.GetRegex(nameRegex);
                this.ShowRegex = RegexSupport.GetRegex(showRegex);
                this.ShownameRegex = RegexSupport.GetRegex(shownameRegex);
                this.ValueRegex = RegexSupport.GetRegex(valueRegex);
                this.NameFormat = nameFormat;
                this.ValueFormat = valueFormat;
            }

            /// <summary>
            /// Regex to match against the node's parent's 'name' attribute.
            /// </summary>
            public Regex ParentNameRegex { get; private set; }

            /// <summary>
            /// Regex to match against the node's 'name' attribute.
            /// </summary>
            public Regex NameRegex { get; private set; }
            
            /// <summary>
            /// Regex to match against the node's 'show' attribute.
            /// </summary>
            public Regex ShowRegex { get; private set; }

            /// <summary>
            /// Regex to match against the node's 'showname' attribute.
            /// </summary>
            public Regex ShownameRegex { get; private set; }

            /// <summary>
            /// Regex to match against the node's 'value' attribute.
            /// </summary>
            public Regex ValueRegex { get; private set; }

            /// <summary>
            /// Format string used to generate a new node 'name' attribute.
            /// </summary>
            public string NameFormat { get; private set; }

            /// <summary>
            /// Format string used to generate new node 'value' attribute.
            /// </summary>
            public string ValueFormat { get; private set; }
        }
    }
}
