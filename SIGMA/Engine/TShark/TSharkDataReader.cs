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
using System.Linq;
using System.Net;
using System.Text;
using System.Xml;
using Sigma.Common;

namespace Sigma.Engine.TShark
{

    /// <summary>
    /// Reads TShark formatted XML data files.
    /// </summary>
    internal class TSharkDataReader
    {

        /// <summary>
        /// Reads TShark formatted data files.
        /// </summary>
        /// <param name="sourceFile">The original capture file path.</param>
        /// <param name="schema">The TShark schema.</param>
        /// <param name="dataFile">The TShark data file path.</param>
        /// <param name="callback">Callback that will receive data rows.</param>
        /// <param name="fixups">Fixups object containing any fixups to apply.</param>
        public static void Read(string sourceFile, TSharkDataSchema schema, string dataFile, TSharkDataReaderCallback callback, TSharkFixups fixups)
        {
            XmlReaderSettings xmlReaderSettings = new XmlReaderSettings();
            xmlReaderSettings.ConformanceLevel = ConformanceLevel.Document;
            xmlReaderSettings.IgnoreComments = true;
            xmlReaderSettings.IgnoreProcessingInstructions = true;
            xmlReaderSettings.IgnoreWhitespace = true;
            xmlReaderSettings.ValidationType = ValidationType.None;

            CapFile capFile = new CapFile();
            capFile.Path = sourceFile;

            using (StreamReader dataReader = new StreamReader(dataFile, Encoding.UTF8))
            using (XmlReader xmlReader = XmlReader.Create(dataReader, xmlReaderSettings))
            {
                ParseState state = new ParseState();
                state.Schema = schema;
                state.Document = new XmlDocument();
                state.Reader = xmlReader;
                state.File = capFile;
                state.Callback = callback;
                state.Fixups = fixups;

                int index = 0;

                // read all packets in the file
                while (true)
                {
                    if (!xmlReader.ReadToFollowing("packet"))
                        break;

                    CapPacket capPacket = new CapPacket();
                    capPacket.File = state.File;
                    capPacket.Number = index++;

                    try
                    {
                        state.Packet = capPacket;

                        ReadPacket(state);
                    }
                    catch (Exception ex)
                    {
                        Log.WriteError("Error processing packet.\nIndex: {0}\nError: {1}", index, ex.Message);
                    }
                    finally
                    {
                        state.Packet = null;
                    }
                }
            }
        }

        /// <summary>
        /// Reads a packet entry.
        /// </summary>
        /// <param name="state">The parse state.</param>
        private static void ReadPacket(ParseState state)
        {
            XmlReader xmlReader = state.Reader;

            if (!xmlReader.ReadToDescendant("proto"))
                return;

            CapDataRow packetRow = null;
            state.DataRow = null;

            TSharkDataReaderCallback callback = state.Callback;
            
            int index = 0;
            while (true)
            {
                if (xmlReader.Name != "proto")
                {
                    if (packetRow != null && callback != null)
                    {
                        callback(packetRow);
                    }
                    return;
                }

                CapProtocol capProtocol = new CapProtocol();
                capProtocol.Packet = state.Packet;
                capProtocol.NestingLevel = index++;

                try
                {
                    state.Protocol = capProtocol;

                    ReadProtocol(state);
                }
                finally
                {
                    state.Protocol = null;
                    if (packetRow == null)
                    {
                        packetRow = state.DataRow;
                    }
                }
            }
        }

        /// <summary>
        /// Reads a protocol entry.
        /// </summary>
        /// <param name="state">The parse state.</param>
        private static void ReadProtocol(ParseState state)
        {
            XmlReader xmlReader = state.Reader;
            XmlDocument xmlDocument = state.Document;
            XmlNode xmlNode = xmlDocument.ReadNode(xmlReader);
            TreeNode treeNode = GetTreeNode(state, null, xmlNode);

            CapDataRow protocolRow = Flatten(treeNode);
            if (protocolRow != null)
            {
                if (state.DataRow != null)
                {
                    state.DataRow.ChildRows.Add(protocolRow);
                }

                // add the source file
                if (treeNode.Name == "geninfo")
                {
                    protocolRow.Columns["file"] = state.File.Path;
                }

                state.DataRow = protocolRow;
            }
        }

        /// <summary>
        /// Flattens a parsed tree node into a data row.
        /// </summary>
        /// <param name="node">Node to flatten.</param>
        /// <returns>A data row.</returns>
        private static CapDataRow Flatten(TreeNode node)
        {
            return Flatten(node, null, null);
        }

        /// <summary>
        /// Flattens a parsed tree node into a data row.
        /// </summary>
        /// <param name="node">Node to flatten.</param>
        /// <param name="namingPrefix">Current naming prefix.</param>
        /// <param name="lastParentName">The last assigned parent name in the tree being flattened.</param>
        /// <returns>A data row.</returns>
        private static CapDataRow Flatten(TreeNode node, string namingPrefix, string lastParentName)
        {
            int namelessLeafChildren = 0;
            int namelessBranchChildren = 0;
            List<TreeNode> leafChildren = new List<TreeNode>();
            List<TreeNode> branchChildren = new List<TreeNode>();

            foreach (TreeNode childNode in node.ChildNodes)
            {
                bool nameless = string.IsNullOrEmpty(childNode.Name);
                if (childNode.ChildNodes.Count > 0)
                {
                    branchChildren.Add(childNode);
                    if (nameless)
                    {
                        namelessBranchChildren++;
                    }
                }
                else
                {
                    leafChildren.Add(childNode);
                    if (nameless)
                    {
                        namelessLeafChildren++;
                    }
                }
            }

            string name;
            if (!string.IsNullOrEmpty(node.Name))
            {
                name = FilterName(node.Name);
            }
            else
            {
                if (!string.IsNullOrEmpty(lastParentName))
                {
                    name = string.Concat(lastParentName, ".", "_group");
                }
                else
                {
                    name = "_group";
                }
            }
            
            string tableName = CombineNames(namingPrefix, name);

            string innerNamingPrefix = namingPrefix;
            if (string.IsNullOrEmpty(innerNamingPrefix))
            {
                innerNamingPrefix = tableName;
            }

            // uncomment this line to ensure that fields are always prefixed with their parent field/protocol name (overlap removed)
            // this can generate very long table names so it's not recommended
            //innerNamingPrefix = tableName;

            CapDataRow row = new CapDataRow();
            row.Table = tableName;

            // unnamed columns

            if (namelessLeafChildren > 0)
            {
                int index = 0;

                string unnamedTableName = string.Concat(tableName, ".", "_value");
                foreach (TreeNode leafChild in leafChildren)
                {
                    if (string.IsNullOrEmpty(leafChild.Name))
                    {
                        CapDataRow childRow = new CapDataRow();
                        childRow.Table = unnamedTableName;
                        childRow.Columns["_index"] = index;
                        childRow.Columns["_value"] = leafChild.TypedValue;

                        row.ChildRows.Add(childRow);

                        index++;
                    }
                }
            }

            // named columns

            if (namelessLeafChildren < leafChildren.Count)
            {
                foreach (TreeNode leafChild in leafChildren)
                {
                    if (!string.IsNullOrEmpty(leafChild.Name))
                    {
                        string columnName = FilterName(leafChild.Name);
                        object typedValue = leafChild.TypedValue;
                        string typedValueString = leafChild.TypedValueString;
                        
                        if (typedValue != null)
                        {
                            InsertColumn(row, columnName, typedValue);

                            if (typedValueString != null)
                            {
                                InsertColumn(row, string.Concat(columnName, "_string"), typedValueString);
                            }
                        }
                    }
                }
            }

            // unnamed branches

            if (namelessBranchChildren > 0)
            {
                int index = 0;

                foreach (TreeNode branchChild in branchChildren)
                {
                    if (string.IsNullOrEmpty(branchChild.Name))
                    {
                        CapDataRow childRow = Flatten(branchChild, innerNamingPrefix, name);
                        if (childRow != null)
                        {
                            childRow.Columns["_index"] = index;
                            index++;

                            row.ChildRows.Add(childRow);
                        }
                    }
                }
            }

            // named branches

            if (namelessBranchChildren < branchChildren.Count)
            {
                foreach (TreeNode branchChild in branchChildren)
                {
                    if (!string.IsNullOrEmpty(branchChild.Name))
                    {
                        CapDataRow childRow = Flatten(branchChild, innerNamingPrefix, name);
                        if (childRow != null)
                        {
                            row.ChildRows.Add(childRow);
                        }
                    }
                }
            }

            object typedLocalData = node.TypedValue;
            string typedLocalDataString = node.TypedValueString;
            if (typedLocalData != null)
            {
                row.Columns["_value"] = typedLocalData;
                if (typedLocalDataString != null)
                {
                    row.Columns["_string"] = typedLocalDataString;
                }
            }

            if (row.Columns.Count > 0 || row.ChildRows.Count > 0)
            {
                return row;
            }
            else
            {
                return null;
            }
        }

        /// <summary>
        /// Inserts a column into a data row.
        /// </summary>
        /// <remarks>
        /// This method may update an existing column, making it multivalue, if necessary.
        /// </remarks>
        /// <param name="row">Row to modify.</param>
        /// <param name="columnName">Name of the column to insert.</param>
        /// <param name="typedValue">Column value to insert.</param>
        private static void InsertColumn(CapDataRow row, string columnName, object typedValue)
        {
            object existingValue;
            if (row.Columns.TryGetValue(columnName, out existingValue))
            {
                object[] combinedValue = existingValue as object[];
                if (combinedValue != null)
                {
                    object[] oldCombinedValue = combinedValue;

                    combinedValue = new object[oldCombinedValue.Length + 1];
                    Array.Copy(oldCombinedValue, combinedValue, oldCombinedValue.Length);
                    combinedValue[combinedValue.Length - 1] = typedValue;
                }
                else
                {
                    combinedValue = new object[] { existingValue, typedValue };
                }

                row.Columns[columnName] = combinedValue;
            }
            else
            {
                row.Columns[columnName] = typedValue;
            }
        }

        /// <summary>
        /// Filters a node name.
        /// </summary>
        /// <param name="name">Node name to filter.</param>
        /// <returns>Filtered name.</returns>
        private static string FilterName(string name)
        {
            string remainder;
            return FilterName(name, out remainder);
        }

        /// <summary>
        /// Filters a node name.
        /// </summary>
        /// <param name="name">Node name to filter.</param>
        /// <param name="remainder">Receives any left over input data (typically data after a colon ':').</param>
        /// <returns>Filtered name.</returns>
        private static string FilterName(string name, out string remainder)
        {
            if (name == null)
            {
                remainder = null;
                return null;
            }

            remainder = string.Empty;

            if (name.Length == 0)
            {
                return string.Empty;
            }

            StringBuilder filtered = new StringBuilder();
            for (int i = 0; i < name.Length; i++)
            {
                char c = name[i];
                if (char.IsLetterOrDigit(c))
                {
                    filtered.Append(char.ToLowerInvariant(c));
                }
                else if (c == ':')
                {
                    remainder = name.Substring(i + 1).TrimStart();
                    break;
                }
                else if (c == '.' || c == ' ' || c == '_' || c == '-')
                {
                    filtered.Append('.');
                }
            }

            return filtered.ToString();
        }

        /// <summary>
        /// Combines two names, a prefix and a suffix.
        /// </summary>
        /// <param name="prefix">Prefix name.</param>
        /// <param name="suffix">Suffix name.</param>
        /// <returns>Combined name.</returns>
        private static string CombineNames(string prefix, string suffix)
        {
            if (string.IsNullOrEmpty(prefix))
                return suffix;
            if (string.IsNullOrEmpty(suffix))
                return prefix;

            string[] prefixParts = prefix.Split('.');
            string[] suffixParts = suffix.Split('.');

            StringBuilder joined = new StringBuilder();

            // TODO could use a boyer-moore variation here if it's too slow

            int x = 0;
            while (x < prefixParts.Length)
            {
                if (prefixParts[x].Equals(suffixParts[0], StringComparison.OrdinalIgnoreCase))
                {
                    int y = 1;
                    while ((x + y) < prefixParts.Length && y < suffixParts.Length)
                    {
                        if (!prefixParts[(x + y)].Equals(suffixParts[y], StringComparison.OrdinalIgnoreCase))
                        {
                            break;
                        }

                        y++;
                    }
                    if (x + y >= prefixParts.Length)
                    {
                        break;
                    }
                }

                joined.Append(prefixParts[x]);
                joined.Append('.');

                x++;
            }

            joined.Append(suffix);
            return joined.ToString();
        }
        
        /// <summary>
        /// Parses XML child nodes of the given node into TreeNode instances.
        /// </summary>
        /// <param name="state">The parse state.</param>
        /// <param name="parent">Parent tree node.</param>
        /// <param name="node">XML node to parse.</param>
        /// <returns>A list of parsed child nodes.</returns>
        private static IList<TreeNode> GetTreeNodeChildren(ParseState state, TreeNode parent, XmlNode node)
        {
            List<TreeNode> treeNodes = new List<TreeNode>();
            foreach (XmlNode childNode in node.ChildNodes)
            {
                try
                {
                    TreeNode treeNode = GetTreeNode(state, parent, childNode);
                    treeNodes.Add(treeNode);
                }
                catch (Exception ex)
                {
                    Log.WriteError(ex.Message);
                }
            }

            return treeNodes;
        }

        /// <summary>
        /// Converts a string value to an instance of a runtime type.
        /// </summary>
        /// <param name="state">The parse state.</param>
        /// <param name="name">Field name.</param>
        /// <param name="showname">Node's 'showname' attribute.</param>
        /// <param name="show">Node's 'show' attribute.</param>
        /// <param name="value">Node's 'value' attribute.</param>
        /// <param name="typedType">Receives the runtime type of the value.</param>
        /// <param name="typedValue">Receives the typed value.</param>
        /// <param name="typedValueString">Receives the string corresponding to the typed value.</param>
        private static void GetTypedValue(ParseState state, string name, string showname, string show, string value, out Type typedType, out object typedValue, out string typedValueString)
        {
            typedType = null;
            typedValue = null;
            typedValueString = null;

            try
            {
                TSharkField field;
                if (state.Schema.Fields.TryGetValue(name, out field))
                {
                    typedType = field.Type;

                    if (typedType == typeof(bool))
                    {
                        
                        bool? t;
                        switch (show)
                        {
                            case "0":
                                t = false;
                                break;
                            case "1":
                                t = true;
                                break;
                            default:
                                t = null;
                                break;
                        }

                        if (t == null)
                        {
                            typedType = typeof(string);
                            typedValue = show;
                        }
                        else
                        {
                            typedValue = (bool)t;
                        }
                    }
                    else if (typedType == typeof(byte[]))
                    {
                        if (value == null)
                        {
                            typedValue = new byte[0];
                        }
                        else
                        {
                            typedValue = TSharkTypeParser.ParseHexBytes(value);
                        }
                    }
                    else if (typedType == typeof(byte) || typedType == typeof(ushort) || typedType == typeof(uint) || typedType == typeof(ulong))
                    {
                        ParseUnsigned(show, ref typedType, ref typedValue, field);
                    }
                    else if (typedType == typeof(sbyte) || typedType == typeof(short) || typedType == typeof(int) || typedType == typeof(long))
                    {
                        ParseSigned(show, ref typedType, ref typedValue, field);
                    }
                    else if (typedType == typeof(float))
                    {
                        typedValue = float.Parse(show);
                    }
                    else if (typedType == typeof(double))
                    {
                        typedValue = double.Parse(show);
                    }
                    else if (typedType == typeof(DateTime))
                    {
                        typedValue = TSharkTypeParser.ParseDateTime(show);
                    }
                    else if (typedType == typeof(TimeSpan))
                    {
                        // loss of precision here
                        typedValue = TimeSpan.FromSeconds(double.Parse(show));
                    }
                    else if (typedType == typeof(IPAddress))
                    {
                        IPAddress ipAddress;
                        if (!IPAddress.TryParse(show, out ipAddress))
                        {
                            ipAddress = new IPAddress(TSharkTypeParser.ParseHexBytes(value));
                        }
                        typedValue = ipAddress;
                    }
                    else if (typedType == typeof(Guid))
                    {
                        Guid t;
                        if (!Guid.TryParse(show, out t))
                            throw new Exception(string.Format("Badly formatted Guid value: {0}", show));
                        typedValue = t;
                    }
                    else if (typedType == typeof(string) || typedType == null)
                    {
                        typedType = typeof(string);
                        typedValue = show;
                    }

                    //Log.WriteInfo("Type: {0} Value: {1}", typedType, show);
                }

                if (field != null)
                {
                    if (typedValue == null)
                    {
                        Log.WriteWarning("Unable to convert field value.\nName: {0}\nValue: {1}", name, show);
                    }
                    else
                    {
                        typedValueString = field.FindString(typedValue);
                    }
                }
                else
                {
                    //Log.WriteInfo("No field definition for field.\nName: {0}\nValue: {1}", name, show);
                }
            }
            catch (Exception ex)
            {
                typedValue = null;

                Log.WriteWarning("Unable to convert field value.\nName: {0}\nValue: {1}\nError: {2}", name, show, ex.Message);
            }

            if (typedValue == null)
            {
                typedType = typeof(string);

                if (show == null || (value != null && name.IndexOf(show, 0, StringComparison.OrdinalIgnoreCase) >= 0))
                {
                    typedValue = value;
                }
                else
                {
                    typedValue = show;
                }

                typedValueString = null;
            }
        }

        /// <summary>
        /// Parses a signed integer value, storing it in the smallest available signed integer type.
        /// </summary>
        /// <remarks>
        /// This method is called internally by GetTypedValue.
        /// </remarks>
        /// <param name="show">Value to parse.</param>
        /// <param name="typedType">Receives the parsed type.</param>
        /// <param name="typedValue">Receives the parsed value.</param>
        /// <param name="field">The field descriptor.</param>
        private static void ParseSigned(string show, ref Type typedType, ref object typedValue, TSharkField field)
        {
            if (field.DisplayBase.Equals("BASE_NONE"))
            {
                typedType = typeof(string);
                typedValue = show;
                return;
            }

            // oct values seem to be stored in decimal form...?
            
            long v;
            if (field.DisplayBase.StartsWith("BASE_HEX"))
            {
                v = BitConverter.ToInt64(TSharkTypeParser.Reverse(TSharkTypeParser.ParseHexBytes(show, 8)), 0);
            }
            /*else if (field.DisplayBase.StartsWith("BASE_OCT"))
            {
                v = BitConverter.ToInt64(TSharkTypeParser.Reverse(TSharkTypeParser.ParseOctBytes(show, 8)), 0);
            }*/
            else //if (field.DisplayBase.StartsWith("BASE_DEC"))
            {
                v = long.Parse(show);
            }

            if (v >= sbyte.MinValue && v <= sbyte.MaxValue)
            {
                typedType = typeof(sbyte);
                typedValue = (sbyte)v;
            }
            else if (v >= short.MinValue && v <= short.MaxValue)
            {
                typedType = typeof(short);
                typedValue = (short)v;
            }
            else if (v >= int.MinValue && v <= int.MaxValue)
            {
                typedType = typeof(int);
                typedValue = (int)v;
            }
            else //if (v >= long.MinValue && v <= long.MaxValue)
            {
                typedType = typeof(long);
                typedValue = (long)v;
            }
        }

        /// <summary>
        /// Parses an unsigned integer value, storing it in the smallest available unsigned integer type.
        /// </summary>
        /// <remarks>
        /// This method is called internally by GetTypedValue.
        /// </remarks>
        /// <param name="show">Value to parse.</param>
        /// <param name="typedType">Receives the parsed type.</param>
        /// <param name="typedValue">Receives the parsed value.</param>
        /// <param name="field">The field descriptor.</param>
        private static void ParseUnsigned(string show, ref Type typedType, ref object typedValue, TSharkField field)
        {
            if (field.DisplayBase.Equals("BASE_NONE"))
            {
                typedType = typeof(string);
                typedValue = show;
                return;
            }

            // oct values seem to be stored in decimal form...?

            ulong v;
            if (field.DisplayBase.StartsWith("BASE_HEX"))
            {
                v = BitConverter.ToUInt64(TSharkTypeParser.Reverse(TSharkTypeParser.ParseHexBytes(show, 8)), 0);
            }
            /*else if (field.DisplayBase.StartsWith("BASE_OCT"))
            {
                v = BitConverter.ToUInt64(TSharkTypeParser.Reverse(TSharkTypeParser.ParseOctBytes(show, 8)), 0);
            }*/
            else //if (field.DisplayBase.StartsWith("BASE_DEC"))
            {
                v = ulong.Parse(show);
            }

            if (v >= byte.MinValue && v <= byte.MaxValue)
            {
                typedType = typeof(byte);
                typedValue = (byte)v;
            }
            else if (v >= ushort.MinValue && v <= ushort.MaxValue)
            {
                typedType = typeof(ushort);
                typedValue = (ushort)v;
            }
            else if (v >= uint.MinValue && v <= uint.MaxValue)
            {
                typedType = typeof(uint);
                typedValue = (uint)v;
            }
            else //if (v >= ulong.MinValue && v <= ulong.MaxValue)
            {
                typedType = typeof(ulong);
                typedValue = (ulong)v;
            }
        }

        /// <summary>
        /// Parses a single XML node into a TreeNode instance.
        /// </summary>
        /// <param name="state">The parse state.</param>
        /// <param name="parent">Parent tree node.</param>
        /// <param name="node">XML node to parse.</param>
        /// <returns>The parsed node.</returns>
        private static TreeNode GetTreeNode(ParseState state, TreeNode parent, XmlNode node)
        {
            string type;
            string name;
            string showname;
            int size;
            int pos;
            string show;
            string value;
            bool hide;

            type = GetTreeNodeAttributes(node, out name, out showname, out size, out pos, out show, out value, out hide);

            string parentName;
            if (parent != null)
            {
                parentName = parent.Name;
            }
            else
            {
                parentName = null;
            }

            string protocolName = state.Protocol.Name;
            if (protocolName == null && type == "proto")
            {
                protocolName = name;
                state.Protocol.Name = protocolName;
            }

            bool fixupApplied = state.Fixups.ApplyFixups(protocolName, parentName, ref name, ref show, ref showname, ref value);

            // push the formatted value across into the fields we're going to process
            if (fixupApplied)
            {
                show = value;
                showname = value;
            }

            Type typedType;
            object typedValue;
            string typedValueString;

            if (string.IsNullOrEmpty(name))
            {
                // uncomment this line to re-enable auto-generation of node names, not recommended unless using a very small subset of dissectors
                // warning: this will cause tables/columns to be created with names based on values in the captured traffic, consider using a template fixup instead
                //name = FilterName(show, out show);

                typedType = typeof(string);
                typedValue = show;
                typedValueString = null;
            }
            else
            {
                if ("field".Equals(type, StringComparison.OrdinalIgnoreCase))
                {
                    GetTypedValue(state, name, showname, show, value, out typedType, out typedValue, out typedValueString);
                }
                else
                {
                    typedType = null;
                    typedValue = null;
                    typedValueString = null;
                }
            }

            TreeNode result = new TreeNode(parent, type, name, showname, size, pos, show, value, hide, typedType, typedValue, typedValueString);

            if (node.HasChildNodes)
            {
                GetTreeNodeChildren(state, result, node);
            }

            // enforce: packet -> protocol -> field
            string expectedChildNodeType;
            switch (type)
            {
                case "packet":
                    expectedChildNodeType = "proto";
                    break;
                case "proto":
                    expectedChildNodeType = "field";
                    break;
                default:
                case "field":
                    expectedChildNodeType = "field";
                    break;
            }

            IList<TreeNode> childNodes = result.ChildNodes;
            for (int i = 0; i < childNodes.Count; i++)
            {
                TreeNode childNode = childNodes[i];
                if (childNode.Type != expectedChildNodeType)
                {
                    childNodes.RemoveAt(i--);
                }
            }

            return result;
        }

        /// <summary>
        /// Retrieves TShark attributes from the given XML node.
        /// </summary>
        /// <param name="node">XML node to examine.</param>
        /// <param name="name">Receives the 'name' attribute.</param>
        /// <param name="showname">Receives the 'showname' attribute.</param>
        /// <param name="size">Receives the 'size' attribute.</param>
        /// <param name="pos">Receives the 'pos' attribute.</param>
        /// <param name="show">Receives the 'show' attribute.</param>
        /// <param name="value">Receives the 'value' attribute.</param>
        /// <param name="hide">Receives the 'hide' attribute.</param>
        /// <returns>The node's name (e.g. "field").</returns>
        private static string GetTreeNodeAttributes(XmlNode node, out string name, out string showname, out int size, out int pos, out string show, out string value, out bool hide)
        {
            try
            {
                name = SafeGetAttributeValue(node, "name");
                showname = SafeGetAttributeValue(node, "showname");
                size = int.Parse(SafeGetAttributeValue(node, "size") ?? "0");
                pos = int.Parse(SafeGetAttributeValue(node, "pos") ?? "0");
                show = SafeGetAttributeValue(node, "show");
                value = SafeGetAttributeValue(node, "value");
                hide = SafeGetAttributeValue(node, "hide") == "yes";

                return node.Name;
            }
            catch
            {
                throw new Exception(string.Format("Error parsing tree node. Node: {0}", node.OuterXml));
            }
        }

        /// <summary>
        /// Safely gets the value of an XML attributes.
        /// </summary>
        /// <param name="node">The XML node to examine.</param>
        /// <param name="attributeName">The attribute name.</param>
        /// <returns>The attribute value, or null if the attribute was not found.</returns>
        private static string SafeGetAttributeValue(XmlNode node, string attributeName)
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
        /// Represents a parsed TShark tree node.
        /// </summary>
        private class TreeNode
        {

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="parent">Parent node (optional).</param>
            /// <param name="type">Node type (e.g. "field").</param>
            /// <param name="name">Node's 'name' attribute.</param>
            /// <param name="showname">Node's 'showname' attribute.</param>
            /// <param name="size">Node's 'size' attribute.</param>
            /// <param name="pos">Node's 'pos' attribute.</param>
            /// <param name="show">Node's 'show' attribute.</param>
            /// <param name="value">Node's 'value' attribute.</param>
            /// <param name="hide">Node's 'hide' attribute.</param>
            /// <param name="typedType">Node's runtime value type.</param>
            /// <param name="typedValue">Node's runtime typed value.</param>
            /// <param name="typedValueString">The string corresponding to the node's typed value.</param>
            public TreeNode(TreeNode parent, string type, string name, string showname, int size, int pos, string show, string value, bool hide, Type typedType, object typedValue, string typedValueString)
            {
                this.Parent = parent;
                if (parent != null)
                {
                    parent.ChildNodes.Add(this);
                }

                this.Type = type;
                this.Name = name;
                this.Showname = showname;
                this.Size = size;
                this.Pos = pos;
                this.Show = show;
                this.Value = value;
                this.Hide = hide;
                this.ChildNodes = new List<TreeNode>();
                this.TypedType = typedType;
                this.TypedValue = typedValue;
                this.TypedValueString = typedValueString;
            }

            /// <summary>
            /// Node's type (e.g. "field").
            /// </summary>
            public string Type { get; set; }

            /// <summary>
            /// Node's 'name' attribute.
            /// </summary>
            public string Name { get; set; }

            /// <summary>
            /// Node's 'showname' attribute.
            /// </summary>
            public string Showname { get; set; }

            /// <summary>
            /// Node's 'size' attribute.
            /// </summary>
            public int Size { get; set; }

            /// <summary>
            /// Node's 'pos' attribute.
            /// </summary>
            public int Pos { get; set; }

            /// <summary>
            /// Node's 'show' attribute.
            /// </summary>
            public string Show { get; set; }

            /// <summary>
            /// Node's 'value' attribute.
            /// </summary>
            public string Value { get; set; }

            /// <summary>
            /// Node's 'hide' attribute.
            /// </summary>
            public bool Hide { get; set; }

            /// <summary>
            /// The runtime type of this node's value.
            /// </summary>
            public Type TypedType { get; set; }

            /// <summary>
            /// The runtime typed value of this node.
            /// </summary>
            public object TypedValue { get; set; }

            /// <summary>
            /// The string corresponding to this node's typed value.
            /// </summary>
            public string TypedValueString { get; set; }

            /// <summary>
            /// List of child nodes.
            /// </summary>
            public IList<TreeNode> ChildNodes { get; private set; }

            /// <summary>
            /// This node's parent node.
            /// </summary>
            public TreeNode Parent { get; private set; }

            /// <summary>
            /// Finds a single child by its 'name' attribute.
            /// </summary>
            /// <remarks>
            /// This method throws an exception if multiple matching nodes are found.
            /// </remarks>
            /// <param name="name">The name to look for.</param>
            /// <returns>The specified child, null if not found.</returns>
            public TreeNode FindChild(string name)
            {
                return ChildNodes.SingleOrDefault(n => n.Name == name);
            }

            /// <summary>
            /// Returns a <see cref="System.String"/> that represents this instance.
            /// </summary>
            /// <returns>
            /// A <see cref="System.String"/> that represents this instance.
            /// </returns>
            public override string ToString()
            {
                return string.Format("[ Type: '{0}', Name: '{1}', Showname: '{2}', Show: '{3}' ]", Type, Name, Showname, Show); 
            }
        }

        /// <summary>
        /// Encapsulates parse state.
        /// </summary>
        private class ParseState
        {

            /// <summary>
            /// The parsed TShark schema.
            /// </summary>
            public TSharkDataSchema Schema { get; set; }

            /// <summary>
            /// The XML reader.
            /// </summary>
            public XmlReader Reader { get; set; }

            /// <summary>
            /// A reusable XML document instance.
            /// </summary>
            public XmlDocument Document { get; set; }

            /// <summary>
            /// The file being parsed.
            /// </summary>
            public CapFile File { get; set; }

            /// <summary>
            /// The packet being parsed.
            /// </summary>
            public CapPacket Packet { get; set; }

            /// <summary>
            /// The protocol being parsed.
            /// </summary>
            public CapProtocol Protocol { get; set; }

            /// <summary>
            /// The data row being parsed.
            /// </summary>
            public CapDataRow DataRow { get; set; }

            /// <summary>
            /// The active data reader callback.
            /// </summary>
            public TSharkDataReaderCallback Callback { get; set; }

            /// <summary>
            /// The active fixups.
            /// </summary>
            public TSharkFixups Fixups { get; set; }
        }
    }

    /// <summary>
    /// Delegates of this type are used as callbacks with the TSharkDataReader class.
    /// </summary>
    /// <param name="row">The row to process.</param>
    internal delegate void TSharkDataReaderCallback(CapDataRow row);

    /// <summary>
    /// Contains file metadata.
    /// </summary>
    public class CapFile
    {

        /// <summary>
        /// File path.
        /// </summary>
        public string Path { get; set; }
    }

    /// <summary>
    /// Contains packet metadata.
    /// </summary>
    public class CapPacket
    {

        /// <summary>
        /// The parent file.
        /// </summary>
        public CapFile File { get; set; }

        /// <summary>
        /// The packet number.
        /// </summary>
        public long Number { get; set; }
    }

    /// <summary>
    /// Contains protocol metadata.
    /// </summary>
    public class CapProtocol
    {

        /// <summary>
        /// Constructor.
        /// </summary>
        public CapProtocol()
        {
            this.Rows = new List<CapDataRow>();
        }

        /// <summary>
        /// The protocol name.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// The parent packet.
        /// </summary>
        public CapPacket Packet { get; set; }

        /// <summary>
        /// The nesting level.
        /// </summary>
        public int NestingLevel { get; set; }

        /// <summary>
        /// List of rows comprising this protocol.
        /// </summary>
        public IList<CapDataRow> Rows { get; set; }
    }

    /// <summary>
    /// Contains a data row.
    /// </summary>
    public class CapDataRow
    {

        /// <summary>
        /// Constructor.
        /// </summary>
        public CapDataRow()
        {
            this.Columns = new Dictionary<string, object>();
            this.ChildRows = new List<CapDataRow>();
            this.ID = -1;
        }

        /// <summary>
        /// Table name.
        /// </summary>
        public string Table { get; set; }

        /// <summary>
        /// Row identifier.
        /// </summary>
        public long ID { get; set; }

        /// <summary>
        /// Column name/value mapping.
        /// </summary>
        public IDictionary<string, object> Columns { get; set; }

        /// <summary>
        /// List of child rows attached to this row.
        /// </summary>
        public IList<CapDataRow> ChildRows { get; set; }
    }
}
