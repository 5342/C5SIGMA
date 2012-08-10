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
using System.Text.RegularExpressions;
using System.Xml;
using Sigma.Common;
using Sigma.Common.Support;

namespace Sigma.Engine.Database
{

    /// <summary>
    /// Exposes blacklisting and whitelisting functionality for the database creation process.
    /// </summary>
    internal class DataFilter
    {

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <remarks>
        /// If a data filter preset is specified, a filter definition is loaded from an embedded resource.
        /// The name of the embedded resource is created by adding "Filter.xml" to the preset name.
        /// </remarks>
        /// <param name="preset">Name of a data filter preset.</param>
        public DataFilter(string preset)
        {
            try
            {
                LoadInternalFilter(preset);
            }
            catch
            {
            }
        }

        /// <summary>
        /// Loads an embedded resource containing filter rules.
        /// </summary>
        /// <remarks>
        /// The name of the embedded resource is created by adding "Filter.xml" to the preset name.
        /// </remarks>
        /// <param name="preset">Name of a data filter preset.</param>
        private void LoadInternalFilter(string preset)
        {
            if (string.IsNullOrEmpty(preset))
                return;

            string resourceName = null;
            try
            {
                string suffix = string.Concat(preset, "Filter.xml");

                Assembly assembly = Assembly.GetExecutingAssembly();
                foreach (string name in assembly.GetManifestResourceNames())
                {
                    if (name.EndsWith(suffix, StringComparison.OrdinalIgnoreCase))
                    {
                        resourceName = name;
                        break;
                    }
                }

                if (resourceName == null)
                {
                    throw new Exception(string.Format("Unknown filter preset '{0}'.", preset));
                }
            }
            catch (Exception ex)
            {
                Log.WriteError(ex.Message);
                Log.WriteError("Unable to locate embedded filter.");
                return;
            }

            int installedCount = 0;
            try
            {
                Assembly assembly = Assembly.GetExecutingAssembly();
                using (Stream filterStream = assembly.GetManifestResourceStream(resourceName))
                {
                    LoadFilterStream(ref installedCount, filterStream);
                }

                Log.WriteInfo("Installed embedded filter: {0}", installedCount);
            }
            catch (Exception ex)
            {
                Log.WriteError(ex.Message);
                Log.WriteError("Unable to load embedded filter: {0}", resourceName);
                return;
            }
        }

        /// <summary>
        /// Loads a file containing filter rules.
        /// </summary>
        public void LoadExternalFilter(string path)
        {
            int installedCount = 0;
            try
            {
                using (FileStream filterStream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read))
                {
                    LoadFilterStream(ref installedCount, filterStream);
                }

                Log.WriteInfo("Installed external filter: {0}", installedCount);
            }
            catch (Exception ex)
            {
                Log.WriteError(ex.Message);
                Log.WriteError("Unable to load external filter: {0}", path);
                return;
            }
        }

        /// <summary>
        /// Loads a filter from a stream.
        /// </summary>
        /// <param name="installedCount">Tracks the number of installed filter rules.</param>
        /// <param name="filterStream">Stream containing filter rules.</param>
        private void LoadFilterStream(ref int installedCount, Stream filterStream)
        {
            XmlDocument filter = new XmlDocument();
            filter.Load(filterStream);

            foreach (XmlNode node in filter.SelectNodes("/filter/tables/*"))
            {
                bool installed;
                try
                {
                    installed = InstallTableCommand(node);
                }
                catch (Exception ex)
                {
                    Log.WriteError(ex.Message);
                    installed = false;
                }

                if (!installed)
                {
                    Log.WriteError("Skipped rule: {0}", node.OuterXml);
                }
                else
                {
                    //Log.WriteDebug("Command installed: {0}", node.OuterXml);
                    installedCount++;
                }
            }

            foreach (XmlNode node in filter.SelectNodes("/filter/columns/*"))
            {
                bool installed;
                try
                {
                    installed = InstallColumnCommand(node);
                }
                catch (Exception ex)
                {
                    Log.WriteError(ex.Message);
                    installed = false;
                }

                if (!installed)
                {
                    Log.WriteError("Skipped rule: {0}", node.OuterXml);
                }
                else
                {
                    //Log.WriteDebug("Command installed: {0}", node.OuterXml);
                    installedCount++;
                }
            }
        }

        /// <summary>
        /// Gets the filter type corresponding to the given node.
        /// </summary>
        /// <param name="node">Node to examine.</param>
        /// <returns>The filter type corresponding to the given node.</returns>
        private DataFilterType GetFilterType(XmlNode node)
        {
            switch (node.Name.ToLowerInvariant())
            {
                case "allow":
                    return DataFilterType.Allow;
                case "deny":
                    return DataFilterType.Deny;
                default:
                    return DataFilterType.Unknown;
            }
        }

        /// <summary>
        /// Installs a single table rule from its XML representation.
        /// </summary>
        /// <param name="node">Command XML node.</param>
        private bool InstallTableCommand(XmlNode node)
        {
            DataFilterType type = GetFilterType(node);
            string tableNameRegex = SafeGetAttributeValue(node, "tableName");

            if (type == DataFilterType.Unknown || string.IsNullOrEmpty(tableNameRegex))
                return false;

            TableFilterRule rule = new TableFilterRule(type, tableNameRegex);
            tableFilter.Add(rule);
            return true;
        }

        /// <summary>
        /// Installs a single column rule from its XML representation.
        /// </summary>
        /// <param name="node">Command XML node.</param>
        private bool InstallColumnCommand(XmlNode node)
        {
            DataFilterType type = GetFilterType(node);
            string tableNameRegex = SafeGetAttributeValue(node, "tableName");
            string columnNameRegex = SafeGetAttributeValue(node, "columnName");

            if (type == DataFilterType.Unknown || string.IsNullOrEmpty(tableNameRegex) || string.IsNullOrEmpty(columnNameRegex))
                return false;

            ColumnFilterCommand rule = new ColumnFilterCommand(type, tableNameRegex, columnNameRegex);
            columnFilter.Add(rule);
            return true;
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
        /// Filters a table based on its name.
        /// </summary>
        /// <param name="tableName">Table name to filter.</param>
        /// <returns>The result of the filtering.</returns>
        public DataFilterType FilterTable(string tableName)
        {
            DataFilterType result = DataFilterType.Unknown;
            foreach (TableFilterRule rule in tableFilter)
            {
                if (RegexSupport.CheckMatch(rule.TableNameRegex, tableName))
                {
                    result = rule.Type;
                }
            }
            return result;
        }

        /// <summary>
        /// Filters a column based on its name and the name of the table containing it.
        /// </summary>
        /// <param name="tableName">Table name to filter.</param>
        /// <param name="columnName">Column name to filter.</param>
        /// <returns>The result of the filtering.</returns>
        public DataFilterType FilterColumn(string tableName, string columnName)
        {
            DataFilterType result = DataFilterType.Unknown;
            foreach (ColumnFilterCommand rule in columnFilter)
            {
                if (RegexSupport.CheckMatch(rule.TableNameRegex, tableName) && RegexSupport.CheckMatch(rule.ColumnNameRegex, columnName))
                {
                    result = rule.Type;
                }
            }
            return result;
        }

        /// <summary>
        /// List of installed table filter rules.
        /// </summary>
        private List<TableFilterRule> tableFilter = new List<TableFilterRule>();

        /// <summary>
        /// List of installed column filter rules.
        /// </summary>
        private List<ColumnFilterCommand> columnFilter = new List<ColumnFilterCommand>();

        /// <summary>
        /// Represents a single table filter rule.
        /// </summary>
        private class TableFilterRule
        {

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="type">Type of rule.</param>
            /// <param name="tableNameRegex">Regular expression to match against table names.</param>
            public TableFilterRule(DataFilterType type, string tableNameRegex)
            {
                this.Type = type;
                this.TableNameRegex = RegexSupport.GetRegex(tableNameRegex);
            }

            /// <summary>
            /// Gets the filter rule type.
            /// </summary>
            public DataFilterType Type { get; private set; }

            /// <summary>
            /// Gets the regular expression to match against table names.
            /// </summary>
            public Regex TableNameRegex { get; private set; }
        }

        /// <summary>
        /// Represents a single column filter rule.
        /// </summary>
        private class ColumnFilterCommand
        {

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="type">Type of rule.</param>
            /// <param name="tableNameRegex">Regular expression to match against table names.</param>
            /// <param name="columnNameRegex">Regular expression to match against column names.</param>
            public ColumnFilterCommand(DataFilterType type, string tableNameRegex, string columnNameRegex)
            {
                this.Type = type;
                this.TableNameRegex = RegexSupport.GetRegex(tableNameRegex);
                this.ColumnNameRegex = RegexSupport.GetRegex(columnNameRegex);
            }

            /// <summary>
            /// Gets the filter rule type.
            /// </summary>
            public DataFilterType Type { get; private set; }

            /// <summary>
            /// Gets the regular expression to match against table names.
            /// </summary>
            public Regex TableNameRegex { get; private set; }

            /// <summary>
            /// Gets the regular expression to match against column names.
            /// </summary>
            public Regex ColumnNameRegex { get; private set; }
        }
    }


    /// <summary>
    /// Enumeration of data filter types.
    /// </summary>
    public enum DataFilterType
    {

        /// <summary>
        /// Unknown.
        /// </summary>
        Unknown,

        /// <summary>
        /// Allow.
        /// </summary>
        Allow,

        /// <summary>
        /// Deny.
        /// </summary>
        Deny
    }
}
