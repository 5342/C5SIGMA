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
using System.Net;

namespace Sigma.Engine.TShark
{

    /// <summary>
    /// Holds a parsed model of TShark's data (protocol/field/string) schema.
    /// </summary>
    internal class TSharkDataSchema
    {

        /// <summary>
        /// Constructor.
        /// </summary>
        public TSharkDataSchema()
        {
            Protocols = new Dictionary<string, TSharkProtocol>();
            Fields = new Dictionary<string, TSharkField>();
            Strings = new Dictionary<string, TSharkString>();
        }

        /// <summary>
        /// Gets the dictionary of registered protocols.
        /// </summary>
        public IDictionary<string, TSharkProtocol> Protocols { get; private set; }

        /// <summary>
        /// Gets the dictionary of registered fields.
        /// </summary>
        public IDictionary<string, TSharkField> Fields { get; private set; }

        /// <summary>
        /// Gets the dictionary of registered strings.
        /// </summary>
        public IDictionary<string, TSharkString> Strings { get; private set; }

        /// <summary>
        /// Adds a protocol to the schema.
        /// </summary>
        /// <param name="longName">Protocol long name.</param>
        /// <param name="shortName">Protocol short name.</param>
        /// <param name="filterName">Protocol filter name.</param>
        public void AddProtocol(string longName, string shortName, string filterName)
        {
            TSharkProtocol existing;
            if (Protocols.TryGetValue(shortName, out existing))
            {
                existing.Merge(longName, shortName, filterName);
            }
            else
            {
                Protocols[shortName] = new TSharkProtocol(longName, shortName, filterName);
            }
        }

        /// <summary>
        /// Adds a field to the schema.
        /// </summary>
        /// <param name="longName">Field long name.</param>
        /// <param name="shortName">Field short name.</param>
        /// <param name="ftenumType">Field type (e.g. FT_INT32, FT_STRING, etc)</param>
        /// <param name="protocolShortName">Parent protocol's short name.</param>
        /// <param name="description">Description of the field.</param>
        /// <param name="displayBase">Display base of the field (e.g. BASE_DEC, BASE_HEX, BASE_DEC).</param>
        /// <param name="bitmask">Bitmask of the field.</param>
        public void AddField(string longName, string shortName, string ftenumType, string protocolShortName, string description, string displayBase, string bitmask)
        {
            TSharkProtocol protocol = GetProtocol(protocolShortName);

            Type type = TranslateFtenum(ftenumType);
            TSharkField existing;
            if (Fields.TryGetValue(shortName, out existing))
            {
                existing.Merge(protocol, longName, shortName, description, displayBase, bitmask, type);
            }
            else
            {
                Fields[shortName] = new TSharkField(protocol, longName, shortName, description, displayBase, bitmask, type);
            }
        }

        /// <summary>
        /// Translates a string type name (e.g. FT_INT32, FT_STRING, etc) into a corresponding runtime type.
        /// </summary>
        /// <param name="ftenumType">Type name to translate.</param>
        /// <returns>The corresponding runtime type.</returns>
        private static Type TranslateFtenum(string ftenumType)
        {
            switch (ftenumType.ToUpperInvariant())
            {
                case "FT_NONE":
                    return typeof(string);
                case "FT_PROTOCOL":
                    return typeof(string);
                case "FT_BOOLEAN":
                    return typeof(bool);
                case "FT_UINT8":
                    return typeof(byte);
                case "FT_UINT16":
                    return typeof(ushort);
                case "FT_UINT24":
                case "FT_UINT32":
                    return typeof(uint);
                case "FT_UINT64":
                    return typeof(ulong);
                case "FT_INT8":
                    return typeof(sbyte);
                case "FT_INT16":
                    return typeof(short);
                case "FT_INT24":
                case "FT_INT32":
                    return typeof(int);
                case "FT_INT64":
                    return typeof(long);
                case "FT_FLOAT":
                    return typeof(float);
                case "FT_DOUBLE":
                    return typeof(double);
                case "FT_ABSOLUTE_TIME":
                    return typeof(DateTime);
                case "FT_RELATIVE_TIME":
                    return typeof(TimeSpan);
                case "FT_STRING":
                case "FT_STRINGZ":
                case "FT_EBCDIC":
                case "FT_UINT_STRING":
                    return typeof(string);
                case "FT_ETHER":
                    return typeof(string);
                case "FT_BYTES":
                case "FT_UINT_BYTES":
                    return typeof(byte[]);
                case "FT_IPV4":
                case "FT_IPV6":
                    return typeof(IPAddress);
                case "FT_IPXNET":
                    return typeof(string);
                case "FT_FRAMENUM":
                    return typeof(uint);
                case "FT_PCRE":
                    return typeof(string);
                case "FT_GUID":
                    return typeof(Guid);
                case "FT_OID":
                    return typeof(string);
                case "FT_EUI64":
                    return typeof(ulong);
                default:
                case "FT_NUM_TYPES":
                    throw new NotSupportedException(string.Format("Field type not supported: {0}", ftenumType));
            }
        }

        /// <summary>
        /// Adds a string to the schema.
        /// </summary>
        /// <param name="fieldShortName">Parent field short name.</param>
        /// <param name="value">Value the string corresponds to.</param>
        /// <param name="valueString">Human readable string.</param>
        public void AddValueString(string fieldShortName, long value, string valueString)
        {
            TSharkField field = GetField(fieldShortName);

            string key = string.Concat(fieldShortName, "(", value, ")");

            TSharkString existing;
            if (Strings.TryGetValue(key, out existing))
            {
                TSharkStringValue existingStringValue = (TSharkStringValue)existing;
                existingStringValue.Merge(valueString);
            }
            else
            {
                Strings[key] = new TSharkStringValue(field, value, valueString);
            }
        }

        /// <summary>
        /// Adds a string to the schema.
        /// </summary>
        /// <param name="fieldShortName">Parent field short name.</param>
        /// <param name="lowerBound">Lower bound for values the string corresponds to.</param>
        /// <param name="upperBound">Upper bound for values the string corresponds to.</param>
        /// <param name="rangeString">Human readable string.</param>
        public void AddRangeString(string fieldShortName, long lowerBound, long upperBound, string rangeString)
        {
            TSharkField field = GetField(fieldShortName);

            string key = string.Concat(fieldShortName, "(", lowerBound, "-", upperBound, ")");

            TSharkString existing;
            if (Strings.TryGetValue(key, out existing))
            {
                TSharkStringRange existingStringRange = (TSharkStringRange)existing;
                existingStringRange.Merge(rangeString);
            }
            else
            {
                Strings[key] = new TSharkStringRange(field, lowerBound, upperBound, rangeString);
            }
        }

        /// <summary>
        /// Adds a string to the schema.
        /// </summary>
        /// <param name="fieldShortName">Parent field short name.</param>
        /// <param name="trueString">Human readable string corresponding to a value of true.</param>
        /// <param name="falseString">Human readable string corresponding to a value of false.</param>
        public void AddTrueFalseString(string fieldShortName, string trueString, string falseString)
        {
            TSharkField field = GetField(fieldShortName);

            string key = string.Concat(fieldShortName, "(t/f)");

            TSharkString existing;
            if (Strings.TryGetValue(key, out existing))
            {
                TSharkStringTrueFalse existingStringTrueFalse = (TSharkStringTrueFalse)existing;
                existingStringTrueFalse.Merge(trueString, falseString);
            }
            else
            {
                Strings[key] = new TSharkStringTrueFalse(field, trueString, falseString);
            }
        }

        /// <summary>
        /// Gets a registered protocol by its short name.
        /// </summary>
        /// <param name="protocolShortName">Short name of the protocol to get.</param>
        /// <returns>The registered protocol with the given short name.</returns>
        private TSharkProtocol GetProtocol(string protocolShortName)
        {
            string protocolKey = protocolShortName;
            TSharkProtocol protocol;
            if (!Protocols.TryGetValue(protocolKey, out protocol))
            {
                throw new Exception(string.Format("Unknown protocol: {0}", protocolShortName));
            }
            return protocol;
        }

        /// <summary>
        /// Gets a registered field by its short name.
        /// </summary>
        /// <param name="fieldShortName">Short name of the field to get.</param>
        /// <returns>The registered field with the given short name.</returns>
        private TSharkField GetField(string fieldShortName)
        {
            string fieldKey = fieldShortName;
            TSharkField field;
            if (!Fields.TryGetValue(fieldKey, out field))
            {
                throw new Exception(string.Format("Unknown field: {0}", fieldShortName));
            }
            return field;
        }
    }

    /// <summary>
    /// Encapsulates information about a TShark protocol.
    /// </summary>
    internal class TSharkProtocol
    {

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="longName">Protocol long name.</param>
        /// <param name="shortName">Protocol short name.</param>
        /// <param name="filterName">Protocol filter name.</param>
        public TSharkProtocol(string longName, string shortName, string filterName)
        {
            this.LongName = longName;
            this.ShortName = shortName;
            this.FilterName = filterName;
            this.Fields = new List<TSharkField>();
        }

        /// <summary>
        /// Gets the protocol's long name.
        /// </summary>
        public string LongName { get; private set; }

        /// <summary>
        /// Gets the protocol's short name.
        /// </summary>
        public string ShortName { get; private set; }

        /// <summary>
        /// Gets the protocol's filter name.
        /// </summary>
        public string FilterName { get; private set; }

        /// <summary>
        /// Gets the list of field registered for this protocol.
        /// </summary>
        public IList<TSharkField> Fields { get; private set; }

        /// <summary>
        /// Merges the given values into this instance.
        /// </summary>
        /// <param name="longName">Protocol long name.</param>
        /// <param name="shortName">Protocol short name.</param>
        /// <param name="filterName">Protocol filter name.</param>
        public void Merge(string longName, string shortName, string filterName)
        {
            if (!ShortName.Equals(shortName, StringComparison.OrdinalIgnoreCase))
                throw new Exception(string.Format("Unable to merge duplicate protocols (short name conflict): {0}", shortName));

            if (!string.IsNullOrEmpty(FilterName) && !string.IsNullOrEmpty(filterName) && !FilterName.Equals(filterName, StringComparison.OrdinalIgnoreCase))
                throw new Exception(string.Format("Unable to merge duplicate protocols (filter name conflict): {0}", shortName));

            if (string.IsNullOrEmpty(LongName))
            {
                LongName = longName;
            }
            else if (!longName.Equals(LongName, StringComparison.OrdinalIgnoreCase))
            {
                LongName = string.Concat(LongName, " / ", longName);
            }

            if (string.IsNullOrEmpty(FilterName))
            {
                FilterName = filterName;
            }
        }
    }

    /// <summary>
    /// Encapsulates information about a TShark field.
    /// </summary>
    internal class TSharkField
    {

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="protocol">Parent protocol.</param>
        /// <param name="longName">Field long name.</param>
        /// <param name="shortName">Field short name.</param>
        /// <param name="description">Field description.</param>
        /// <param name="displayBase">Field display base.</param>
        /// <param name="bitmask">Field bitmask.</param>
        /// <param name="type">Field type.</param>
        public TSharkField(TSharkProtocol protocol, string longName, string shortName, string description, string displayBase, string bitmask, Type type)
        {
            this.Protocol = protocol;
            this.LongName = longName;
            this.ShortName = shortName;
            this.Description = description;
            this.DisplayBase = displayBase;
            this.Bitmask = bitmask;
            this.Type = type;
            this.Strings = new List<TSharkString>();

            protocol.Fields.Add(this);
        }

        /// <summary>
        /// Gets the parent protocol.
        /// </summary>
        public TSharkProtocol Protocol { get; private set; }

        /// <summary>
        /// Gets the field long name.
        /// </summary>
        public string LongName { get; private set; }

        /// <summary>
        /// Gets the field short name.
        /// </summary>
        public string ShortName { get; private set; }

        /// <summary>
        /// Gets the field description.
        /// </summary>
        public string Description { get; private set; }

        /// <summary>
        /// Gets the field display base.
        /// </summary>
        public string DisplayBase { get; private set; }

        /// <summary>
        /// Gets the field bitmask.
        /// </summary>
        public string Bitmask { get; private set; }

        /// <summary>
        /// Gets the field runtime type.
        /// </summary>
        public Type Type { get; private set; }

        /// <summary>
        /// Gets the list of string registered for this field's values.
        /// </summary>
        public IList<TSharkString> Strings { get; private set; }

        /// <summary>
        /// Merges the given values into this instance.
        /// </summary>
        /// <param name="protocol">Parent protocol.</param>
        /// <param name="longName">Field long name.</param>
        /// <param name="shortName">Field short name.</param>
        /// <param name="description">Field description.</param>
        /// <param name="displayBase">Field display base.</param>
        /// <param name="bitmask">Field bitmask.</param>
        /// <param name="type">Field type.</param>
        public void Merge(TSharkProtocol protocol, string longName, string shortName, string description, string displayBase, string bitmask, Type type)
        {
            if (Protocol != protocol)
                throw new Exception(string.Format("Unable to merge duplicate fields (protocol conflict): {0}", shortName));
            
            if (!ShortName.Equals(shortName, StringComparison.OrdinalIgnoreCase))
                throw new Exception(string.Format("Unable to merge duplicate fields (short name conflict): {0}", shortName));
            
            if (Type != null && type != null && Type != type)
                throw new Exception(string.Format("Unable to merge duplicate fields (type conflict): {0}", shortName));

            if (Type == null)
                Type = type;

            if (string.IsNullOrEmpty(Bitmask))
            {
                Bitmask = bitmask;
            }
            else if (!bitmask.Equals(Bitmask, StringComparison.OrdinalIgnoreCase))
            {
                Bitmask = string.Concat(Bitmask, " / ", bitmask);
            }
            
            if (string.IsNullOrEmpty(LongName))
            {
                LongName = longName;
            }
            else if (!longName.Equals(LongName, StringComparison.OrdinalIgnoreCase))
            {
                LongName = string.Concat(LongName, " / ", longName);
            }

            if (string.IsNullOrEmpty(Description))
            {
                Description = description;
            }
            else if (!description.Equals(Description, StringComparison.OrdinalIgnoreCase))
            {
                Description = string.Concat(Description, " / ", description);
            }

            if (string.IsNullOrEmpty(DisplayBase))
            {
                DisplayBase = displayBase;
            }
            else if (!displayBase.Equals(DisplayBase, StringComparison.OrdinalIgnoreCase))
            {
                DisplayBase = string.Concat(DisplayBase, " / ", displayBase);
            }
        }

        /// <summary>
        /// Finds a registered string that corresponds to the given field value.
        /// </summary>
        /// <param name="value">Field value.</param>
        /// <returns>Corresponding string, or null if none.</returns>
        public string FindString(object value)
        {
            if (Strings.Count == 0 || value == null)
                return null;

            if (value is bool)
            {
                bool boolValue = (bool)value;
                foreach (TSharkString s in Strings)
                {
                    TSharkStringTrueFalse tfs = s as TSharkStringTrueFalse;
                    if (tfs != null)
                    {
                        if (boolValue)
                        {
                            return tfs.TrueStringValue;
                        }
                        else
                        {
                            return tfs.FalseStringValue;
                        }
                    }
                }            
            }
            else
            {
                long longValue;
                if (!long.TryParse(Convert.ToString(value), out longValue))
                {
                    return null;
                }

                foreach (TSharkString s in Strings)
                {
                    TSharkStringRange rs = s as TSharkStringRange;
                    if (rs != null)
                    {
                        if (longValue >= rs.LowerBound && longValue <= rs.UpperBound)
                        {
                            return rs.StringValue;
                        }
                    }
                    else
                    {
                        TSharkStringValue vs = s as TSharkStringValue;
                        if (vs != null)
                        {
                            if (vs.Value == longValue)
                            {
                                return vs.StringValue;
                            }
                        }
                    }
                } 
            }

            return null;
        }
    }

    /// <summary>
    /// Base class for registered TShark string types.
    /// </summary>
    internal abstract class TSharkString
    {

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="field">Parent field.</param>
        public TSharkString(TSharkField field)
        {
            this.Field = field;

            field.Strings.Add(this);
        }

        /// <summary>
        /// Gets the parent field.
        /// </summary>
        public TSharkField Field { get; private set; }
    }

    /// <summary>
    /// Encapsulates a registered TShark single value string.
    /// </summary>
    internal class TSharkStringValue : TSharkString
    {

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="field">Parent field.</param>
        /// <param name="value">Value the string corresponds to.</param>
        /// <param name="stringValue">Human readable string.</param>
        public TSharkStringValue(TSharkField field, long value, string stringValue)
            : base(field)
        {
            this.Value = value;
            this.StringValue = stringValue;
        }

        /// <summary>
        /// Gets the value this instance corresponds to.
        /// </summary>
        public long Value { get; set; }

        /// <summary>
        /// Gets the human readable string.
        /// </summary>
        public string StringValue { get; set; }

        /// <summary>
        /// Merges the given string into this instance.
        /// </summary>
        /// <param name="stringValue">Human readable string.</param>
        public void Merge(string stringValue)
        {
            if (string.IsNullOrEmpty(StringValue))
            {
                StringValue = stringValue;
            }
            else if (!stringValue.Equals(StringValue, StringComparison.OrdinalIgnoreCase))
            {
                StringValue = string.Concat(StringValue, " / ", stringValue);
            }
        }
    }

    /// <summary>
    /// Encapsulates a registered TShark range string.
    /// </summary>
    internal class TSharkStringRange : TSharkString
    {

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="field">Parent field.</param>
        /// <param name="lowerBound">Lower bound of values the string corresponds to.</param>
        /// <param name="upperBound">Upper bound of values the string corresponds to.</param>
        /// <param name="stringValue">Human readable string.</param>
        public TSharkStringRange(TSharkField field, long lowerBound, long upperBound, string stringValue)
            : base(field)
        {
            this.LowerBound = lowerBound;
            this.UpperBound = upperBound;
            this.StringValue = stringValue;
        }

        /// <summary>
        /// Gets the lower bound of values the string corresponds to.
        /// </summary>
        public long LowerBound { get; set; }

        /// <summary>
        /// Gets the upper bound of values the string corresponds to.
        /// </summary>
        public long UpperBound { get; set; }

        /// <summary>
        /// Gets the human readable string.
        /// </summary>
        public string StringValue { get; set; }

        /// <summary>
        /// Merges the given string into this instance.
        /// </summary>
        /// <param name="stringValue">Human readable string.</param>
        public void Merge(string stringValue)
        {
            if (string.IsNullOrEmpty(StringValue))
            {
                StringValue = stringValue;
            }
            else if (!stringValue.Equals(StringValue, StringComparison.OrdinalIgnoreCase))
            {
                StringValue = string.Concat(StringValue, " / ", stringValue);
            }
        }
    }

    /// <summary>
    /// Encapsulates a registered TShark true/false string.
    /// </summary>
    internal class TSharkStringTrueFalse : TSharkString
    {

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="field">Parent field.</param>
        /// <param name="trueStringValue">The true string value.</param>
        /// <param name="falseStringValue">The false string value.</param>
        public TSharkStringTrueFalse(TSharkField field, string trueStringValue, string falseStringValue)
            : base(field)
        {
            this.TrueStringValue = trueStringValue;
            this.FalseStringValue = falseStringValue;
        }

        /// <summary>
        /// Gets the true string value.
        /// </summary>
        public string TrueStringValue { get; set; }

        /// <summary>
        /// Gets the false string value.
        /// </summary>
        public string FalseStringValue { get; set; }

        /// <summary>
        /// Merges the given values into this instance.
        /// </summary>
        /// <param name="trueStringValue">The true string value.</param>
        /// <param name="falseStringValue">The false string value.</param>
        public void Merge(string trueStringValue, string falseStringValue)
        {
            if (string.IsNullOrEmpty(TrueStringValue))
            {
                TrueStringValue = trueStringValue;
            }
            else if (!trueStringValue.Equals(TrueStringValue, StringComparison.OrdinalIgnoreCase))
            {
                TrueStringValue = string.Concat(TrueStringValue, " / ", trueStringValue);
            }

            if (string.IsNullOrEmpty(FalseStringValue))
            {
                FalseStringValue = falseStringValue;
            }
            else if (!falseStringValue.Equals(FalseStringValue, StringComparison.OrdinalIgnoreCase))
            {
                FalseStringValue = string.Concat(FalseStringValue, " / ", falseStringValue);
            }
        }
    }
}
