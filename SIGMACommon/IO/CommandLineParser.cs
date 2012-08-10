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
using System.Linq;
using System.Reflection;
using System.Text;

namespace Sigma.Common.IO
{

    /// <summary>
    /// Manages the parsing of command line arguments.
    /// </summary>
    public static class CommandLineParser
    {

        /// <summary>
        /// Gets a list of expected command line arguments for the given data object.
        /// </summary>
        /// <param name="dataObject">The data object.</param>
        /// <returns>List of expected command line arguments.</returns>
        private static IList<CommandLineMemberInfo> Examine(object dataObject)
        {
            Type type = dataObject.GetType();
            return Examine(type);
        }

        /// <summary>
        /// Gets a list of expected command line arguments for the given data object.
        /// </summary>
        /// <param name="type">The data object type.</param>
        /// <returns>List of expected command line arguments.</returns>
        private static IList<CommandLineMemberInfo> Examine(Type type)
        {
            List<CommandLineMemberInfo> result = new List<CommandLineMemberInfo>();
            foreach (MemberInfo member in type.GetMembers())
            {
                CommandLineAttribute[] attributes = (CommandLineAttribute[])member.GetCustomAttributes(typeof(CommandLineAttribute), true);
                if (attributes == null || attributes.Length == 0)
                    continue;

                CommandLineMemberInfo info = new CommandLineMemberInfo();
                info.MemberInfo = member;
                info.ShortArgumentName = attributes[0].ShortName;
                info.LongArgumentName = attributes[0].LongName;
                info.HelpText = attributes[0].HelpText;
                info.Required = attributes[0].Required;

                if (member is FieldInfo)
                {
                    FieldInfo fieldInfo = (FieldInfo)member;
                    info.ArgumentType = fieldInfo.FieldType;
                }
                else if (member is PropertyInfo)
                {
                    PropertyInfo propertyInfo = (PropertyInfo)member;
                    info.ArgumentType = propertyInfo.PropertyType;
                }

                result.Add(info);                
            }

            result.Sort();
            return result;
        }

        /// <summary>
        /// Parses command line arguments into the given data object.
        /// </summary>
        /// <remarks>
        /// Members of the given data object should be marked with instances of CommandLineAttribute to define parsing behaviour.
        /// </remarks>
        /// <param name="dataObject">The data object.</param>
        /// <param name="args">Arguments to parse.</param>
        public static void Parse(object dataObject, string[] args)
        {
            ICommandLineArguments helper = dataObject as ICommandLineArguments;

            IList<CommandLineMemberInfo> members = Examine(dataObject);
            IList<CommandLineMemberInfo> requiredMembers = new List<CommandLineMemberInfo>(members.Where(m => m.Required));

            Dictionary<string, CommandLineMemberInfo> shortNames = new Dictionary<string, CommandLineMemberInfo>();
            Dictionary<string, CommandLineMemberInfo> longNames = new Dictionary<string, CommandLineMemberInfo>();

            foreach (CommandLineMemberInfo member in members)
            {
                shortNames[member.ShortArgumentName] = member;
                longNames[member.LongArgumentName] = member;
            }

            if (helper != null)
                helper.Initialize();

            for (int i = 0; i < args.Length; i++)
            {
                string name = args[i].ToLowerInvariant();

                CommandLineMemberInfo member;
                if (name.StartsWith("--"))
                {
                    string longName = name.Substring(2);
                    longNames.TryGetValue(longName, out member);
                }
                else if (name.StartsWith("-"))
                {
                    string shortName = name.Substring(1);
                    shortNames.TryGetValue(shortName, out member);
                }
                else
                {
                    member = null;
                }

                if (member == null)
                {
                    throw new Exception(string.Format("Unrecognized command line option: {0}", name));
                }

                try
                {
                    if (member.ArgumentType == typeof(bool))
                    {
                        SetValue(dataObject, member.MemberInfo, true);
                    }
                    else if (member.ArgumentType == typeof(int) || member.ArgumentType == typeof(long))
                    {
                        long value = long.Parse(args[++i]);
                        SetValue(dataObject, member.MemberInfo, true);
                    }
                    else if (member.ArgumentType == typeof(uint) || member.ArgumentType == typeof(ulong))
                    {
                        ulong value = ulong.Parse(args[++i]);
                        SetValue(dataObject, member.MemberInfo, true);
                    }
                    else if (member.ArgumentType == typeof(string))
                    {
                        SetValue(dataObject, member.MemberInfo, args[++i]);
                    }
                }
                catch
                {
                    throw new Exception(string.Format("Badly formatted command line option: {0}", name));
                }

                requiredMembers.Remove(member);
            }

            if (requiredMembers.Count > 0)
            {
                StringBuilder missingArguments = new StringBuilder();
                missingArguments.Append("Missing required command line option(s): ");
                for (int i = 0; i < requiredMembers.Count; i++)
                {
                    if (i > 0)
                        missingArguments.Append(", ");

                    CommandLineMemberInfo member = requiredMembers[i];
                    missingArguments.AppendFormat("--{0} (-{1})", member.LongArgumentName, member.ShortArgumentName);
                }

                throw new Exception(missingArguments.ToString());
            }

            if (helper != null)
                helper.Validate();
        }

        /// <summary>
        /// Sets the value of a member of a data object.
        /// </summary>
        /// <param name="dataObject">Data object.</param>
        /// <param name="member">Member to set.</param>
        /// <param name="value">The value to set.</param>
        private static void SetValue(object dataObject, MemberInfo member, object value)
        {
            if (member is FieldInfo)
            {
                FieldInfo fieldInfo = (FieldInfo)member;
                value = Convert.ChangeType(value, fieldInfo.FieldType);
                fieldInfo.SetValue(dataObject, value);
            }
            else if (member is PropertyInfo)
            {
                PropertyInfo propertyInfo = (PropertyInfo)member;
                value = Convert.ChangeType(value, propertyInfo.PropertyType);
                propertyInfo.SetValue(dataObject, value, null);
            }
            else
            {
                throw new ArgumentException();
            }
        }

        /// <summary>
        /// Generates help text for the given data object.
        /// </summary>
        /// <remarks>
        /// Members of the giveen data object should be marked with instances of CommandLineAttribute to define parsing behaviour.
        /// </remarks>
        /// <param name="dataObject">The data object.</param>
        /// <param name="executableName">Name of the executable to generate the help text for.</param>
        /// <returns>Help text.</returns>
        public static string HelpText(object dataObject, string executableName)
        {
            Type type = dataObject.GetType();
            return HelpText(type, executableName);
        }

        /// <summary>
        /// Generates help text for the given data object.
        /// </summary>
        /// <remarks>
        /// Members of the given data object should be marked with instances of CommandLineAttribute to define parsing behaviour.
        /// </remarks>
        /// <param name="dataObjectType">Runtime type of the data object.</param>
        /// <param name="executableName">Name of the executable to generate the help text for.</param>
        /// <returns>Help text.</returns>
        public static string HelpText(Type dataObjectType, string executableName)
        {
            IList<CommandLineMemberInfo> members = Examine(dataObjectType);

            IList<CommandLineMemberInfo> requiredMembers = new List<CommandLineMemberInfo>(members.Where(m => m.Required));
            IList<CommandLineMemberInfo> optionalMembers = new List<CommandLineMemberInfo>(members.Where(m => !m.Required));

            StringBuilder builder = new StringBuilder();
            builder.AppendFormat(string.Format("Usage: {0}", executableName));
            
            foreach (CommandLineMemberInfo member in requiredMembers)
            {
                builder.AppendLine();
                if (member.ArgumentType == typeof(bool))
                {
                    builder.AppendFormat("    --{0}|-{1}", member.LongArgumentName, member.ShortArgumentName);
                }
                else
                {
                    builder.AppendFormat("    --{0}|-{1} <{1}value>", member.LongArgumentName, member.ShortArgumentName);
                }
            }

            foreach (CommandLineMemberInfo member in optionalMembers)
            {
                builder.AppendLine();
                if (member.ArgumentType == typeof(bool))
                {
                    builder.AppendFormat("    [--{0}|-{1}]", member.LongArgumentName, member.ShortArgumentName);
                }
                else
                {
                    builder.AppendFormat("    [--{0}|-{1} <{1}value>]", member.LongArgumentName, member.ShortArgumentName);
                }
            }

            builder.AppendLine();

            if (requiredMembers.Count > 0)
            {
                foreach (CommandLineMemberInfo member in requiredMembers)
                {
                    builder.AppendLine();
                    if (member.ArgumentType == typeof(bool))
                    {
                        builder.AppendFormat("--{0} (-{1})", member.LongArgumentName, member.ShortArgumentName);
                    }
                    else
                    {
                        builder.AppendFormat("--{0} <value> (-{1})", member.LongArgumentName, member.ShortArgumentName);
                    }

                    builder.AppendLine();
                    builder.AppendFormat("    {0}", member.HelpText);
                    builder.AppendLine();
                }
            }

            if (optionalMembers.Count > 0)
            {
                foreach (CommandLineMemberInfo member in optionalMembers)
                {
                    builder.AppendLine(); 
                    if (member.ArgumentType == typeof(bool))
                    {
                        builder.AppendFormat("--{0} (-{1})", member.LongArgumentName, member.ShortArgumentName);
                    }
                    else
                    {
                        builder.AppendFormat("--{0} <value> (-{1})", member.LongArgumentName, member.ShortArgumentName);
                    }

                    builder.AppendLine();
                    builder.AppendFormat("    {0}", member.HelpText);
                    builder.AppendLine();
                }
            }

            return builder.ToString();
        }

        /// <summary>
        /// Encapsulates information about an expected command line argument.
        /// </summary>
        private class CommandLineMemberInfo : IComparable<CommandLineMemberInfo>
        {

            /// <summary>
            /// Long argument name (e.g. "argumentname")
            /// </summary>
            public string LongArgumentName { get; set; }

            /// <summary>
            /// Short argument name (e.g. "an")
            /// </summary>
            public string ShortArgumentName { get; set; }

            /// <summary>
            /// Argument help text.
            /// </summary>
            public string HelpText { get; set; }

            /// <summary>
            /// True if the argument is required, false otherwise.
            /// </summary>
            public bool Required { get; set; }

            /// <summary>
            /// Runtime argument type.
            /// </summary>
            public Type ArgumentType { get; set; }

            /// <summary>
            /// The member that holds the argument value.
            /// </summary>
            public MemberInfo MemberInfo { get; set; }

            /// <summary>
            /// Compares the current object with another object of the same type.
            /// </summary>
            /// <param name="other">An object to compare with this object.</param>
            /// <returns>
            /// A 32-bit signed integer that indicates the relative order of the objects being compared.
            /// </returns>
            public int CompareTo(CommandLineMemberInfo other)
            {
                return LongArgumentName.CompareTo(other.LongArgumentName);
            }
        }
    }
}
