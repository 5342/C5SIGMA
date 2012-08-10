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
    /// This attribute is used to mark members of types that can be automatically populated from the command line.
    /// </summary>
    [AttributeUsage(AttributeTargets.Field | AttributeTargets.Property, AllowMultiple = false, Inherited = true)]
    public class CommandLineAttribute : Attribute
    {

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="shortName">Short argument name (e.g. "an")</param>
        /// <param name="longName">Long argument name (e.g. "argumentname")</param>
        /// <param name="helpText">Help text.</param>
        public CommandLineAttribute(string shortName, string longName, string helpText)
            : this(shortName, longName, helpText, false)
        {
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="shortName">Short argument name (e.g. "an")</param>
        /// <param name="longName">Long argument name (e.g. "argumentname")</param>
        /// <param name="helpText">Help text.</param>
        /// <param name="required">True if the argument is required, false otherwise.</param>
        public CommandLineAttribute(string shortName, string longName, string helpText, bool required)
        {
            this.ShortName = shortName.ToLowerInvariant();
            this.LongName = longName.ToLowerInvariant();
            this.HelpText = helpText;
            this.Required = required;
        }

        /// <summary>
        /// Gets the short argument name (e.g. "an").
        /// </summary>
        public string ShortName { get; private set; }

        /// <summary>
        /// Gets the long argument name (e.g. "argumentname").
        /// </summary>
        public string LongName { get; private set; }

        /// <summary>
        /// Gets the help text.
        /// </summary>
        public string HelpText { get; private set; }

        /// <summary>
        /// Gets or sets whether the command line argument is required.
        /// </summary>
        public bool Required { get; private set; }
    }
}
