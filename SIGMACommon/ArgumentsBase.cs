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
    /// Base class for command line argument encapsulation types.
    /// </summary>
    public abstract class ArgumentsBase : ICommandLineArguments
    {

        /// <summary>
        /// Show usage information.
        /// </summary>
        [CommandLine("h", "help", "Display this usage information.")]
        public bool Help { get; set; }

        /// <summary>
        /// Initializes the data object.
        /// </summary>
        public virtual void Initialize()
        {
        }

        /// <summary>
        /// Validates arguments after parsing.
        /// </summary>
        public virtual void Validate()
        {
        }
    }
}
