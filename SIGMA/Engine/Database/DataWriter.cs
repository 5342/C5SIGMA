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
using Sigma.Engine.TShark;

namespace Sigma.Engine.Database
{

    /// <summary>
    /// Base class for data writer implementations.
    /// </summary>
    internal abstract class DataWriter : IDisposable
    {

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="type">This instance's type.</param>
        public DataWriter(DataWriterType type)
        {
            this.Type = type;
        }

        /// <summary>
        /// Gets this instance's type.
        /// </summary>
        public DataWriterType Type { get; private set; }

        /// <summary>
        /// Gets or sets the filter to be used by this writer.
        /// </summary>
        public DataFilter Filter { get; set; }

        /// <summary>
        /// Writes a single data row.
        /// </summary>
        /// <param name="row">Row to write.</param>
        public abstract void WriteRow(CapDataRow row);

        /// <summary>
        /// Flushes this instance.
        /// </summary>
        protected abstract void Flush();

        /// <summary>
        /// Disposes of this instance.
        /// </summary>
        public virtual void Dispose()
        {
            Flush();
        }
    }

    /// <summary>
    /// Enumeration of DataWriter types.
    /// </summary>
    internal enum DataWriterType
    {

        /// <summary>
        /// SQL Server data writer.
        /// </summary>
        SqlServer,

        /// <summary>
        /// MySQL data writer.
        /// </summary>
        MySql
    }
}
