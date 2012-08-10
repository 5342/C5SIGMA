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

using System.Collections.Generic;
using System.IO;

namespace Sigma.Common.Support
{

    /// <summary>
    /// Provides helper methods that support interaction with the file system.
    /// </summary>
    public static class FileSystemSupport
    {

        /// <summary>
        /// Recursively scans the given directory for files.
        /// </summary>
        /// <remarks>
        /// This method uses yield to avoid pre-caching the whole directory structure.
        /// </remarks>
        /// <param name="path">Path to scan.</param>
        /// <param name="pattern">Optional pattern to match (this may be multiple patterns separated by a pipe '|' character).</param>
        /// <returns>An enumeration of files available in the given directory matching the pattern (if specified).</returns>
        public static IEnumerable<string> RecursiveScan(string path, string pattern)
        {
            Queue<string> directories = new Queue<string>();
            directories.Enqueue(path);

            string[] patternParts;
            if (string.IsNullOrEmpty(pattern))
            {
                patternParts = null;
            }
            else
            {
                patternParts = pattern.Split('|');
            }

            while (directories.Count > 0)
            {
                string currentPath = directories.Dequeue();

                List<string> files = new List<string>();
                try
                {
                    if (patternParts == null)
                    {
                        files.AddRange(Directory.GetFiles(currentPath));
                    }
                    else
                    {
                        foreach (string part in patternParts)
                        {
                            files.AddRange(Directory.GetFiles(currentPath, part));
                        }
                    }

                    foreach (string directory in Directory.GetDirectories(currentPath))
                    {
                        directories.Enqueue(directory);
                    }
                }
                catch
                {
                    files = null;
                }

                if (files != null)
                {
                    foreach (string file in files)
                    {
                        yield return file;
                    }
                }
            }
        }
    }
}
