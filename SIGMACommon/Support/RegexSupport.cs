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
using System.Text;
using System.Text.RegularExpressions;

namespace Sigma.Common.Support
{

    /// <summary>
    /// Provides helper methods for constructing and running regular expressions.
    /// </summary>
    public static class RegexSupport
    {

        /// <summary>
        /// Regex that matches all strings.
        /// </summary>
        private static readonly Regex MatchAllRegex = new Regex(".*");

        /// <summary>
        /// Gets or creates a regular expression object for the given regular expression pattern.
        /// </summary>
        /// <param name="pattern">Pattern to create a regular expression object for.</param>
        /// <returns>A regular expression object that parses the given pattern.</returns>
        public static Regex GetRegex(string pattern)
        {
            if (string.IsNullOrEmpty(pattern))
            {
                return null;
            }
            else if (pattern == ".*")
            {
                return MatchAllRegex;
            }
            else
            {
                return new Regex(pattern);
            }
        }

        /// <summary>
        /// Checks if the given regex matches the given value.
        /// </summary>
        /// <remarks>
        /// Use '^' and '$' in the regular expression to anchor the expression to the input start/end for a match that consumes the whole input.
        /// </remarks>
        /// <param name="regex">Regex to apply.</param>
        /// <param name="value">Value to match against.</param>
        /// <returns>True if matched, false otherwise.</returns>
        public static bool CheckMatch(Regex regex, string value)
        {
            IList<KeyValuePair<string, string>> captures = null;
            return CheckMatch(regex, value, ref captures);
        }

        /// <summary>
        /// Checks if the given regex matches the given value.
        /// </summary>
        /// <remarks>
        /// Use '^' and '$' in the regular expression to anchor the expression to the input start/end for a match that consumes the whole input.
        /// Multiple captures with the same group name are concatenated to form the final named group capture.
        /// </remarks>
        /// <param name="regex">Regex to apply.</param>
        /// <param name="value">Value to match against.</param>
        /// <param name="captures">Receives captured values.</param>
        /// <returns>True if matched, false otherwise.</returns>
        public static bool CheckMatch(Regex regex, string value, ref IList<KeyValuePair<string, string>> captures)
        {
            if (regex == null)
            {
                return string.IsNullOrEmpty(value);
            }

            if (value == null)
            {
                value = string.Empty;
            }

            if (regex == MatchAllRegex)
            {
                return true;
            }

            Match match = regex.Match(value);
            if (match.Success)
            {
                foreach (string groupName in regex.GetGroupNames())
                {
                    if (captures == null)
                    {
                        captures = new List<KeyValuePair<string, string>>();
                    }

                    Group group = match.Groups[groupName];
                    if (group.Success)
                    {
                        if (group.Captures.Count == 1)
                        {
                            captures.Add(new KeyValuePair<string, string>(groupName, group.Value));
                        }
                        else
                        {
                            StringBuilder builder = new StringBuilder();
                            foreach (Capture capture in group.Captures)
                            {
                                builder.Append(capture.Value);
                            }
                            captures.Add(new KeyValuePair<string, string>(groupName, builder.ToString()));
                        }
                    }
                    else
                    {
                        captures.Add(new KeyValuePair<string, string>(groupName, string.Empty));
                    }
                }

                return true;
            }
            else
            {
                return false;
            }
        }
    }
}
