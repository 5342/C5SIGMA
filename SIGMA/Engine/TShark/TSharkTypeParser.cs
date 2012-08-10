using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Globalization;

namespace Sigma.Engine.TShark
{

    /// <summary>
    /// Provides type parsing helper methods.
    /// </summary>
    internal static class TSharkTypeParser
    {

        /// <summary>
        /// Attempts to parse an Int64 value, optionally prefixed with "&amp;h" or "0x" to indicate a hexadecimal formatted string.
        /// </summary>
        /// <param name="input">Input string to parse.</param>
        /// <param name="value">The parsed value.</param>
        /// <returns>True if successfully parsed, false otherwise.</returns>
        public static bool TryParseInt64(string input, out long value)
        {
            input = input.Trim();
            if (input.StartsWith("0x", StringComparison.OrdinalIgnoreCase) || input.StartsWith("&h", StringComparison.OrdinalIgnoreCase))
            {
                return long.TryParse(input.Substring(2), NumberStyles.AllowHexSpecifier, null, out value);
            }
            else
            {
                return long.TryParse(input, out value);
            }
        }

        /// <summary>
        /// Attempts to parse an Int32 value, optionally prefixed with "&amp;h" or "0x" to indicate a hexadecimal formatted string.
        /// </summary>
        /// <param name="input">Input string to parse.</param>
        /// <param name="value">The parsed value.</param>
        /// <returns>True if successfully parsed, false otherwise.</returns>
        public static bool TryParseInt32(string input, out int value)
        {
            input = input.Trim();
            if (input.StartsWith("0x", StringComparison.OrdinalIgnoreCase) || input.StartsWith("&h", StringComparison.OrdinalIgnoreCase))
            {
                return int.TryParse(input.Substring(2), NumberStyles.AllowHexSpecifier, null, out value);
            }
            else
            {
                return int.TryParse(input, out value);
            }
        }

        /// <summary>
        /// Attempts to parse an Int16 value, optionally prefixed with "&amp;h" or "0x" to indicate a hexadecimal formatted string.
        /// </summary>
        /// <param name="input">Input string to parse.</param>
        /// <param name="value">The parsed value.</param>
        /// <returns>True if successfully parsed, false otherwise.</returns>
        public static bool TryParseInt16(string input, out short value)
        {
            input = input.Trim();
            if (input.StartsWith("0x", StringComparison.OrdinalIgnoreCase) || input.StartsWith("&h", StringComparison.OrdinalIgnoreCase))
            {
                return short.TryParse(input.Substring(2), NumberStyles.AllowHexSpecifier, null, out value);
            }
            else
            {
                return short.TryParse(input, out value);
            }
        }

        /// <summary>
        /// Attempts to parse a UInt64 value, optionally prefixed with "&amp;h" or "0x" to indicate a hexadecimal formatted string.
        /// </summary>
        /// <param name="input">Input string to parse.</param>
        /// <param name="value">The parsed value.</param>
        /// <returns>True if successfully parsed, false otherwise.</returns>
        public static bool TryParseUInt64(string input, out ulong value)
        {
            input = input.Trim();
            if (input.StartsWith("0x", StringComparison.OrdinalIgnoreCase) || input.StartsWith("&h", StringComparison.OrdinalIgnoreCase))
            {
                return ulong.TryParse(input.Substring(2), NumberStyles.AllowHexSpecifier, null, out value);
            }
            else
            {
                return ulong.TryParse(input, out value);
            }
        }

        /// <summary>
        /// Attempts to parse a UInt32 value, optionally prefixed with "&amp;h" or "0x" to indicate a hexadecimal formatted string.
        /// </summary>
        /// <param name="input">Input string to parse.</param>
        /// <param name="value">The parsed value.</param>
        /// <returns>True if successfully parsed, false otherwise.</returns>
        public static bool TryParseUInt32(string input, out uint value)
        {
            input = input.Trim();
            if (input.StartsWith("0x", StringComparison.OrdinalIgnoreCase) || input.StartsWith("&h", StringComparison.OrdinalIgnoreCase))
            {
                return uint.TryParse(input.Substring(2), NumberStyles.AllowHexSpecifier, null, out value);
            }
            else
            {
                return uint.TryParse(input, out value);
            }
        }

        /// <summary>
        /// Attempts to parse a UInt16 value, optionally prefixed with "&amp;h" or "0x" to indicate a hexadecimal formatted string.
        /// </summary>
        /// <param name="input">Input string to parse.</param>
        /// <param name="value">The parsed value.</param>
        /// <returns>True if successfully parsed, false otherwise.</returns>
        public static bool TryParseUInt16(string input, out ushort value)
        {
            input = input.Trim();
            if (input.StartsWith("0x", StringComparison.OrdinalIgnoreCase) || input.StartsWith("&h", StringComparison.OrdinalIgnoreCase))
            {
                return ushort.TryParse(input.Substring(2), NumberStyles.AllowHexSpecifier, null, out value);
            }
            else
            {
                return ushort.TryParse(input, out value);
            }
        }

        /// <summary>
        /// Reverses a byte array in place, returning a reference to the original array.
        /// </summary>
        /// <param name="value">Byte array to reverse.</param>
        public static byte[] Reverse(byte[] value)
        {
            for (int i = 0; i < value.Length / 2; i++)
            {
                byte t = value[i];
                value[i] = value[value.Length - 1 - i];
                value[value.Length - 1 - i] = t;
            }
            return value;
        }

        /// <summary>
        /// Parses a string of hexadecimal characters into a byte array.
        /// </summary>
        /// <param name="input">String to parse.</param>
        /// <returns>Parsed byte array.</returns>
        public static byte[] ParseOctBytes(string input)
        {
            return ParseOctBytes(input, null);
        }

        /// <summary>
        /// Parses a string of hexadecimal characters into a byte array.
        /// </summary>
        /// <param name="input">String to parse.</param>
        /// <param name="minLength">Minimum array length, zero left padded to fill.</param>
        /// <returns>Parsed byte array.</returns>
        public static byte[] ParseOctBytes(string input, int? minLength)
        {
            // TODO optimise this method

            if (input.StartsWith("0o") || input.StartsWith("&"))
            {
                input = input.Substring(2);
            }

            int splitIndex = input.IndexOf('(');
            if (splitIndex >= 0)
            {
                input = input.Substring(0, splitIndex).TrimEnd();
            }

            int pad;
            int size = (input.Length * 3 + 7) / 8;
            if (minLength == null)
            {
                minLength = size;
            }

            if (size < minLength)
            {
                pad = (int)minLength - size;
                size = (int)minLength;
            }
            else
            {
                pad = 0;
            }

            byte[] result = new byte[size];

            int bits = 7 - (input.Length * 3 + 7) % 8;
            int buffer = 0;
            int x = 0;
            for (int i = 0; i < size - pad; i++)
            {
                while (bits < 8 && x < input.Length)
                {
                    buffer <<= 3;
                    buffer |= OctTo3Bits(input[x++]);
                    bits += 3;
                }

                result[i + pad] = (byte)(buffer >> (bits - 8));
                bits -= 8;
            }

            return result;
        }

        /// <summary>
        /// Converts a byte array into its hexadecimal representation.
        /// </summary>
        /// <param name="input">Byte array to convert.</param>
        /// <returns>Hexadecimal representation of the byte array.</returns>
        public static string ConstructHexBytes(byte[] input)
        {
            StringBuilder builder = new StringBuilder(input.Length * 2);
            for (int i = 0; i < input.Length; i++)
            {
                builder.Append(NibbleToHex(input[i] >> 4));
                builder.Append(NibbleToHex(input[i]));
            }
            return builder.ToString();
        }

        /// <summary>
        /// Parses a string of hexadecimal characters into a byte array.
        /// </summary>
        /// <param name="input">String to parse.</param>
        /// <returns>Parsed byte array.</returns>
        public static byte[] ParseHexBytes(string input)
        {
            return ParseHexBytes(input, null);
        }

        /// <summary>
        /// Parses a string of hexadecimal characters into a byte array.
        /// </summary>
        /// <param name="input">String to parse.</param>
        /// <param name="minLength">Minimum array length, zero left padded to fill.</param>
        /// <returns>Parsed byte array.</returns>
        public static byte[] ParseHexBytes(string input, int? minLength)
        {
            // TODO optimise this method

            if (input == null)
                return null;

            if (input.Length == 0)
                return new byte[0];

            if (input.StartsWith("0x") || input.StartsWith("&"))
            {
                input = input.Substring(2);
            }

            int splitIndex = input.IndexOf('(');
            if (splitIndex >= 0)
            {
                input = input.Substring(0, splitIndex).TrimEnd();
            }

            int pad;
            int size = (input.Length * 4 + 7) / 8;
            if (minLength == null)
            {
                minLength = size;
            }

            if (size < minLength)
            {
                pad = (int)minLength - size;
                size = (int)minLength;
            }
            else
            {
                pad = 0;
            }

            byte[] result = new byte[size];

            int bits = 7 - (input.Length * 4 + 7) % 8;
            int buffer = 0;
            int x = 0;
            for (int i = 0; i < size - pad; i++)
            {
                while (bits < 8 && x < input.Length)
                {
                    buffer <<= 4;
                    buffer |= HexToNibble(input[x++]);
                    bits += 4;
                }

                result[i + pad] = (byte)(buffer >> (bits - 8));
                bits -= 8;
            }

            return result;
        }

        /// <summary>
        /// Converts an octal digit [0-7] to a three bit value.
        /// </summary>
        /// <param name="c">Character to convert.</param>
        /// <returns>Three bit value corresponding to the given character.</returns>
        public static int OctTo3Bits(char c)
        {
            if (c >= '0' && c <= '7')
            {
                return c - '0';
            }
            else
            {
                throw new Exception(string.Format("Not a valid octal digit: Unicode 0x{0:X4}", (int)c));
            }
        }

        /// <summary>
        /// Converts a hexadecimal digit [0-9,a-z,A-Z] to a nibble (4 bits).
        /// </summary>
        /// <param name="c">Character to convert.</param>
        /// <returns>Nibble corresponding to the given character.</returns>
        public static int HexToNibble(char c)
        {
            if (c >= '0' && c <= '9')
            {
                return c - '0';
            }
            else if (c >= 'a' && c <= 'f')
            {
                return (c - 'a') + 10;
            }
            else if (c >= 'A' && c <= 'F')
            {
                return (c - 'A') + 10;
            }
            else
            {
                throw new Exception(string.Format("Not a valid hexadecimal digit: Unicode 0x{0:X4}", (int)c));
            }
        }

        /// <summary>
        /// Converts a nibble (4 bits) to the corresponding hexadecimal digit.
        /// </summary>
        /// <param name="i">Value to convert.</param>
        /// <returns>Hexadecimal digit corresponding to the given nibble.</returns>
        public static char NibbleToHex(int i)
        {
            const string Digits = "0123456789abcdef";
            return Digits[i & 0x0f];
        }

        /// <summary>
        /// Parses a TShark formatted DateTime value.
        /// </summary>
        /// <remarks>
        /// The input string is assumed to be in local time. The parsed value is returned as UTC.
        /// </remarks>
        /// <param name="dateTime">Value to parse.</param>
        /// <returns>Parsed value.</returns>
        public static DateTime ParseDateTime(string dateTime)
        {
            const string FormatString = "MMM d, yyyy HH:mm:ss.FFFFFFF";
            
            int splitIndex = dateTime.IndexOf('.');
            if (splitIndex >= 0 && splitIndex < dateTime.Length - 7)
            {
                dateTime = dateTime.Substring(0, splitIndex + 7);
            }

            DateTime result = DateTime.ParseExact(dateTime, FormatString, null, DateTimeStyles.AssumeLocal | DateTimeStyles.AllowInnerWhite);
            result = result.ToUniversalTime();
            return result;
        }
    }
}
