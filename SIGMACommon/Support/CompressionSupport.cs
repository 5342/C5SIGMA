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
using System.IO;
using System.IO.Compression;

namespace Sigma.Common.Support
{

    /// <summary>
    /// Provides helper methods to support the compression and decompression of files and streams.
    /// </summary>
    public static class CompressionSupport
    {

        /// <summary>
        /// Transfers all data from one Stream to another Stream.
        /// </summary>
        /// <param name="input">Input stream.</param>
        /// <param name="output">Output stream.</param>
        /// <returns>The number of bytes transferred.</returns>
        private static long Transfer(Stream input, Stream output)
        {
            long size = 0;

            byte[] buffer = new byte[32768];
            while (true)
            {
                int bytesRead = input.Read(buffer, 0, buffer.Length);
                if (bytesRead <= 0)
                    break;

                output.Write(buffer, 0, bytesRead);
                size += bytesRead;
            }

            return size;
        }

        /// <summary>
        /// Compresses a file.
        /// </summary>
        /// <param name="pathIn">Input file path.</param>
        /// <param name="pathOut">Output file path.</param>
        public static void CompressFile(string pathIn, string pathOut)
        {
            using (Stream input = new FileStream(pathIn, FileMode.Open, FileAccess.Read, FileShare.Read))
            using (Stream output = CompressStream(pathOut))
            {
                Transfer(input, output);
            }
        }

        /// <summary>
        /// Decompresses a file.
        /// </summary>
        /// <param name="pathIn">Input file path.</param>
        /// <param name="pathOut">Output file path.</param>
        public static void DecompressFile(string pathIn, string pathOut)
        {
            using (Stream input = DecompressStream(pathIn))
            using (Stream output = new FileStream(pathOut, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                Transfer(input, output);
            }
        }

        /// <summary>
        /// Wraps the given stream in a decompression stream.
        /// </summary>
        /// <param name="input">Stream to wrap.</param>
        /// <returns>Decompression stream from which decompressed data can be read.</returns>
        public static Stream DecompressStream(Stream input)
        {
            Stream result = new GZipStream(input, CompressionMode.Decompress);
            int magic = result.ReadByte() << 24 | result.ReadByte() << 16 | result.ReadByte() << 8 | result.ReadByte();
            if (magic != MagicNumber)
                throw new Exception("Unsupported compression scheme or bad magic number.");
            return result;
        }

        /// <summary>
        /// Opens the specified file for read and wraps the resulting stream in a decompression stream.
        /// </summary>
        /// <param name="pathIn">Input file path.</param>
        /// <returns>Decompression stream from which decompressed data can be read.</returns>
        public static Stream DecompressStream(string pathIn)
        {
            return DecompressStream(new FileStream(pathIn, FileMode.Open, FileAccess.Read, FileShare.Read));
        }

        /// <summary>
        /// Wraps the given stream in a compression stream.
        /// </summary>
        /// <param name="output">Stream to wrap.</param>
        /// <returns>Compression stream to which data can be written.</returns>
        public static Stream CompressStream(Stream output)
        {
            Stream result = new GZipStream(output, CompressionMode.Compress);
            result.WriteByte(0xff & (MagicNumber >> 24));
            result.WriteByte(0xff & (MagicNumber >> 16));
            result.WriteByte(0xff & (MagicNumber >> 8));
            result.WriteByte(0xff & (MagicNumber));
            return result;
        }

        /// <summary>
        /// Opens the specified file for write and wraps the resulting stream in a compression stream.
        /// </summary>
        /// <param name="pathOut">Output file path.</param>
        /// <returns>Compression stream to which data can be written.</returns>
        public static Stream CompressStream(string pathOut)
        {
            return CompressStream(new FileStream(pathOut, FileMode.Create, FileAccess.Write, FileShare.None));
        }

        /// <summary>
        /// Magic number used to mark compressed streams ("SIGM").
        /// </summary>
        private const int MagicNumber = 0x5349474d;
    }
}
