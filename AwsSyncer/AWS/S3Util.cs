// Copyright (c) 2016 Henric Jungheim <software@henric.org>
// 
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
// THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.

using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.AspNetCore.WebUtilities;

namespace AwsSyncer.AWS
{
    public static class S3Util
    {
        static S3Util()
        {
            KeyAlphabet = DiscoverAlphabet();
        }

        public static IReadOnlyCollection<char> KeyAlphabet { get; }

        public static string ComputeS3Etag(byte[] md5Digest) =>
            '"' + BitConverter.ToString(md5Digest).Replace("-", string.Empty) + '"';

        static char[] DiscoverAlphabet()
        {
            var buffer = new byte[3];
            var alphabet = new HashSet<char>();

            for (var i = (int)byte.MinValue; i <= byte.MaxValue; ++i)
            {
                buffer[0] = (byte)i;

                var encoded = S3EncodeKey(buffer);

                foreach (var ch in encoded)
                    alphabet.Add(ch);
            }

            //Debug.WriteLine($"alphabet has {alphabet.Count} letters");

            if (64 != alphabet.Count)
                return null;

            return alphabet.OrderBy(c => c).ToArray();
        }

        public static string S3EncodeKey(byte[] value) => Base64UrlTextEncoder.Encode(value);

        public static byte[] DecodeKey(string value) => Base64UrlTextEncoder.Decode(value);
    }
}
