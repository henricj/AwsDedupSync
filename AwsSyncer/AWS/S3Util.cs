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
using Microsoft.AspNetCore.WebUtilities;

namespace AwsSyncer.AWS;

public static class S3Util
{
    public static string KeyAlphabet => "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

    public static string ComputeS3Etag(byte[] md5Digest) =>
        $"\"{Convert.ToHexString(md5Digest).ToLowerInvariant()}\"";

    public static string S3EncodeKey(ReadOnlySpan<byte> value) => WebEncoders.Base64UrlEncode(value);

    public static byte[] DecodeKey(string value, int offset = 0, int count = 0)
    {
        if (offset < 0 || offset >= value.Length)
            throw new ArgumentOutOfRangeException(nameof(offset));
        if (count < 0 || offset + count > value.Length)
            throw new ArgumentOutOfRangeException(nameof(count));

        if (count == 0)
            count = value.Length - offset;

        if (count == 0)
            return [];

        try
        {
            return WebEncoders.Base64UrlDecode(value, offset, count);
        }
        catch (FormatException)
        {
            return [];
        }
    }
}
