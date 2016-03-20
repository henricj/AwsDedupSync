// Copyright (c) 2014 Henric Jungheim <software@henric.org>
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

using System.IO;
using System.Security.Cryptography;
using System.Threading.Tasks;
using KeccakOpt;

namespace AwsSyncer
{
    public class StreamFingerprinter
    {
        public async Task<BlobFingerprint> GetFingerprintAsync(Stream stream)
        {
            using (var sha3 = new Keccak())
            using (var sha256 = SHA256.Create())
            using (var md5 = MD5.Create())
            {
                var buffer = new byte[32 * 1024];

                var totalLength = 0L;

                for (; ; )
                {
                    var length = await stream.ReadAsync(buffer, 0, buffer.Length).ConfigureAwait(false);

                    if (length < 1)
                        break;

                    totalLength += length;

                    sha3.TransformBlock(buffer, 0, length, null, 0);

                    sha256.TransformBlock(buffer, 0, length, null, 0);

                    md5.TransformBlock(buffer, 0, length, null, 0);
                }

                sha3.TransformFinalBlock(buffer, 0, 0);

                sha256.TransformFinalBlock(buffer, 0, 0);

                md5.TransformFinalBlock(buffer, 0, 0);

                var fingerprint = new BlobFingerprint(totalLength, sha3.Hash, sha256.Hash, md5.Hash);

                return fingerprint;
            }
        }
    }
}
