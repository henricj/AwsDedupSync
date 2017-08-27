// Copyright (c) 2014, 2017 Henric Jungheim <software@henric.org>
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

using System.Collections.Concurrent;
using System.IO;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using KeccakOpt;

namespace AwsSyncer.FileBlobs
{
    public class StreamFingerprinter
    {
        const int BufferSize = 256 * 1024;
        readonly ConcurrentStack<byte[]> _buffers = new ConcurrentStack<byte[]>();

        public async Task<BlobFingerprint> GetFingerprintAsync(Stream stream, CancellationToken cancellationToken)
        {
            using (var sha3 = new Keccak())
            using (var sha256 = SHA256.Create())
            using (var md5 = MD5.Create())
            {
                var buffers = new[] { AllocateBuffer(), AllocateBuffer() };
                var index = 0;

                var totalLength = 0L;

                var readTask = stream.ReadAsync(buffers[index], 0, buffers[index].Length, cancellationToken);

                for (; ; )
                {
                    var length = await readTask.ConfigureAwait(false);

                    if (length < 1)
                        break;

                    var buffer = buffers[index];

                    if (++index >= buffers.Length)
                        index = 0;

                    readTask = stream.ReadAsync(buffers[index], 0, buffers[index].Length, cancellationToken);

                    totalLength += length;

                    sha3.TransformBlock(buffer, 0, length, buffer, 0);

                    sha256.TransformBlock(buffer, 0, length, buffer, 0);

                    md5.TransformBlock(buffer, 0, length, buffer, 0);
                }

                sha3.TransformFinalBlock(buffers[0], 0, 0);

                sha256.TransformFinalBlock(buffers[0], 0, 0);

                md5.TransformFinalBlock(buffers[0], 0, 0);

                foreach (var buffer in buffers)
                    FreeBuffer(buffer);

                var fingerprint = new BlobFingerprint(totalLength, sha3.Hash, sha256.Hash, md5.Hash);

                return fingerprint;
            }
        }

        byte[] AllocateBuffer()
        {
            if (!_buffers.TryPop(out var buffer))
                buffer = new byte[BufferSize];

            return buffer;
        }

        void FreeBuffer(byte[] buffer)
        {
            if (BufferSize != buffer.Length)
                return;

            _buffers.Push(buffer);
        }
    }
}
