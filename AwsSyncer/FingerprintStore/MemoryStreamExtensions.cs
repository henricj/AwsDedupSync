// Copyright (c) 2019 Henric Jungheim <software@henric.org>
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
using System.Buffers;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace AwsSyncer.FingerprintStore
{
    public static class MemoryStreamExtensions
    {
        public static async Task WriteAsync(this Stream stream, ReadOnlyMemory<byte> readOnlyMemory, CancellationToken cancellationToken)
        {
            if (MemoryMarshal.TryGetArray(readOnlyMemory, out var segment))
                await stream.WriteAsync(segment.Array, segment.Offset, segment.Count, cancellationToken).ConfigureAwait(false);
            else
            {
                var writeBuffer = ArrayPool<byte>.Shared.Rent(readOnlyMemory.Length);

                readOnlyMemory.CopyTo(writeBuffer);

                await stream.WriteAsync(writeBuffer, 0, readOnlyMemory.Length, cancellationToken).ConfigureAwait(false);

                ArrayPool<byte>.Shared.Return(writeBuffer);
            }
        }

        public static async Task WriteAsync(this Stream stream, ReadOnlySequence<byte> readOnlySequence, CancellationToken cancellationToken)
        {
            byte[] writeBuffer = null;

            //try
            //{
            foreach (var readOnlyMemory in readOnlySequence)
            {
                if (MemoryMarshal.TryGetArray(readOnlyMemory, out var segment))
                    await stream.WriteAsync(segment.Array, segment.Offset, segment.Count, cancellationToken).ConfigureAwait(false);
                else
                {
                    if (null != writeBuffer && writeBuffer.Length < readOnlyMemory.Length)
                    {
                        var temp = writeBuffer;
                        writeBuffer = null;

                        ArrayPool<byte>.Shared.Return(temp);
                    }

                    if (null == writeBuffer)
                        writeBuffer = ArrayPool<byte>.Shared.Rent(Math.Max(readOnlyMemory.Length, 64 * 1024));

                    readOnlyMemory.CopyTo(writeBuffer);

                    await stream.WriteAsync(writeBuffer, 0, readOnlyMemory.Length, cancellationToken).ConfigureAwait(false);
                }
            }
            //}
            //finally
            //{
            if (null != writeBuffer) ArrayPool<byte>.Shared.Return(writeBuffer);
            //}
        }
    }
}
