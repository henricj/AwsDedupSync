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
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
using AwsSyncer.Types;
using MessagePack;

namespace AwsSyncer.FingerprintStore
{
    public sealed partial class MessagePackFileFingerprintStore
    {
        sealed class FileFingerprintWriter : IDisposable
        {
            readonly Pipe _pipe = new Pipe();
            Task _compressorTask;
            byte[] _intBuffer;
            Stream _stream;

            public void Dispose()
            {
                _stream?.Dispose();
            }

            public void Open(FileInfo fi)
            {
                if (null != _stream) throw new InvalidOperationException("The writer is already open");

                _stream = OpenMsgPackFileForWrite(fi);

                _compressorTask = Task.Run(RunCompressorAsync);
            }

            async Task RunCompressorAsync()
            {
                var reader = _pipe.Reader;
                var cancellationToken = default(CancellationToken);

                try
                {
                    using (var outputBufferedStream = new BufferedStream(_stream, 128 * 1024))
                    using (var deflate = new DeflateStream(outputBufferedStream, CompressionLevel.Optimal))
                    using (var inputBufferedStream = new BufferedStream(deflate, 64 * 1024))
                    {
                        for (; ; )
                        {
                            if (!reader.TryRead(out var readResult))
                                readResult = await reader.ReadAsync(cancellationToken).ConfigureAwait(false);

                            var buffer = readResult.Buffer;

                            await inputBufferedStream.WriteAsync(buffer, cancellationToken).ConfigureAwait(false);

                            buffer = buffer.Slice(buffer.Length);

                            reader.AdvanceTo(buffer.Start, buffer.End);

                            if (readResult.IsCompleted) break;
                        }

                        await inputBufferedStream.FlushAsync(cancellationToken).ConfigureAwait(false);
                    }
                }
                finally
                {
                    reader.Complete();
                }
            }

            public async Task CloseAsync(CancellationToken cancellationToken)
            {
                _pipe.Writer.Complete();

                var tcs = new TaskCompletionSource<bool>();

                _pipe.Reader.OnWriterCompleted((e, o) =>
                {
                    if (null == e) tcs.TrySetResult(true);
                    else tcs.TrySetException(e);
                }, null);

                await Task.WhenAll(_compressorTask, tcs.Task).ConfigureAwait(false);
            }

            public void Write(FileFingerprint fileFingerprint)
            {
                var writer = _pipe.Writer;

                var blob = MessagePackSerializer.SerializeUnsafe(fileFingerprint).AsSpan();

                var lengthBlob = IntFormatter.Serialize(ref _intBuffer, 0, blob.Length, MessagePackSerializer.DefaultResolver);

                var outputLength = lengthBlob + blob.Length;

                var output = writer.GetSpan(outputLength);

                Debug.Assert(output.Length >= outputLength);

                _intBuffer.AsSpan(0, lengthBlob).CopyTo(output);

                output = output.Slice(lengthBlob);

                blob.CopyTo(output);

                writer.Advance(outputLength);
            }

            public async ValueTask FlushAsync(CancellationToken cancellationToken)
            {
                await _pipe.Writer.FlushAsync(cancellationToken).ConfigureAwait(false);
            }
        }
    }
}
