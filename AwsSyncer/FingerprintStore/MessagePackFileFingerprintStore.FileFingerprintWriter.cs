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
using System.IO.Compression;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
using AwsSyncer.Types;
using MessagePack;

namespace AwsSyncer.FingerprintStore;

public sealed partial class MessagePackFileFingerprintStore
{
    sealed class FileFingerprintWriter : IDisposable
    {
        readonly ArrayBufferWriter<byte> _fingerprintWriter = new();
        readonly Pipe _pipe = new();
        Task _compressorTask;
        Stream _stream;

        public void Dispose() => _stream?.Dispose();

        public void Open(FileInfo fi)
        {
            if (null != _stream)
                throw new InvalidOperationException("The writer is already open");

            _stream = OpenMsgPackFileForWrite(fi);

            _compressorTask = Task.Run(RunCompressorAsync);
        }

        async Task RunCompressorAsync()
        {
            var reader = _pipe.Reader;
            var cancellationToken = default(CancellationToken);

            try
            {
                var outputBufferedStream = new BufferedStream(_stream, 128 * 1024);
                var deflate = new DeflateStream(outputBufferedStream, CompressionLevel.Optimal);
                var inputBufferedStream = new BufferedStream(deflate, 64 * 1024);

                await using (outputBufferedStream.ConfigureAwait(false))
                await using (deflate.ConfigureAwait(false))
                await using (inputBufferedStream.ConfigureAwait(false))
                {
                    for (;;)
                    {
                        if (!reader.TryRead(out var readResult))
                            readResult = await reader.ReadAsync(cancellationToken).ConfigureAwait(false);

                        var buffer = readResult.Buffer;

                        await inputBufferedStream.WriteAsync(buffer, cancellationToken).ConfigureAwait(false);

                        buffer = buffer.Slice(buffer.Length);

                        reader.AdvanceTo(buffer.Start, buffer.End);

                        if (readResult.IsCompleted)
                            break;
                    }

                    await inputBufferedStream.FlushAsync(cancellationToken).ConfigureAwait(false);
                }
            }
            finally
            {
                await reader.CompleteAsync().ConfigureAwait(false);
            }
        }

        public async Task CloseAsync()
        {
            await _pipe.Writer.CompleteAsync().ConfigureAwait(false);

            await _compressorTask.ConfigureAwait(false);
        }

        static void IntSerializer(IBufferWriter<byte> bufferWriter, int value)
        {
            var writer = new MessagePackWriter(bufferWriter);

            IntFormatter.Serialize(ref writer, value, MessagePackSerializerOptions.Standard);
        }

        public void Write(FileFingerprint fileFingerprint)
        {
            var writer = _pipe.Writer;

            MessagePackSerializer.Serialize(writer, fileFingerprint);
        }

        public async ValueTask FlushAsync(CancellationToken cancellationToken) =>
            await _pipe.Writer.FlushAsync(cancellationToken).ConfigureAwait(false);
    }
}
