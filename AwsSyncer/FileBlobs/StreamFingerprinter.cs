// Copyright (c) 2014, 2017, 2023 Henric Jungheim <software@henric.org>
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
using System.IO;
using System.IO.Pipelines;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using AwsSyncer.Types;
using KeccakOpt;
using Microsoft.Extensions.ObjectPool;

namespace AwsSyncer.FileBlobs;

public interface IStreamFingerprinter
{
    ValueTask<BlobFingerprint> GetFingerprintAsync(Stream stream, CancellationToken cancellationToken);
}

public class StreamFingerprinter(ObjectPoolProvider pipePool) : IStreamFingerprinter
{
    static readonly DefaultPooledObjectPolicy<State> PipePooledObjectPolicy = new();

    readonly ObjectPool<State> _statePool = pipePool.Create(PipePooledObjectPolicy);

    public async ValueTask<BlobFingerprint> GetFingerprintAsync(Stream stream, CancellationToken cancellationToken)
    {
        var state = _statePool.Get();
        try
        {
            return await state.CreateFingerprintAsync(stream, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            state.Reset();
            _statePool.Return(state);
        }
    }

    sealed class State : IDisposable
    {
        readonly IncrementalHash _md5 = IncrementalHash.CreateHash(HashAlgorithmName.MD5);
        readonly Pipe _pipe = new();
        readonly IncrementalHash _sha256 = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        readonly Keccak _sha3 = new();

        public void Dispose()
        {
            _sha3.Dispose();
            _sha256.Dispose();
            _md5.Dispose();
        }

        public async ValueTask<BlobFingerprint> CreateFingerprintAsync(Stream stream, CancellationToken cancellationToken)
        {
            var totalLength = 0L;

            var reader = _pipe.Reader;
            var writer = _pipe.Writer;

            var readTask = Task.Run(async () =>
            {
                try
                {
                    await stream.CopyToAsync(writer, cancellationToken).ConfigureAwait(false);
                    await writer.CompleteAsync().ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    await writer.CompleteAsync(ex).ConfigureAwait(false);
                }
            }, cancellationToken);

            try
            {
                for (;;)
                {
                    if (!reader.TryRead(out var result))
                    {
                        result = await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
                    }

                    if (result.IsCanceled)
                        break;

                    if (result.Buffer.IsEmpty)
                    {
                        if (result.IsCompleted)
                            break;

                        continue;
                    }

                    var buffer = result.Buffer;
                    totalLength += buffer.Length;

                    if (buffer.IsSingleSegment)
                    {
                        _sha3.AppendData(buffer.FirstSpan);
                        _sha256.AppendData(buffer.FirstSpan);
                        _md5.AppendData(buffer.FirstSpan);
                    }
                    else
                    {
                        foreach (var memory in buffer)
                        {
                            _sha3.AppendData(memory.Span);
                            _sha256.AppendData(memory.Span);
                            _md5.AppendData(memory.Span);
                        }
                    }

                    reader.AdvanceTo(buffer.End);
                }
            }
            finally
            {
                await reader.CompleteAsync().ConfigureAwait(false);
                await readTask.ConfigureAwait(false);
            }

            var sha3Hash = new byte[Keccak.HashSizeInBytes];
            _sha3.GetHashAndReset(sha3Hash);

            var sha256Hash = new byte[_sha256.HashLengthInBytes];
            _sha256.GetHashAndReset(sha256Hash);

            var md5Hash = new byte[_md5.HashLengthInBytes];
            _md5.GetHashAndReset(md5Hash);

            var fingerprint = new BlobFingerprint(totalLength, sha3Hash, sha256Hash, md5Hash);

            return fingerprint;
        }

        public void Reset()
        {
            _pipe.Reset();
            _sha3.Initialize();
            _sha256.TryGetHashAndReset([], out _);
            _md5.TryGetHashAndReset([], out _);
        }
    }
}
