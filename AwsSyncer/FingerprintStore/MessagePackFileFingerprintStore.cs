// Copyright (c) 2016-2019 Henric Jungheim <software@henric.org>
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
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
using AwsSyncer.Types;
using AwsSyncer.Utility;
using MessagePack;
using MessagePack.Formatters;

namespace AwsSyncer.FingerprintStore
{
    public sealed partial class MessagePackFileFingerprintStore : IFileFingerprintStore
    {
        static readonly DirectoryInfo MsgPackDirectory = GetMsgPackDirectory();
        static readonly Dictionary<string, FileFingerprint> EmptyFileFingerprints = new Dictionary<string, FileFingerprint>();
        static readonly IMessagePackFormatter<int> IntFormatter = MessagePackSerializer.DefaultResolver.GetFormatter<int>();
        readonly FileSequence _fileSequence;
        readonly AsyncLock _lock = new AsyncLock();
        FileFingerprintWriter _writer;

        public MessagePackFileFingerprintStore() => _fileSequence = new FileSequence(MsgPackDirectory);

        public int UpdateCount { get; private set; }
        public long UpdateSize { get; private set; }

        public void Dispose()
        {
            try
            {
                CloseWriterAsync().GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                Debug.WriteLine("CloseWriter() failed: " + ex.Message);
            }

            _lock.Dispose();
        }

        public async Task<IReadOnlyDictionary<string, FileFingerprint>> LoadBlobsAsync(CancellationToken cancellationToken)
        {
            try
            {
                var ret = await LoadBlobCacheRetryAsync(cancellationToken).ConfigureAwait(false);

                if (null != ret)
                    return ret;
            }
            catch (FileFormatException)
            {
                // The schema has changed?
            }

            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                await RebuildCacheAsync(EmptyFileFingerprints, cancellationToken).ConfigureAwait(false);
            }
            catch (IOException ex)
            {
                Console.WriteLine("LoadBlobsAsync() delete failed: " + ex.Message);
            }

            return EmptyFileFingerprints;
        }

        public async Task CloseAsync(CancellationToken cancellationToken)
        {
            using (await _lock.LockAsync(cancellationToken).ConfigureAwait(false))
            {
                await CloseWriterAsync().ConfigureAwait(false);
            }
        }

        public async Task StoreBlobsAsync(ICollection<FileFingerprint> fileFingerprints, CancellationToken cancellationToken)
        {
            using (await _lock.LockAsync(cancellationToken).ConfigureAwait(false))
                await StoreBlobsImplAsync(fileFingerprints, cancellationToken).ConfigureAwait(false);
        }

        static Stream OpenMsgPackFileForRead(FileInfo fi) =>
            new FileStream(fi.FullName, FileMode.Open,
                FileAccess.Read, FileShare.Read,
                8192, FileOptions.SequentialScan | FileOptions.Asynchronous);

        static Stream OpenMsgPackFileForWrite(FileInfo fi)
        {
            fi.Directory.Refresh();

            if (!fi.Directory.Exists)
                fi.Directory.Create();

            return new FileStream(fi.FullName, FileMode.CreateNew,
                FileAccess.Write, FileShare.None,
                8192, FileOptions.SequentialScan | FileOptions.Asynchronous);
        }

        static DirectoryInfo GetMsgPackDirectory()
        {
            var localApplicationData = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData,
                Environment.SpecialFolderOption.Create);

            var path = Path.Combine(localApplicationData, "AwsSyncer", "MsgPackPaths");

            var bucket = ConfigurationManager.AppSettings["bucket"];

            if (!string.IsNullOrWhiteSpace(bucket))
                path = Path.Combine(path, bucket);

            return new DirectoryInfo(path);
        }

        async Task<IReadOnlyDictionary<string, FileFingerprint>> LoadBlobCacheRetryAsync(CancellationToken cancellationToken)
        {
            var delay = 3.0;

            for (var retry = 0; retry < 3; ++retry)
            {
                try
                {
                    using (await _lock.LockAsync(cancellationToken).ConfigureAwait(false))
                        return await LoadBlobsImplAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Cache read failed: " + ex.Message);
                }

                var rnd = 0.5 * delay * RandomUtil.ThreadLocalRandom.NextDouble();

                await Task.Delay(TimeSpan.FromSeconds(delay + rnd), cancellationToken).ConfigureAwait(false);

                delay *= 2;
            }

            return null;
        }

        async Task<IReadOnlyDictionary<string, FileFingerprint>> LoadBlobsImplAsync(CancellationToken cancellationToken)
        {
            var blobs = new Dictionary<string, FileFingerprint>();

            var needRebuild = false;

            var blobCount = 0;
            var fileCount = 0;

            _fileSequence.Rescan();

            var totalSize = 0L;
            var compressedSize = 0L;

            foreach (var fileInfo in _fileSequence.Files)
            {
                ++fileCount;

                try
                {
                    fileInfo.Refresh();

                    if (fileInfo.Length < 5)
                    {
                        needRebuild = true;
                        continue;
                    }

                    cancellationToken.ThrowIfCancellationRequested();

                    var pl = new Pipe();

                    var writer = pl.Writer;


                    void CopyToWriter(int length, byte[] buffer)
                    {
                        var input = buffer.AsSpan(0, length);

                        while (input.Length > 0)
                        {
                            var output = writer.GetSpan(length);

                            if (output.Length <= 0) throw new InvalidOperationException("Unexpected non-positive span length: " + output.Length);

                            var copySize = Math.Min(input.Length, output.Length);

                            input.Slice(0, copySize).CopyTo(output);

                            input = input.Slice(copySize);

                            writer.Advance(copySize);
                        }
                    }

                    var decompressTask = Task.Run(async () =>
                    {
                        var buffer = ArrayPool<byte>.Shared.Rent(64 * 1024);

                        using (var fileStream = OpenMsgPackFileForRead(fileInfo))
                        using (var decodeStream = new DeflateStream(fileStream, CompressionMode.Decompress))
                        {
                            try
                            {
                                for (; ; )
                                {
                                    var read = await decodeStream.ReadAsync(buffer, 0, buffer.Length, cancellationToken).ConfigureAwait(false);

                                    if (read < 1) break;

                                    totalSize += read;

                                    CopyToWriter(read, buffer);

                                    await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
                                }

                                compressedSize += fileStream.Length;
                            }
                            catch (IOException ex)
                            {
                                needRebuild = true;

                                // The entry might or might not be valid.
                                Debug.WriteLine("MessagePackFileFingerprintStore.LoadBlobsImplAsync() read failed: " + ex.Message);
                            }
                            finally
                            {
                                ArrayPool<byte>.Shared.Return(buffer);

                                writer.Complete();
                            }
                        }
                    }, cancellationToken);

                    var reader = pl.Reader;

                    var deserializeTask = Task.Run(async () =>
                    {
                        var workBuffer = ArrayPool<byte>.Shared.Rent(64 * 1024);

                        try
                        {
                            for (; ; )
                            {
                                if (!reader.TryRead(out var input))
                                    input = await reader.ReadAsync(cancellationToken).ConfigureAwait(false);

                                var buffer = input.Buffer;

                                while (buffer.Length > 32)
                                {
                                    // We now have enough data to start reading.

                                    buffer.Slice(0, 5).CopyTo(workBuffer.AsSpan());

                                    var length = IntFormatter.Deserialize(workBuffer, 0, MessagePackSerializer.DefaultResolver, out var actualSize);

                                    if (length == 0) return; // EOF

                                    if (length < 0 || length > workBuffer.Length) throw new FileFormatException($"Invalid block length {length}");

                                    buffer = buffer.Slice(actualSize);

                                    while (buffer.Length < length)
                                    {
                                        if (input.IsCompleted) return;

                                        reader.AdvanceTo(buffer.Start, buffer.End);

                                        if (!reader.TryRead(out input))
                                            input = await reader.ReadAsync(cancellationToken).ConfigureAwait(false);

                                        buffer = input.Buffer;
                                    }

                                    // We now have enough data to decode the block.

                                    buffer.Slice(0, length).CopyTo(workBuffer.AsSpan());

                                    buffer = buffer.Slice(length);

                                    var fileFingerprint = MessagePackSerializer.Deserialize<FileFingerprint>(workBuffer, 0, MessagePackSerializer.DefaultResolver, out actualSize);

                                    if (length != actualSize) throw new FileFormatException($"Block length mismatch {length} != {actualSize}");

                                    if (blobs.ContainsKey(fileFingerprint.FullFilePath))
                                        Debug.WriteLine($"Collision for {fileFingerprint.FullFilePath}");

                                    blobs[fileFingerprint.FullFilePath] = fileFingerprint;

                                    ++blobCount;
                                }

                                reader.AdvanceTo(buffer.Start, buffer.End);

                                if (input.IsCompleted) break;
                            }
                        }
                        finally
                        {
                            ArrayPool<byte>.Shared.Return(workBuffer);
                            reader.Complete();
                        }
                    });

                    await Task.WhenAll(decompressTask, deserializeTask).ConfigureAwait(false);
                }
                catch (IOException)
                {
                    needRebuild = true;
                }
                catch (InvalidDataException)
                {
                    needRebuild = true;
                }
                catch (FileFormatException)
                {
                    needRebuild = true;
                }
            }

            Debug.WriteLine($"Read {totalSize.BytesToMiB():F2}MiB bytes from {compressedSize.BytesToMiB():F2}MiB file");

            var count = (double)blobs.Count;
            Debug.WriteLine($"Average size {totalSize / count:F1} bytes or {compressedSize / count:F1} compressed");

            if (blobCount > blobs.Count + 100 + blobs.Count / 8)
                needRebuild = true;

            if (fileCount > 16)
                needRebuild = true;

            if (needRebuild)
            {
                Console.WriteLine("Rebuilding cache files");

                await RebuildCacheAsync(blobs, cancellationToken).ConfigureAwait(false);
            }

            return blobs;
        }

        async Task RebuildCacheAsync(Dictionary<string, FileFingerprint> blobs, CancellationToken cancellationToken)
        {
            FileInfo tempFileInfo = null;
            var allOk = false;

            try
            {
                await CloseWriterAsync().ConfigureAwait(false);

                if (blobs.Count > 0)
                {
                    do
                    {
                        var tempFileName = Path.GetRandomFileName();

                        tempFileName = Path.Combine(MsgPackDirectory.FullName, tempFileName);

                        tempFileInfo = new FileInfo(tempFileName);
                    } while (tempFileInfo.Exists);

                    OpenWriter(tempFileInfo);

                    await StoreBlobsImplAsync(blobs.Values, cancellationToken).ConfigureAwait(false);

                    await CloseWriterAsync().ConfigureAwait(false);
                }

                allOk = true;
            }
            catch (OperationCanceledException)
            { }
            catch (Exception ex)
            {
                Console.WriteLine("Unable to truncate corrupt file: " + ex.Message);
            }

            if (allOk)
            {
                foreach (var file in _fileSequence.Files)
                    file.Delete();

                _fileSequence.Rescan();

                tempFileInfo?.MoveTo(_fileSequence.NewFile().FullName);

                _fileSequence.Rescan();
            }
            else
                tempFileInfo?.Delete();
        }

        async Task StoreBlobsImplAsync(ICollection<FileFingerprint> fileFingerprints, CancellationToken cancellationToken)
        {
            if (null == _writer) OpenWriter(_fileSequence.NewFile());

            var count = 0;

            foreach (var fileFingerprint in fileFingerprints)
            {
                if (cancellationToken.IsCancellationRequested)
                    break;

                _writer.Write(fileFingerprint);

                if (++count > 200)
                {
                    count = 0;
                    await _writer.FlushAsync(cancellationToken).ConfigureAwait(false);
                }

                ++UpdateCount;
                UpdateSize += fileFingerprint.Fingerprint.Size;
            }

            await _writer.FlushAsync(cancellationToken).ConfigureAwait(false);

            cancellationToken.ThrowIfCancellationRequested();
        }

        void OpenWriter(FileInfo file)
        {
            Debug.WriteLine("MsgPackFileFingerprintStore.OpenWriter()");

            if (null != _writer) return;

            var writer = new FileFingerprintWriter();

            writer.Open(file);

            _writer = writer;
        }

        async Task CloseWriterAsync()
        {
            Debug.WriteLine("MessagePackFileFingerprintStore.CloseWriter()");

            if (null == _writer) return;

            var writer = _writer;

            _writer = null;

            await writer.CloseAsync(CancellationToken.None).ConfigureAwait(false);

            writer.Dispose();
        }
    }
}
