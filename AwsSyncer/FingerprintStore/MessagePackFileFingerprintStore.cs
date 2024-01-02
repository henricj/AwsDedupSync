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
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AwsSyncer.Types;
using AwsSyncer.Utility;
using MessagePack;
using MessagePack.Formatters;

namespace AwsSyncer.FingerprintStore;

public sealed partial class MessagePackFileFingerprintStore : IFileFingerprintStore
{
    static readonly Dictionary<string, FileFingerprint> EmptyFileFingerprints = [];
    static readonly IMessagePackFormatter<int> IntFormatter = MessagePackSerializer.DefaultOptions.Resolver.GetFormatter<int>();
    readonly FileSequence _fileSequence;
    readonly AsyncLock _lock = new();
    readonly DirectoryInfo _msgPackDirectory;
    FileFingerprintWriter _writer;

    public MessagePackFileFingerprintStore(string bucket)
    {
        _msgPackDirectory = GetMsgPackDirectory(bucket);
        _fileSequence = new(_msgPackDirectory);
    }

    public int UpdateCount { get; private set; }
    public long UpdateSize { get; private set; }

    public async ValueTask DisposeAsync()
    {
        try
        {
            await CloseWriterAsync().ConfigureAwait(false);
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
                return ret.ToFrozenDictionary(StringComparer.Ordinal);
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

    public async Task StoreBlobsAsync(IReadOnlyCollection<FileFingerprint> fileFingerprints, CancellationToken cancellationToken)
    {
        using (await _lock.LockAsync(cancellationToken).ConfigureAwait(false))
        {
            await StoreBlobsImplAsync(fileFingerprints, cancellationToken).ConfigureAwait(false);
        }
    }

    static FileStream OpenMsgPackFileForRead(FileInfo fi) =>
        new(fi.FullName, FileMode.Open,
            FileAccess.Read, FileShare.Read,
            8192, FileOptions.SequentialScan | FileOptions.Asynchronous);

    static FileStream OpenMsgPackFileForWrite(FileInfo fi)
    {
        if (fi.Directory is null)
            throw new InvalidOperationException($"No directory for {fi.FullName}");

        fi.Directory.Refresh();

        if (!fi.Directory.Exists)
            fi.Directory.Create();

        return new(fi.FullName, FileMode.CreateNew,
            FileAccess.Write, FileShare.None,
            8192, FileOptions.SequentialScan | FileOptions.Asynchronous);
    }

    static DirectoryInfo GetMsgPackDirectory(string bucket)
    {
        var localApplicationData = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData,
            Environment.SpecialFolderOption.Create);

        var path = Path.Combine(localApplicationData, "AwsSyncer", "MsgPackPaths");

        if (!string.IsNullOrWhiteSpace(bucket))
            path = Path.Combine(path, bucket);

        return new(path);
    }

    async Task<IReadOnlyDictionary<string, FileFingerprint>> LoadBlobCacheRetryAsync(CancellationToken cancellationToken)
    {
        var delay = 3.0;

        for (var retry = 0; retry < 3; ++retry)
        {
            try
            {
                using (await _lock.LockAsync(cancellationToken).ConfigureAwait(false))
                {
                    return await LoadBlobsImplAsync(cancellationToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Cache read failed: " + ex.Message);
            }

            var rnd = 0.5 * delay * Random.Shared.NextDouble();

            await Task.Delay(TimeSpan.FromSeconds(delay + rnd), cancellationToken).ConfigureAwait(false);

            delay *= 2;
        }

        return null;
    }

    async Task<IReadOnlyDictionary<string, FileFingerprint>> LoadBlobsImplAsync(CancellationToken cancellationToken)
    {
        var blobs = new Dictionary<string, FileFingerprint>(StringComparer.Ordinal);

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

                // Scope
                {
                    var fileStream = OpenMsgPackFileForRead(fileInfo);
                    var decodeStream = new DeflateStream(fileStream, CompressionMode.Decompress);

                    await using (fileStream.ConfigureAwait(false))
                    await using (decodeStream.ConfigureAwait(false))
                    {
                        try
                        {
                            using (var streamReader = new MessagePackStreamReader(decodeStream, true))
                            {
                                while (await streamReader.ReadAsync(cancellationToken) is { } message)
                                {
                                    totalSize += message.Length;

                                    var fileFingerprint =
                                        MessagePackSerializer.Deserialize<FileFingerprint>(message,
                                            cancellationToken: cancellationToken);

                                    if (blobs.ContainsKey(fileFingerprint.FullFilePath))
                                        Debug.WriteLine($"Collision for {fileFingerprint.FullFilePath}");

                                    blobs[fileFingerprint.FullFilePath] = fileFingerprint;

                                    ++blobCount;
                                }
                            }

                            compressedSize += fileStream.Length;
                        }
                        catch (IOException ex)
                        {
                            needRebuild = true;

                            // The entry might or might not be valid.
                            Debug.WriteLine("MessagePackFileFingerprintStore.LoadBlobsImplAsync() read failed: " +
                                ex.Message);
                        }
                    }
                }
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

        if (blobs.Count > 0)
        {
            var count = (double)blobs.Count;
            Debug.WriteLine($"Average size {totalSize / count:F1} bytes or {compressedSize / count:F1} compressed");
        }

        if (blobCount > blobs.Count + 100 + blobs.Count / 8)
            needRebuild = true;

        if (fileCount > 16)
            needRebuild = true;

        if (needRebuild)
        {
            Console.WriteLine("Rebuilding cache files");

            var invalidBlobs = blobs.Where(kv => kv.Value.Invalid).Select(kv => kv.Key).ToArray();
            foreach (var key in invalidBlobs)
                blobs.Remove(key);

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

                    tempFileName = Path.GetFullPath(tempFileName, _msgPackDirectory.FullName);

                    tempFileInfo = new(tempFileName);
                } while (tempFileInfo.Exists);

                OpenWriter(tempFileInfo);

                await StoreBlobsImplAsync(blobs.Select(kv => kv.Value), cancellationToken).ConfigureAwait(false);

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

        if (allOk && tempFileInfo is not null)
        {
            foreach (var file in _fileSequence.Files)
                file.Delete();

            _fileSequence.Rescan();

            tempFileInfo.Refresh();

            tempFileInfo.MoveTo(_fileSequence.NewFile().FullName, true);

            _fileSequence.Rescan();
        }
        else
            tempFileInfo?.Delete();
    }

    async Task StoreBlobsImplAsync(IEnumerable<FileFingerprint> fileFingerprints, CancellationToken cancellationToken)
    {
        if (_writer is null)
            OpenWriter(_fileSequence.NewFile());

        Debug.Assert(_writer is not null);

        var count = 0;

        foreach (var fileFingerprint in fileFingerprints)
        {
            if (cancellationToken.IsCancellationRequested)
                break;

            _writer.Write(fileFingerprint);

            if (++count > 500)
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

        if (_writer is not null)
            return;

        var writer = new FileFingerprintWriter();

        writer.Open(file);

        _writer = writer;
    }

    async Task CloseWriterAsync()
    {
        Debug.WriteLine("MessagePackFileFingerprintStore.CloseWriter()");

        if (_writer is null)
            return;

        var writer = _writer;

        _writer = null;

        await writer.CloseAsync().ConfigureAwait(false);

        writer.Dispose();
    }
}
