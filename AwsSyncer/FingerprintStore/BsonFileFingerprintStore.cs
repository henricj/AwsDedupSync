// Copyright (c) 2016-2017 Henric Jungheim <software@henric.org>
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
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Threading;
using System.Threading.Tasks;
using AwsSyncer.Types;
using AwsSyncer.Utility;
using Newtonsoft.Json;
using Newtonsoft.Json.Bson;

namespace AwsSyncer.FingerprintStore;

public sealed class BsonFileFingerprintStore : IFileFingerprintStore
{
    static readonly Dictionary<string, FileFingerprint> EmptyFileFingerprints = [];
    readonly DirectoryInfo _bsonDirectory;
    readonly FileSequence _fileSequence;
    readonly AsyncLock _lock = new();
    Stream _bsonFile;
    Stream _bufferStream;
    DeflateStream _encodeStream;
    BsonDataWriter _jsonWriter;

    public BsonFileFingerprintStore(string bucket)
    {
        _bsonDirectory = GetBsonDirectory(bucket);
        _fileSequence = new(_bsonDirectory);
    }

    public int UpdateCount { get; private set; }
    public long UpdateSize { get; private set; }

    public void Dispose()
    {
        try
        {
            CloseWriter();
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
        catch (JsonReaderException)
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
            if (_jsonWriter is null)
                return;

            await _jsonWriter.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    public async Task StoreBlobsAsync(ICollection<FileFingerprint> fileFingerprints, CancellationToken cancellationToken)
    {
        using (await _lock.LockAsync(cancellationToken).ConfigureAwait(false))
        {
            await StoreBlobsImplAsync(fileFingerprints, cancellationToken).ConfigureAwait(false);
        }
    }

    static FileStream OpenBsonFileForRead(FileInfo fi) =>
        new(fi.FullName, FileMode.Open,
            FileAccess.Read, FileShare.Read,
            8192, FileOptions.SequentialScan | FileOptions.Asynchronous);

    FileStream OpenBsonFileForWrite(FileInfo fi)
    {
        if (!_bsonDirectory.Exists)
            _bsonDirectory.Create();

        return new(fi.FullName, FileMode.CreateNew,
            FileAccess.Write, FileShare.None,
            8192, FileOptions.SequentialScan | FileOptions.Asynchronous);
    }

    static DirectoryInfo GetBsonDirectory(string bucket)
    {
        var localApplicationData = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData,
            Environment.SpecialFolderOption.Create);

        var path = Path.Combine(localApplicationData, "AwsSyncer", "BsonPaths");

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
                    continue;

                var fileStream = OpenBsonFileForRead(fileInfo);
                var decodeStream = new DeflateStream(fileStream, CompressionMode.Decompress);
                var bs = new SequentialReadStream(decodeStream);

                await using (fileStream.ConfigureAwait(false))
                await using (decodeStream.ConfigureAwait(false))
                await using (fileStream.ConfigureAwait(false))
                {
                    using var br = new BsonDataReader(bs);
                    br.DateTimeKindHandling = DateTimeKind.Utc;
                    br.SupportMultipleContent = true;

                    while (await br.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        try
                        {
                            var fileFingerprint = await ReadFileFingerprintAsync(br, cancellationToken).ConfigureAwait(false);

                            if (fileFingerprint is null)
                            {
                                needRebuild = true;
                                break;
                            }

                            if (blobs.ContainsKey(fileFingerprint.FullFilePath))
                                Debug.WriteLine($"Collision for {fileFingerprint.FullFilePath}");

                            blobs[fileFingerprint.FullFilePath] = fileFingerprint;

                            ++blobCount;
                        }
                        catch (IOException ex)
                        {
                            needRebuild = true;

                            // The entry might or might not be valid.
                            Debug.WriteLine("BsonFileFingerprintStore.LoadBlobsImplAsync() read failed: " + ex.Message);
                        }
                    }

                    totalSize += bs.Position;
                    compressedSize += fileStream.Length;
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
            catch (JsonException)
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

    static async Task<FileFingerprint> ReadFileFingerprintAsync(JsonReader reader, CancellationToken cancellationToken)
    {
        if (JsonToken.StartObject != reader.TokenType)
            throw new JsonReaderException("State not object");

        string path = null;
        DateTime? modified = null;
        long? size = null;
        byte[] md5 = null;
        byte[] sha256 = null;
        byte[] sha3_512 = null;

        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (reader.TokenType == JsonToken.EndObject)
            {
                if (path is null || !modified.HasValue || !size.HasValue ||
                    md5 is null || sha256 is null || sha3_512 is null)
                    throw new JsonReaderException("Missing required property");

                var fingerprint = new BlobFingerprint(size.Value, sha3_512, sha256, md5);

                return new(path, modified.Value, fingerprint, true);
            }

            if (reader.TokenType != JsonToken.PropertyName)
                throw new JsonReaderException("Missing property name");

            var name = (string)reader.Value;

            if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                throw new JsonReaderException("Missing property value for " + name);

            switch (name)
            {
                case "path":
                    path = (string)reader.Value;
                    break;
                case "modified":
                    modified = DateTime.FromBinary((long)reader.Value);
                    break;
                case "size":
                    size = (long)reader.Value;
                    break;
                case "sha256":
                    sha256 = (byte[])reader.Value;
                    break;
                case "md5":
                    md5 = (byte[])reader.Value;
                    break;
                case "sha3_512":
                    sha3_512 = (byte[])reader.Value;
                    break;
                default:
                    Debug.WriteLine("Unknown name: " + name);
                    break;
            }
        }

        return null;
    }

    static async Task WriteFileFingerprintAsync(JsonWriter writer, FileFingerprint blob, CancellationToken cancellationToken)
    {
        if (writer.WriteState != WriteState.Start)
            throw new JsonWriterException("State not closed");

        await writer.WriteStartObjectAsync(cancellationToken).ConfigureAwait(false);

        await writer.WritePropertyNameAsync("path", cancellationToken).ConfigureAwait(false);
        await writer.WriteValueAsync(blob.FullFilePath, cancellationToken).ConfigureAwait(false);
        await writer.WritePropertyNameAsync("modified", cancellationToken).ConfigureAwait(false);
        await writer.WriteValueAsync(blob.LastModifiedUtc.ToBinary(), cancellationToken).ConfigureAwait(false);

        var fingerprint = blob.Fingerprint;
        await writer.WritePropertyNameAsync("size", cancellationToken).ConfigureAwait(false);
        await writer.WriteValueAsync(fingerprint.Size, cancellationToken).ConfigureAwait(false);
        await writer.WritePropertyNameAsync("sha256", cancellationToken).ConfigureAwait(false);
        await writer.WriteValueAsync(fingerprint.Sha2_256, cancellationToken).ConfigureAwait(false);
        await writer.WritePropertyNameAsync("md5", cancellationToken).ConfigureAwait(false);
        await writer.WriteValueAsync(fingerprint.Md5, cancellationToken).ConfigureAwait(false);
        await writer.WritePropertyNameAsync("sha3_512", cancellationToken).ConfigureAwait(false);
        await writer.WriteValueAsync(fingerprint.Sha3_512, cancellationToken).ConfigureAwait(false);

        await writer.WriteEndObjectAsync(cancellationToken);
    }

    async Task RebuildCacheAsync(Dictionary<string, FileFingerprint> blobs, CancellationToken cancellationToken)
    {
        FileInfo tempFileInfo = null;
        var allOk = false;

        try
        {
            do
            {
                var tempFileName = Path.GetRandomFileName();

                tempFileName = Path.Combine(_bsonDirectory.FullName, tempFileName);

                tempFileInfo = new(tempFileName);
            } while (tempFileInfo.Exists);

            CloseWriter();

            _bsonFile = OpenBsonFileForWrite(tempFileInfo);

            await StoreBlobsImplAsync(blobs.Values, cancellationToken).ConfigureAwait(false);

            CloseWriter();

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

            tempFileInfo.MoveTo(_fileSequence.NewFile().FullName);

            _fileSequence.Rescan();
        }
        else
            tempFileInfo?.Delete();
    }

    async Task StoreBlobsImplAsync(ICollection<FileFingerprint> fileFingerprints, CancellationToken cancellationToken)
    {
        OpenWriter();

        foreach (var blob in fileFingerprints)
        {
            if (cancellationToken.IsCancellationRequested)
                break;

            await WriteFileFingerprintAsync(_jsonWriter, blob, cancellationToken).ConfigureAwait(false);

            ++UpdateCount;
            UpdateSize += blob.Fingerprint.Size;
        }

        cancellationToken.ThrowIfCancellationRequested();
    }

    void OpenWriter()
    {
        Debug.WriteLine("BsonFileFingerprintStore.OpenWriter()");

        if (_bsonFile is null)
        {
            var fi = _fileSequence.NewFile();
            _bsonFile = OpenBsonFileForWrite(fi);
        }

        _encodeStream ??= new(_bsonFile, CompressionLevel.Optimal);

        _bufferStream ??= new BufferedStream(_encodeStream, 512 * 1024);

        _jsonWriter ??= new(_bufferStream) { DateTimeKindHandling = DateTimeKind.Utc };
    }

    void CloseWriter()
    {
        Debug.WriteLine("BsonFileFingerprintStore.CloseWriter()");

        var jsonWriter = _jsonWriter;
        _jsonWriter = null;

        var bufferStream = _bufferStream;
        _bufferStream = null;

        var encodeStream = _encodeStream;
        _encodeStream = null;

        var bsonFile = _bsonFile;
        _bsonFile = null;

        if (null != jsonWriter)
        {
            jsonWriter.Flush();

            jsonWriter.Close();
        }

        bufferStream?.Close();

        encodeStream?.Close();

        bsonFile?.Close();
    }
}
