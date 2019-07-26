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

#if false
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Threading;
using System.Threading.Tasks;
using AwsSyncer.Types;
using AwsSyncer.Utility;
using Newtonsoft.Json;
using Newtonsoft.Json.Bson;
using ZeroFormatter;

namespace AwsSyncer.FingerprintStore
{
    public sealed class ZeroFileFingerprintStore : IFileFingerprintStore
    {
        static readonly DirectoryInfo CacheDirectory = GetCacheDirectory();
        static readonly Dictionary<string, FileFingerprint> EmptyFileFingerprints = new Dictionary<string, FileFingerprint>();
        readonly FileSequence _fileSequence;
        readonly AsyncLock _lock = new AsyncLock();
        readonly FileFingerprintSerializer _serializer = new FileFingerprintSerializer();
        Stream _bufferStream;
        Stream _cacheFile;
        DeflateStream _encodeStream;

        public ZeroFileFingerprintStore() => _fileSequence = new FileSequence(CacheDirectory);

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

        public async Task FlushAsync(CancellationToken cancellationToken)
        {
            using (await _lock.LockAsync(cancellationToken).ConfigureAwait(false))
            {
                if (null == _bufferStream)
                    return;

                await _bufferStream.FlushAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        public async Task StoreBlobsAsync(ICollection<FileFingerprint> fileFingerprints, CancellationToken cancellationToken)
        {
            using (await _lock.LockAsync(cancellationToken).ConfigureAwait(false)) await StoreBlobsImplAsync(fileFingerprints, cancellationToken).ConfigureAwait(false);
        }

        static Stream OpenCacheFileForRead(FileInfo fi)
            => new FileStream(fi.FullName, FileMode.Open,
            FileAccess.Read, FileShare.Read,
            8192, FileOptions.SequentialScan | FileOptions.Asynchronous);

        static Stream OpenCacheFileForWrite(FileInfo fi)
        {
            if (!CacheDirectory.Exists)
                CacheDirectory.Create();

            return new FileStream(fi.FullName, FileMode.CreateNew,
                FileAccess.Write, FileShare.None,
                8192, FileOptions.SequentialScan | FileOptions.Asynchronous);
        }

        static DirectoryInfo GetCacheDirectory()
        {
            var localApplicationData = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData,
                Environment.SpecialFolderOption.Create);

            var path = Path.Combine(localApplicationData, "AwsSyncer", "CachePaths");

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
                    using (await _lock.LockAsync(cancellationToken).ConfigureAwait(false)) return await LoadBlobsImplAsync(cancellationToken).ConfigureAwait(false);
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

                    using (var fileStream = OpenCacheFileForRead(fileInfo))
                    using (var decodeStream = new DeflateStream(fileStream, CompressionMode.Decompress))
                    using (var bs = new SequentialReadStream(decodeStream))
                    {
                        while (await br.ReadAsync(cancellationToken).ConfigureAwait(false))
                        {
                            cancellationToken.ThrowIfCancellationRequested();

                            try
                            {
                                var fileFingerprint = await ReadFileFingerprintAsync(br, cancellationToken).ConfigureAwait(false);

                                if (null == fileFingerprint)
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
                    if (null == path || !modified.HasValue || !size.HasValue ||
                        null == md5 || null == sha256 || null == sha3_512)
                        throw new JsonReaderException("Missing required property");

                    var fingerprint = new BlobFingerprint(size.Value, sha3_512, sha256, md5);

                    return new FileFingerprint(path, modified.Value, fingerprint, true);
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

        async Task RebuildCacheAsync(Dictionary<string, FileFingerprint> blobs, CancellationToken cancellationToken)
        {
            FileInfo tempFileInfo = null;
            var allOk = false;

            try
            {
                do
                {
                    var tempFileName = Path.GetRandomFileName();

                    tempFileName = Path.Combine(CacheDirectory.FullName, tempFileName);

                    tempFileInfo = new FileInfo(tempFileName);
                } while (tempFileInfo.Exists);

                CloseWriter();

                _cacheFile = OpenCacheFileForWrite(tempFileInfo);

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

                await _serializer.WriteAsync(_bufferStream, blob, cancellationToken).ConfigureAwait(false);

                ++UpdateCount;
                UpdateSize += blob.Fingerprint.Size;
            }

            cancellationToken.ThrowIfCancellationRequested();
        }

        void OpenWriter()
        {
            Debug.WriteLine("BsonFileFingerprintStore.OpenWriter()");

            if (null == _cacheFile)
            {
                var fi = _fileSequence.NewFile();
                _cacheFile = OpenCacheFileForWrite(fi);
            }

            if (null == _encodeStream)
                _encodeStream = new DeflateStream(_cacheFile, CompressionLevel.Optimal);

            if (null == _bufferStream)
                _bufferStream = new BufferedStream(_encodeStream, 512 * 1024);
        }

        void CloseWriter()
        {
            Debug.WriteLine("BsonFileFingerprintStore.CloseWriter()");

            var bufferStream = _bufferStream;
            _bufferStream = null;

            var encodeStream = _encodeStream;
            _encodeStream = null;

            var cacheFile = _cacheFile;
            _cacheFile = null;

            if (null != bufferStream)
            {
                try
                {
                    bufferStream.Flush();
                }
                catch (Exception ex)
                {
                    Debug.WriteLine("CloseWriter flush failed: " + ex.Message);
                }

                bufferStream.Close();
            }

            bufferStream?.Close();

            encodeStream?.Close();

            cacheFile?.Close();
        }
    }

    sealed class FileFingerprintSerializer
    {
        readonly ProtocolAdapter _objectAdapter = new ProtocolAdapter();

        byte[] _objectBuffer;

        public Task WriteAsync(Stream writer, FileFingerprint blob, CancellationToken cancellationToken)
        {
            _objectAdapter.FileFingerprint = blob;

            var size = ZeroFormatterSerializer.Serialize(ref _objectBuffer, 0, _objectAdapter);

            return writer.WriteAsync(_objectBuffer, 0, size, cancellationToken);
        }

        [ZeroFormattable]
        class Protocol
        {
            [Index(0)]
            public virtual string FullFilePath { get; set; }

            [Index(1)]
            public virtual DateTime LastModifiedUtc { get; set; }

            [Index(2)]
            public virtual long Size { get; set; }

            [Index(3)]
            public virtual byte[] Sha3_512 { get; set; }

            [Index(4)]
            public virtual byte[] Sha2_256 { get; set; }

            [Index(5)]
            public virtual byte[] Md5 { get; set; }
        }

        sealed class ProtocolAdapter : Protocol
        {
            public FileFingerprint FileFingerprint { get; set; }

            public override string FullFilePath => FileFingerprint.FullFilePath;
            public override DateTime LastModifiedUtc => FileFingerprint.LastModifiedUtc;
            public override long Size => FileFingerprint.Fingerprint.Size;
            public override byte[] Sha3_512 => FileFingerprint.Fingerprint.Sha3_512;
            public override byte[] Sha2_256 => FileFingerprint.Fingerprint.Sha2_256;
            public override byte[] Md5 => FileFingerprint.Fingerprint.Md5;
        }
    }
}
#endif // false
