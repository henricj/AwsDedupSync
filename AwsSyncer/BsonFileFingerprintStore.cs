// Copyright (c) 2016 Henric Jungheim <software@henric.org>
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
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Bson;
using SevenZip;

namespace AwsSyncer
{
    public sealed class BsonFileFingerprintStore : IDisposable
    {
        static readonly DirectoryInfo BsonDirectory = GetBsonDirectory();
        static readonly Dictionary<string, IFileFingerprint> EmptyFileFingerprints = new Dictionary<string, IFileFingerprint>();
        readonly FileSequence _fileSequence;
        readonly AsyncLock _lock = new AsyncLock();
        Stream _bsonFile;
        Stream _bufferStream;
        BsonWriter _jsonWriter;
        LzmaEncodeStream _lzmaEncodeStream;

        public BsonFileFingerprintStore()
        {
            _fileSequence = new FileSequence(BsonDirectory);
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

        static Stream OpenBsonFileForRead(FileInfo fi)
        {
            return new FileStream(fi.FullName, FileMode.Open,
                FileAccess.Read, FileShare.Read,
                8192, FileOptions.SequentialScan);
        }

        static Stream OpenBsonFileForWrite(FileInfo fi)
        {
            if (!BsonDirectory.Exists)
                BsonDirectory.Create();

            return new FileStream(fi.FullName, FileMode.CreateNew,
                FileAccess.Write, FileShare.None,
                8192, FileOptions.SequentialScan);
        }

        static DirectoryInfo GetBsonDirectory()
        {
            var localApplicationData = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData,
                Environment.SpecialFolderOption.Create);

            var path = Path.Combine(localApplicationData, "AwsSyncer", "BsonPaths");

            var bucket = ConfigurationManager.AppSettings["bucket"];

            if (!string.IsNullOrWhiteSpace(bucket))
                path = Path.Combine(path, bucket);

            return new DirectoryInfo(path);
        }

        public async Task<IReadOnlyDictionary<string, IFileFingerprint>> LoadBlobsAsync(CancellationToken cancellationToken)
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
                RebuildCache(EmptyFileFingerprints);
            }
            catch (IOException ex)
            {
                Console.WriteLine("LoadBlobsAsync() delete failed: " + ex.Message);
            }

            return EmptyFileFingerprints;
        }

        async Task<IReadOnlyDictionary<string, IFileFingerprint>> LoadBlobCacheRetryAsync(CancellationToken cancellationToken)
        {
            var delay = 3.0;

            for (var retry = 0; retry < 3; ++retry)
            {
                try
                {
                    using (await _lock.LockAsync(cancellationToken).ConfigureAwait(false))
                    {
                        return LoadBlobsImpl(cancellationToken);
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

        IReadOnlyDictionary<string, IFileFingerprint> LoadBlobsImpl(CancellationToken cancellationToken)
        {
            var blobs = new Dictionary<string, IFileFingerprint>();

            var needRebuild = false;

            var blobCount = 0;

            _fileSequence.Rescan();

            foreach (var fileInfo in _fileSequence.Files)
            {
                try
                {
                    fileInfo.Refresh();

                    if (fileInfo.Length < 5)
                        continue;

                    using (var fileStream = OpenBsonFileForRead(fileInfo))
                    using (var lzmaDecodeStream = new LzmaDecodeStream(fileStream))
                    using (var br = new BsonReader(lzmaDecodeStream) { DateTimeKindHandling = DateTimeKind.Utc, SupportMultipleContent = true })
                    {
                        while (br.Read())
                        {
                            cancellationToken.ThrowIfCancellationRequested();

                            try
                            {
                                var fileFingerprint = ReadFileFingerprint(br);

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
                                Debug.WriteLine("BsonFileFingerprintStore.LoadBlobsImpl() read failed: " + ex.Message);
                            }
                        }
                    }
                }
                catch (IOException)
                {
                    needRebuild = true;
                }
                catch (LzmaException)
                {
                    needRebuild = true;
                }
                catch (JsonException)
                {
                    needRebuild = true;
                }
            }

            if (blobCount > blobs.Count + 100 + blobs.Count / 8)
                needRebuild = true;

            if (needRebuild)
            {
                Console.WriteLine("Rebuilding cache files");

                RebuildCache(blobs);
            }

            return blobs;
        }

        IFileFingerprint ReadFileFingerprint(JsonReader reader)
        {
            if (JsonToken.StartObject != reader.TokenType)
                throw new JsonReaderException("State not object");

            string path = null;
            DateTime? modified = null;
            long? size = null;
            byte[] md5 = null;
            byte[] sha256 = null;
            byte[] sha3_512 = null;

            while (reader.Read())
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

                if (!reader.Read())
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
                }
            }

            return null;
        }

        void WriteFileFingerprint(JsonWriter writer, IFileFingerprint blob)
        {
            if (writer.WriteState != WriteState.Start)
                throw new JsonWriterException("State not closed");

            writer.WriteStartObject();

            writer.WritePropertyName("path");
            writer.WriteValue(blob.FullFilePath);
            writer.WritePropertyName("modified");
            writer.WriteValue(blob.LastModifiedUtc.ToBinary());

            var fingerprint = blob.Fingerprint;
            writer.WritePropertyName("size");
            writer.WriteValue(fingerprint.Size);
            writer.WritePropertyName("sha256");
            writer.WriteValue(fingerprint.Sha2_256);
            writer.WritePropertyName("md5");
            writer.WriteValue(fingerprint.Md5);
            writer.WritePropertyName("sha3_512");
            writer.WriteValue(fingerprint.Sha3_512);

            writer.WriteEndObject();
        }

        void RebuildCache(Dictionary<string, IFileFingerprint> blobs)
        {
            FileInfo tempFileInfo = null;
            var allOk = false;

            try
            {
                do
                {
                    var tempFileName = Path.GetRandomFileName();

                    tempFileName = Path.Combine(BsonDirectory.FullName, tempFileName);

                    tempFileInfo = new FileInfo(tempFileName);
                } while (tempFileInfo.Exists);

                CloseWriter();

                _bsonFile = OpenBsonFileForWrite(tempFileInfo);

                StoreBlobsImpl(blobs.Values, CancellationToken.None);

                CloseWriter();

                allOk = true;
            }
            catch (OperationCanceledException)
            { }
            catch (Exception)
            {
                Console.WriteLine($"Unable to truncate corrupt file");
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
            {
                tempFileInfo?.Delete();
            }
        }

        void StoreBlobsImpl(ICollection<IFileFingerprint> fileFingerprints, CancellationToken cancellationToken)
        {
            OpenWriter();

            //var serializer = CreateSerializer();

            foreach (var blob in fileFingerprints)
            {
                if (cancellationToken.IsCancellationRequested)
                    break;

                WriteFileFingerprint(_jsonWriter, blob);

                ++UpdateCount;
                UpdateSize += blob.Fingerprint.Size;
            }

            cancellationToken.ThrowIfCancellationRequested();
        }

        void OpenWriter()
        {
            Debug.WriteLine("BsonFileFingerprintStore.OpenWriter()");

            if (null == _bsonFile)
            {
                var fi = _fileSequence.NewFile();
                _bsonFile = OpenBsonFileForWrite(fi);
            }

            if (null == _lzmaEncodeStream)
                _lzmaEncodeStream = new LzmaEncodeStream(_bsonFile);

            if (null == _bufferStream)
                _bufferStream = new BufferedStream(_lzmaEncodeStream, 512 * 1024);

            if (null == _jsonWriter)
                _jsonWriter = new BsonWriter(_bufferStream) { DateTimeKindHandling = DateTimeKind.Utc };
        }

        void CloseWriter()
        {
            Debug.WriteLine("BsonFileFingerprintStore.CloseWriter()");

            var jsonWriter = _jsonWriter;
            _jsonWriter = null;

            var bufferStream = _bufferStream;
            _bufferStream = null;

            var lzmaEncodeStream = _lzmaEncodeStream;
            _lzmaEncodeStream = null;

            var bsonFile = _bsonFile;
            _bsonFile = null;

            if (null != jsonWriter)
            {
                jsonWriter.Flush();

                jsonWriter.Close();
            }

            bufferStream?.Close();

            lzmaEncodeStream?.Close();

            bsonFile?.Close();
        }

        public async Task FlushAsync(CancellationToken cancellationToken)
        {
            using (await _lock.LockAsync(cancellationToken).ConfigureAwait(false))
            {
                _jsonWriter?.Flush();
            }
        }

        public async Task StoreBlobsAsync(ICollection<IFileFingerprint> fileFingerprints, CancellationToken cancellationToken)
        {
            using (await _lock.LockAsync(cancellationToken).ConfigureAwait(false))
            {
                StoreBlobsImpl(fileFingerprints, cancellationToken);
            }
        }
    }
}
