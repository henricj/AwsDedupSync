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

using Amazon.S3;
using Amazon.S3.Model;
using AwsSyncer.Types;
using AwsSyncer.Utility;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace AwsSyncer.AWS
{
    public sealed class S3Blobs : S3PutBase
    {
        const int KeyLength = 512 / 8;
        readonly IPathManager _pathManager;
        readonly S3StorageClass _s3StorageClass;

        public S3Blobs(IAmazonS3 amazon, IPathManager pathManager, S3StorageClass s3StorageClass)
            : base(amazon)
        {
            _pathManager = pathManager ?? throw new ArgumentNullException(nameof(pathManager));
            _s3StorageClass = s3StorageClass ?? throw new ArgumentNullException(nameof(s3StorageClass));
        }

        public async Task<IReadOnlyDictionary<byte[], string>> ListAsync(Statistics statistics, CancellationToken cancellationToken)
        {
            if (null == S3Util.KeyAlphabet)
                return await ListAsync(_pathManager.BlobPrefix, statistics, cancellationToken).ConfigureAwait(false);

            var tasks = S3Util.KeyAlphabet
                .Select(ch => ListAsync(_pathManager.BlobPrefix + ch, statistics, cancellationToken))
                .ToArray();

            await Task.WhenAll(tasks).ConfigureAwait(false);

            return tasks.SelectMany(t => t.Result).ToDictionary(kv => kv.Key, kv => kv.Value, ByteArrayComparer.Instance);
        }

        async Task<Dictionary<byte[], string>> ListAsync(string prefix, Statistics statistics, CancellationToken cancellationToken)
        {
            var keys = new Dictionary<byte[], string>(ByteArrayComparer.Instance);

            var request = new ListObjectsV2Request
            {
                BucketName = _pathManager.Bucket,
                Prefix = prefix,
                Delimiter = "/"
            };

            for (; ; )
            {
                var response = await AmazonS3.ListObjectsV2Async(request, cancellationToken).ConfigureAwait(false);

                foreach (var s3Object in response.S3Objects)
                {
                    var key = _pathManager.GetKeyFromBlobPath(s3Object.Key);

                    // Ignore the folder itself (if there is an empty object there).
                    if (null == key)
                        continue;

                    if (null != statistics)
                    {
                        Interlocked.Increment(ref statistics.Count);
                        Interlocked.Add(ref statistics.TotalSize, s3Object.Size);
                    }

                    if (key.Length != KeyLength)
                    {
                        // Compensate for past trouble...
                        if (KeyLength + 1 == key.Length && 13 == key[KeyLength])
                        {
                            Console.WriteLine($"Object key {s3Object.Key} has trailing carriage return.");

                            var newKey = key[..KeyLength].ToArray();

                            key = newKey;
                        }
                        else
                        {
                            var metadata = await AmazonS3
                                .GetObjectMetadataAsync(_pathManager.Bucket, s3Object.Key, cancellationToken).ConfigureAwait(false);

                            var alternateKeyHeader = metadata.Metadata["x-amz-meta-sha3-512"];

                            if (string.IsNullOrWhiteSpace(alternateKeyHeader))
                            {
                                Console.WriteLine($"Invalid key for object {s3Object.Key}");
                                continue;
                            }

                            PathUtil.RequireNormalizedAsciiName(alternateKeyHeader);

                            var newKey = S3Util.DecodeKey(alternateKeyHeader);

                            key = newKey;
                        }
                    }

                    keys[key] = s3Object.ETag;
                }

                if (!response.IsTruncated)
                    return keys;

                request.ContinuationToken = response.NextContinuationToken;
            }
        }

        public async Task UploadBlobAsync(IUploadBlobRequest uploadBlobRequest, CancellationToken cancellationToken)
        {
            var request = (UploadBlobRequest)uploadBlobRequest;

            Debug.WriteLine($"S3Blobs.UploadBlobAsync() {request.FileFingerprint.FullFilePath} ({request.FileFingerprint.Fingerprint.Key()[..12]})");

            var putObjectRequest = request.Request;

            var bufferSize = (int)Math.Min(81920, Math.Max(1024, request.FileFingerprint.Fingerprint.Size));

            var s = new FileStream(uploadBlobRequest.FileFingerprint.FullFilePath,
                FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize,
                FileOptions.Asynchronous | FileOptions.SequentialScan);

            await using (s.ConfigureAwait(false))
            {
                putObjectRequest.InputStream = s;

                await PutAsync(request, cancellationToken).ConfigureAwait(false);
            }
        }

        public IUploadBlobRequest BuildUploadBlobRequest(Tuple<FileFingerprint, AnnotatedPath> tuple)
        {
            Debug.WriteLine($"S3Blobs.BuildUploadRequest() {tuple.Item1.FullFilePath} ({tuple.Item1.Fingerprint.Key()[..12]})");

            var blobKey = _pathManager.GetBlobPath(tuple.Item1);

            var fullFilePath = tuple.Item2.FileInfo.FullName;

            var fileInfo = new FileInfo(fullFilePath);

            var fingerprint = tuple.Item1.Fingerprint;

            if (fileInfo.Length != fingerprint.Size || fileInfo.LastWriteTimeUtc != tuple.Item1.LastModifiedUtc)
            {
                Debug.WriteLine($"BuildUploadRequest(): {fileInfo.FullName} changed {fingerprint.Size} != {fileInfo.Length} || {tuple.Item1.LastModifiedUtc} != {fileInfo.LastWriteTimeUtc} ({tuple.Item1.LastModifiedUtc - fileInfo.LastWriteTimeUtc})");

                return null;
            }

            var md5Digest = Convert.ToBase64String(fingerprint.Md5);

            var base64Sha256 = Convert.ToBase64String(fingerprint.Sha2_256);

            var request = new PutObjectRequest
            {
                BucketName = _pathManager.Bucket,
                Key = blobKey,
                MD5Digest = md5Digest,
                Headers =
                {
                    ContentType = MimeDetector.GetMimeType(fullFilePath),
                    ContentLength = fingerprint.Size,
                    ContentMD5 = md5Digest,
                    ["x-amz-meta-lastModified"] = tuple.Item1.LastModifiedUtc.ToString("O"),
                    ["x-amz-meta-sha2-256"] = base64Sha256,
                    ["x-amz-meta-sha3-512"] = Convert.ToBase64String(fingerprint.Sha3_512)
                },
                StorageClass = _s3StorageClass,
                AutoCloseStream = false,
                AutoResetStreamPosition = false,
                ChecksumSHA256 = base64Sha256
            };

            if (!string.IsNullOrEmpty(tuple.Item2.Collection))
                request.Headers["x-amz-meta-original-collection"] = tuple.Item2.Collection;

            if (!string.IsNullOrEmpty(tuple.Item2.RelativePath))
                request.Headers["x-amz-meta-original-path"] = tuple.Item2.RelativePath;

            var fileName = Path.GetFileName(fullFilePath);

            if (!string.IsNullOrWhiteSpace(fileName))
            {
                fileName = PathUtil.NormalizeAsciiName(fileName);
                request.Headers["x-amz-meta-original-name"] = fileName;
                request.Headers.ContentDisposition = "attachment; filename=" + fileName;
            }

            return new UploadBlobRequest
            {
                Request = request,
                FileFingerprint = tuple.Item1,
                ETag = S3Util.ComputeS3Etag(fingerprint.Md5),
                Key = blobKey
            };
        }

        public class Statistics
        {
            public long Count;
            public long TotalSize;

            public void Clear()
            {
                Count = 0;
                TotalSize = 0;
            }
        }

        public interface IUploadBlobRequest
        {
            string Key { get; }
            FileFingerprint FileFingerprint { get; }
        }

        class UploadBlobRequest : S3PutRequest, IUploadBlobRequest
        {
            public string Key { get; set; }
            public FileFingerprint FileFingerprint { get; set; }
        }
    }
}
