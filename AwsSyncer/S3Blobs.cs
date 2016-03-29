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
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.AccessControl;
using System.Threading;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;

namespace AwsSyncer
{
    public sealed class S3Blobs : S3PutBase
    {
        readonly IPathManager _pathManager;
        readonly S3StorageClass _s3StorageClass;

        public S3Blobs(IAmazonS3 amazon, IPathManager pathManager, S3StorageClass s3StorageClass)
            : base(amazon)
        {
            if (null == pathManager)
                throw new ArgumentNullException(nameof(pathManager));
            if (null == s3StorageClass)
                throw new ArgumentNullException(nameof(s3StorageClass));

            _pathManager = pathManager;
            _s3StorageClass = s3StorageClass;
        }

        public async Task<IReadOnlyDictionary<string, string>> ListAsync(Statistics statistics, CancellationToken cancellationToken)
        {
            if (null == S3Util.KeyAlphabet)
                return await ListAsync(_pathManager.BlobPrefix, statistics, cancellationToken).ConfigureAwait(false);

            var tasks = S3Util.KeyAlphabet
                .Select(ch => ListAsync(_pathManager.BlobPrefix + ch, statistics, cancellationToken))
                .ToArray();

            await Task.WhenAll(tasks).ConfigureAwait(false);

            return tasks.SelectMany(t => t.Result).ToDictionary(kv => kv.Key, kv => kv.Value);
        }

        async Task<Dictionary<string, string>> ListAsync(string prefix, Statistics statistics, CancellationToken cancellationToken)
        {
            var keys = new Dictionary<string, string>();

            var request = new ListObjectsRequest
            {
                BucketName = _pathManager.Bucket,
                Prefix = prefix,
                Delimiter = "/"
            };

            for (;;)
            {
                var response = await AmazonS3.ListObjectsAsync(request, cancellationToken).ConfigureAwait(false);

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

                    keys[key] = s3Object.ETag;
                }

                if (!response.IsTruncated)
                    return keys;

                request.Marker = response.NextMarker;
            }
        }

        public async Task UploadBlobAsync(IUploadBlobRequest uploadBlobRequest, CancellationToken cancellationToken)
        {
            var request = (UploadBlobRequest)uploadBlobRequest;

            Debug.WriteLine($"S3Blobs.UploadBlobAsync() {request.FileFingerprint.FullFilePath} ({request.FileFingerprint.Fingerprint.Key().Substring(0, 12)})");

            var putObjectRequest = request.Request;

            var bufferSize = (int)Math.Min(81920, Math.Max(1024, request.FileFingerprint.Fingerprint.Size));

            using (var s = new FileStream(uploadBlobRequest.FileFingerprint.FullFilePath,
                FileMode.Open, FileSystemRights.Read, FileShare.Read,
                bufferSize,
                FileOptions.Asynchronous | FileOptions.SequentialScan))
            {
                putObjectRequest.InputStream = s;

                await PutAsync(request, cancellationToken).ConfigureAwait(false);
            }
        }

        public IUploadBlobRequest BuildUploadBlobRequest(Tuple<IFileFingerprint, AnnotatedPath> tuple)
        {
            Debug.WriteLine($"S3Blobs.BuildUploadRequest() {tuple.Item1.FullFilePath} ({tuple.Item1.Fingerprint.Key().Substring(0, 12)})");

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

            var request = new PutObjectRequest
            {
                BucketName = _pathManager.Bucket,
                Key = blobKey,
                MD5Digest = md5Digest,
                Headers =
                {
                    ContentType = MimeDetector.Default.GetMimeType(fullFilePath),
                    ContentLength = fingerprint.Size,
                    ContentMD5 = md5Digest
                },
                StorageClass = _s3StorageClass,
                AutoCloseStream = false,
                AutoResetStreamPosition = false
            };

            request.Headers["x-amz-meta-lastModified"] = tuple.Item1.LastModifiedUtc.ToString("O");
            request.Headers["x-amz-meta-SHA2-256"] = Convert.ToBase64String(fingerprint.Sha2_256);
            request.Headers["x-amz-meta-SHA3-512"] = Convert.ToBase64String(fingerprint.Sha3_512);

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
            IFileFingerprint FileFingerprint { get; }
        }

        class UploadBlobRequest : S3PutRequest, IUploadBlobRequest
        {
            public string Key { get; set; }
            public IFileFingerprint FileFingerprint { get; set; }
        }
    }
}
