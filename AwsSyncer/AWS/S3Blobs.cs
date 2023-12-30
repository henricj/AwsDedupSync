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
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;
using AwsSyncer.Types;
using AwsSyncer.Utility;

namespace AwsSyncer.AWS;

public sealed class S3Blobs(IAmazonS3 amazon, IPathManager pathManager, S3StorageClass s3StorageClass)
    : S3PutBase(amazon)
{
    const int KeyLength = 512 / 8;

    readonly IPathManager _pathManager = pathManager
        ?? throw new ArgumentNullException(nameof(pathManager));

    readonly S3StorageClass _s3StorageClass = s3StorageClass
        ?? throw new ArgumentNullException(nameof(s3StorageClass));

    public async Task<IReadOnlyDictionary<byte[], string>> ListAsync(Statistics statistics, CancellationToken cancellationToken)
    {
        if (S3Util.KeyAlphabet is null)
            return await ListAsync(_pathManager.BlobPrefix, statistics, cancellationToken).ConfigureAwait(false);

        var tasks = S3Util.KeyAlphabet
            .Select(ch => ListAsync(_pathManager.BlobPrefix + ch, statistics, cancellationToken))
            .ToArray();

        await Task.WhenAll(tasks).ConfigureAwait(false);

        return tasks.SelectMany(t => t.Result).ToFrozenDictionary(kv => kv.Key, kv => kv.Value, ByteArrayComparer.Instance);
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

        for (;;)
        {
            var response = await AmazonS3.ListObjectsV2Async(request, cancellationToken).ConfigureAwait(false);

            foreach (var s3Object in response.S3Objects)
            {
                var key = _pathManager.GetKeyFromBlobPath(s3Object.Key);

                // Ignore the folder itself (if there is an empty object there).
                if (key is null)
                    continue;

                statistics?.Add(s3Object.Size);

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

        Debug.WriteLine(
            $"S3Blobs.UploadBlobAsync() {request.FileFingerprint.FullFilePath} ({request.FileFingerprint.Fingerprint.Key()[..12]})");

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

    public IUploadBlobRequest BuildUploadBlobRequest((FileFingerprint fingerprint, AnnotatedPath path) tuple)
    {
        Debug.WriteLine(
            $"S3Blobs.BuildUploadRequest() {tuple.fingerprint.FullFilePath} ({tuple.fingerprint.Fingerprint.Key()[..12]})");

        var fullFilePath = tuple.path.FileInfo.FullName;

        if (tuple.fingerprint.Invalid)
        {
            Debug.WriteLine($"BuildUploadRequest(): {fullFilePath} is invalid");
            return null;
        }

        var blobKey = _pathManager.GetBlobPath(tuple.fingerprint);

        var fileInfo = new FileInfo(fullFilePath);

        var fingerprint = tuple.fingerprint.Fingerprint;

        if (fileInfo.Length != fingerprint.Size || fileInfo.LastWriteTimeUtc != tuple.fingerprint.LastModifiedUtc)
        {
            Debug.WriteLine(
                $"BuildUploadRequest(): {fileInfo.FullName} changed {fingerprint.Size} != {fileInfo.Length} || {tuple.fingerprint.LastModifiedUtc} != {fileInfo.LastWriteTimeUtc} ({tuple.fingerprint.LastModifiedUtc - fileInfo.LastWriteTimeUtc})");

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
                ["x-amz-meta-lastModified"] = tuple.fingerprint.LastModifiedUtc.ToString("O"),
                ["x-amz-meta-sha2-256"] = base64Sha256,
                ["x-amz-meta-sha3-512"] = Convert.ToBase64String(fingerprint.Sha3_512)
            },
            StorageClass = _s3StorageClass,
            AutoCloseStream = false,
            AutoResetStreamPosition = false,
            ChecksumSHA256 = base64Sha256,
            ChecksumAlgorithm = ChecksumAlgorithm.SHA256
        };

        if (!string.IsNullOrEmpty(tuple.path.Collection))
            request.Headers["x-amz-meta-original-collection"] = tuple.path.Collection;

        if (!string.IsNullOrEmpty(tuple.path.RelativePath))
            request.Headers["x-amz-meta-original-path"] = tuple.path.RelativePath;

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
            FileFingerprint = tuple.fingerprint,
            ETag = S3Util.ComputeS3Etag(fingerprint.Md5),
            Key = blobKey
        };
    }

    public sealed class Statistics
    {
        long _count;
        long _totalSize;

        public long Count => _count;

        public long TotalSize => _totalSize;

        public void Add(long size)
        {
            Interlocked.Increment(ref _count);
            Interlocked.Add(ref _totalSize, size);
        }

        public void Clear()
        {
            _count = 0;
            _totalSize = 0;
        }
    }

    public interface IUploadBlobRequest
    {
        string Key { get; }
        FileFingerprint FileFingerprint { get; }
    }

    sealed class UploadBlobRequest : S3PutRequest, IUploadBlobRequest
    {
        public string Key { get; init; }
        public FileFingerprint FileFingerprint { get; init; }
    }
}
