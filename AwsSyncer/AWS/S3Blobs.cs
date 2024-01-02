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
using System.Collections.Concurrent;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;
using AwsSyncer.FileBlobs;
using AwsSyncer.Types;
using AwsSyncer.Utility;

namespace AwsSyncer.AWS;

public sealed class S3Blobs(
    IAmazonS3 amazon,
    IPathManager pathManager,
    S3StorageClass s3StorageClass,
    IStreamFingerprinter fingerprinter,
    bool writeEnabled,
    bool deleteEnabled)
    : S3PutBase(amazon)
{
    const int KeyLength = 512 / 8;
    const string XAmzMetaRecoveredFrom = "x-amz-meta-recovered-from";
    const string XAmzMetaSha2 = "x-amz-meta-sha2-256";
    const string XAmzMetaSha3 = "x-amz-meta-sha3-512";
    const string CacheControlForever = "public, max-age=31536000, immutable";
    readonly bool _deleteEnabled = deleteEnabled;

    readonly IStreamFingerprinter _fingerprinter = fingerprinter
        ?? throw new ArgumentNullException(nameof(fingerprinter));

    readonly IPathManager _pathManager = pathManager
        ?? throw new ArgumentNullException(nameof(pathManager));

    readonly S3StorageClass _s3StorageClass = s3StorageClass
        ?? throw new ArgumentNullException(nameof(s3StorageClass));

    readonly bool _writeEnabled = writeEnabled;

    public async Task<(FrozenDictionary<byte[], string> blobs, FrozenDictionary<string, string> malformedKeys)> ListAsync(
        Statistics statistics, CancellationToken cancellationToken)
    {
        async Task<(Dictionary<byte[], string> blobs, Dictionary<string, string> malformedKeys)> GetListAsync()
        {
            var tasks = S3Util.KeyAlphabet
                .Select(ch => ListAsync(_pathManager.BlobPrefix + ch, statistics, cancellationToken))
                .ToArray();

            await Task.WhenAll(tasks).ConfigureAwait(false);

            var combinedBlobs = tasks
                .SelectMany(t => t.Result.blobs)
                .ToDictionary(ByteArrayComparer.Instance);

            var combinedMalformed = tasks
                .Where(t => t.Result.malformedKeys is not null)
                .SelectMany(t => t.Result.malformedKeys)
                .ToDictionary(StringComparer.Ordinal);

            return (combinedBlobs, combinedMalformed);
        }

        var (blobs, malformed) = await GetListAsync().ConfigureAwait(false);

        if (malformed?.Count > 0)
            await RecoverMalformedKeysAsync(blobs, malformed, cancellationToken).ConfigureAwait(false);

        return (blobs.ToFrozenDictionary(ByteArrayComparer.Instance), malformed?.ToFrozenDictionary(StringComparer.Ordinal));
    }

    async Task RecoverMalformedKeysAsync(Dictionary<byte[], string> blobs, Dictionary<string, string> malformed,
        CancellationToken cancellationToken)
    {
        ConcurrentDictionary<byte[], string> recovered = new(ByteArrayComparer.Instance);
        ConcurrentBag<string> deletedMalformed = [];

        var tempDir = Path.GetTempPath();

        await Parallel.ForEachAsync(malformed,
            new ParallelOptions
            {
                CancellationToken = cancellationToken,
                MaxDegreeOfParallelism = 16
            },
            async (kvp, ct) =>
            {
                var key = kvp.Key;
                var eTag = kvp.Value;

                Console.WriteLine($"Recovering malformed key \"{key}\" (eTag {eTag})");

                try
                {
                    bool IsExistingCrKey()
                    {
                        var decodedKey = _pathManager.GetKeyFromBlobPath(key);
                        if (decodedKey?.Length != KeyLength + 1 || decodedKey[^1] != 13)
                            return false;

                        var actualKey = decodedKey[..KeyLength];

                        return blobs.ContainsKey(actualKey) || recovered.ContainsKey(actualKey);
                    }

                    if (IsExistingCrKey())
                    {
                        Console.WriteLine($"Key \"{key}\" is a CR key with an existing fixed BLOB.");
                    }
                    else
                    {
                        using var response = await AmazonS3.GetObjectAsync(new()
                        {
                            BucketName = _pathManager.Bucket,
                            Key = key,
                            EtagToMatch = eTag,
                            ChecksumMode = ChecksumMode.ENABLED
                        }, ct).ConfigureAwait(false);

                        var path = Path.GetFullPath(Path.GetRandomFileName(), tempDir);

                        await using var file = new FileStream(path, FileMode.Create, FileAccess.ReadWrite,
                            FileShare.Read,
                            0, FileOptions.Asynchronous | FileOptions.DeleteOnClose);

                        await response.ResponseStream.CopyToAsync(file, ct);

                        file.Position = 0;

                        var fingerprint = await _fingerprinter.GetFingerprintAsync(file, cancellationToken).ConfigureAwait(false);

                        var destinationKey = _pathManager.GetBlobPath(fingerprint);

                        if (blobs.ContainsKey(fingerprint.Sha3_512) || recovered.ContainsKey(fingerprint.Sha3_512))
                        {
                            Console.WriteLine($"Key \"{key}\" already exists as \"{destinationKey}\".");
                        }
                        else
                        {
                            // We don't have it...
                            Console.WriteLine($"Copying {key} to {destinationKey}");

                            var copyObjectRequest = new CopyObjectRequest
                            {
                                SourceBucket = _pathManager.Bucket,
                                SourceKey = key,
                                DestinationBucket = _pathManager.Bucket,
                                DestinationKey = destinationKey,
                                Headers =
                                {
                                    CacheControl = CacheControlForever,
                                    ContentType = response.Headers.ContentType,
                                    ContentDisposition = response.Headers.ContentDisposition
                                },
                                MetadataDirective = S3MetadataDirective.REPLACE,
                                ETagToMatch = eTag,
                                StorageClass = _s3StorageClass,
                                ChecksumAlgorithm = ChecksumAlgorithm.SHA256
                            };

                            foreach (var metadataKey in response.Metadata.Keys)
                                copyObjectRequest.Metadata[metadataKey] = response.Metadata[metadataKey];

                            if (copyObjectRequest.Metadata.Keys.Contains(XAmzMetaRecoveredFrom))
                            {
                                copyObjectRequest.Metadata[$"{XAmzMetaRecoveredFrom}-{DateTime.UtcNow:O}"] =
                                    copyObjectRequest.Metadata[XAmzMetaRecoveredFrom];
                            }

                            copyObjectRequest.Metadata[XAmzMetaRecoveredFrom] = key;

                            copyObjectRequest.Metadata[XAmzMetaSha2] = Convert.ToBase64String(fingerprint.Sha2_256);
                            copyObjectRequest.Metadata[XAmzMetaSha3] = Convert.ToBase64String(fingerprint.Sha3_512);

                            if (_writeEnabled)
                            {
                                var copyResponse = await AmazonS3.CopyObjectAsync(copyObjectRequest, cancellationToken)
                                    .ConfigureAwait(false);

                                bool VerifyChecksum(ReadOnlySpan<byte> sourceHash, string base64destinationHash)
                                {
                                    Span<byte> destinationHash = stackalloc byte[sourceHash.Length];
                                    if (!Convert.TryFromBase64String(base64destinationHash, destinationHash, out var written)
                                        || written != destinationHash.Length)
                                    {
                                        return false;
                                    }

                                    return destinationHash.SequenceEqual(sourceHash);
                                }

                                if (!VerifyChecksum(fingerprint.Sha2_256, copyResponse.ChecksumSHA256))
                                    throw new InvalidOperationException("Checksum mismatch");

                                if (!string.Equals(eTag, copyResponse.ETag, StringComparison.OrdinalIgnoreCase))
                                {
                                    Console.WriteLine(
                                        $"ETag mismatch \"{eTag}\" != \"{copyResponse.ETag}\" for key \"{key}\" copy to \"{destinationKey}\".");
                                    eTag = copyResponse.ETag;
                                }
                            }

                            recovered[fingerprint.Sha3_512] = eTag;
                        }
                    }

                    if (!_deleteEnabled)
                    {
                        Debug.WriteLine($"Deleting \"{key}\" disabled.");
                        return;
                    }

                    Console.WriteLine($"Deleting \"{key}\".");

                    if (_writeEnabled)
                    {
                        await AmazonS3.DeleteObjectAsync(new()
                        {
                            BucketName = _pathManager.Bucket,
                            Key = key
                        }, ct).ConfigureAwait(false);
                    }

                    deletedMalformed.Add(key);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Unable to recover \"{key}\": {ex}");
                }
            }
        );

        foreach (var (key, eTag) in recovered)
        {
            blobs[key] = eTag;
        }

        foreach (var key in deletedMalformed)
        {
            malformed.Remove(key);
        }
    }

    async Task<(Dictionary<byte[], string> blobs, Dictionary<string, string> malformedKeys)> ListAsync(
        string prefix, Statistics statistics, CancellationToken cancellationToken)
    {
        var blobs = new Dictionary<byte[], string>(ByteArrayComparer.Instance);
        Dictionary<string, string> malformedKeys = null;

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
                // Ignore the directory itself.
                if (string.Equals(s3Object.Key, prefix, StringComparison.Ordinal))
                    continue;

                var key = _pathManager.GetKeyFromBlobPath(s3Object.Key);

                // Ignore the folder itself (if there is an empty object there).
                if (key is null)
                {
                    Console.WriteLine($"Unexpected object {s3Object.Key}");

                    continue;
                }

                if (key.Length != KeyLength)
                {
                    // Compensate for past trouble...
                    if (KeyLength + 1 == key.Length && 13 == key[KeyLength])
                    {
                        Console.WriteLine($"Object key {s3Object.Key} has trailing carriage return.");
                    }
                    else
                    {
                        var metadata = await AmazonS3
                            .GetObjectMetadataAsync(_pathManager.Bucket, s3Object.Key, cancellationToken).ConfigureAwait(false);

                        var alternateKeyHeader = metadata.Metadata[XAmzMetaSha3];

                        if (string.IsNullOrWhiteSpace(alternateKeyHeader))
                        {
                            Console.WriteLine($"Malformed key for object {s3Object.Key}");
                            continue;
                        }
                    }

                    malformedKeys ??= new(StringComparer.Ordinal);
                    malformedKeys[s3Object.Key] = s3Object.ETag;

                    continue;
                }

                blobs[key] = s3Object.ETag;
            }

            if (!response.IsTruncated)
                return (blobs, malformedKeys);

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

        var fingerprint = tuple.fingerprint.Fingerprint;

        var blobKey = _pathManager.GetBlobPath(fingerprint);

        var fileInfo = new FileInfo(fullFilePath);

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
                CacheControl = CacheControlForever,
                ContentType = MimeDetector.GetMimeType(fullFilePath),
                ContentLength = fingerprint.Size,
                ContentMD5 = md5Digest
            },
            Metadata =
            {
                [XAmzMetaSha2] = base64Sha256,
                [XAmzMetaSha3] = Convert.ToBase64String(fingerprint.Sha3_512),
                ["x-amz-meta-lastModified"] = tuple.fingerprint.LastModifiedUtc.ToString("O")
            },
            StorageClass = _s3StorageClass,
            AutoCloseStream = false,
            AutoResetStreamPosition = false,
            ChecksumSHA256 = base64Sha256,
            ChecksumAlgorithm = ChecksumAlgorithm.SHA256
        };

        if (!string.IsNullOrEmpty(tuple.path.Collection))
            request.Metadata["x-amz-meta-original-collection"] = tuple.path.Collection;

        if (!string.IsNullOrEmpty(tuple.path.RelativePath))
            request.Metadata["x-amz-meta-original-path"] = tuple.path.RelativePath;

        var fileName = Path.GetFileName(fullFilePath);

        if (!string.IsNullOrWhiteSpace(fileName))
        {
            fileName = PathUtil.NormalizeAsciiName(fileName);
            request.Metadata["x-amz-meta-original-name"] = fileName;
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
