// Copyright (c) 2014-2017 Henric Jungheim <software@henric.org>
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
using System.Threading;
using System.Threading.Tasks;
using Amazon.S3;
using AwsSyncer.FileBlobs;
using AwsSyncer.Types;
using AwsSyncer.Utility;
using Microsoft.Extensions.Configuration;

namespace AwsSyncer.AWS;

public interface IAwsManager : IDisposable
{
    Task<(IReadOnlyDictionary<byte[], string> blobs, IReadOnlyDictionary<string, string> invalidKeys)> ScanAsync(
        CancellationToken cancellationToken);

    S3Blobs.IUploadBlobRequest BuildUploadBlobRequest((FileFingerprint fingerprint, AnnotatedPath path) tuple);
    Task UploadBlobAsync(S3Blobs.IUploadBlobRequest uploadBlobRequest, CancellationToken cancellationToken);
    Task<IReadOnlyDictionary<string, string>> GetLinksAsync(string name, CancellationToken cancellationToken);

    S3Links.ICreateLinkRequest BuildLinkRequest(string collection, string relativePath, FileFingerprint fileFingerprint,
        string existingETag = null);

    Task CreateLinkAsync(S3Links.ICreateLinkRequest createLinkRequest, CancellationToken cancellationToken);
    Task DeleteKeys(IReadOnlyCollection<KeyValuePair<string, string>> keys, CancellationToken cancellationToken);
}

public sealed class AwsManager : IAwsManager
{
    readonly bool _actuallyWrite;
    readonly IAmazonS3 _amazonS3;
    readonly IPathManager _pathManager;
    readonly S3Blobs _s3Blobs;
    readonly S3Links _s3Links;

    public AwsManager(IConfiguration config, IAmazonS3 amazonS3, IPathManager pathManager, IStreamFingerprinter fingerprinter,
        bool actuallyWrite, bool deleteEnabled)
    {
        _amazonS3 = amazonS3 ?? throw new ArgumentNullException(nameof(amazonS3));
        _pathManager = pathManager ?? throw new ArgumentNullException(nameof(pathManager));
        _actuallyWrite = actuallyWrite;

        var storageClass = S3StorageClass.Standard;
        var storageClassString = config["StorageClass"];

        if (!string.IsNullOrWhiteSpace(storageClassString))
            storageClass = S3StorageClass.FindValue(storageClassString);

        var blobStorageClass = storageClass;
        var blobStorageClassString = config["BlobStorageClass"];

        if (!string.IsNullOrWhiteSpace(blobStorageClassString))
            blobStorageClass = S3StorageClass.FindValue(blobStorageClassString);

        var linkStorageClass = storageClass;
        var linkStorageClassString = config["LinkStorageClass"];

        if (!string.IsNullOrWhiteSpace(linkStorageClassString))
            linkStorageClass = S3StorageClass.FindValue(linkStorageClassString);

        _s3Blobs = new(amazonS3, pathManager, blobStorageClass, fingerprinter, actuallyWrite, deleteEnabled);
        _s3Links = new(amazonS3, pathManager, linkStorageClass);
    }

    #region IDisposable Members

    public void Dispose() => _amazonS3.Dispose();

    #endregion

    public async Task<(IReadOnlyDictionary<byte[], string> blobs, IReadOnlyDictionary<string, string> invalidKeys)>
        ScanAsync(CancellationToken cancellationToken)
    {
        var statistics = new S3Blobs.Statistics();

        var sw = Stopwatch.StartNew();

        var (s3Blobs, invalidKeys) = await _s3Blobs.ListAsync(statistics, cancellationToken).ConfigureAwait(false);

        sw.Stop();

        Console.WriteLine(
            $"Bucket {_pathManager.Bucket} contains {s3Blobs.Count}/{statistics.Count} items {statistics.TotalSize.BytesToGiB():F2}GiB in {sw.Elapsed}");

        if (invalidKeys is not null && invalidKeys.Count > 0)
            Console.WriteLine($"Found {invalidKeys.Count} invalid keys");

        return (s3Blobs, invalidKeys);
    }

    public S3Blobs.IUploadBlobRequest BuildUploadBlobRequest((FileFingerprint fingerprint, AnnotatedPath path) tuple) =>
        _s3Blobs.BuildUploadBlobRequest(tuple);

    public Task UploadBlobAsync(S3Blobs.IUploadBlobRequest uploadBlobRequest, CancellationToken cancellationToken) =>
        _s3Blobs.UploadBlobAsync(uploadBlobRequest, cancellationToken);

    public Task<IReadOnlyDictionary<string, string>> GetLinksAsync(string name, CancellationToken cancellationToken) =>
        _s3Links.ListAsync(name, cancellationToken);

    public S3Links.ICreateLinkRequest BuildLinkRequest(string collection, string relativePath, FileFingerprint fileFingerprint,
        string existingETag = null) => _s3Links.BuildCreateLinkRequest(collection, relativePath, fileFingerprint, existingETag);

    public Task CreateLinkAsync(S3Links.ICreateLinkRequest createLinkRequest, CancellationToken cancellationToken) =>
        _s3Links.CreateLinkAsync(createLinkRequest, cancellationToken);

    public Task DeleteKeys(IReadOnlyCollection<KeyValuePair<string, string>> keys, CancellationToken cancellationToken)
    {
        if (keys is null || keys.Count < 1)
            return Task.CompletedTask;

        return Parallel.ForEachAsync(keys, new ParallelOptions
            {
                CancellationToken = cancellationToken,
                MaxDegreeOfParallelism = 8
            },
            async (kv, ct) =>
            {
                var (key, eTag) = kv;

                Console.WriteLine($"Deleting {key} with ETag {eTag}");

                if (!_actuallyWrite)
                    return;

                try
                {
                    await _amazonS3.DeleteObjectAsync(_pathManager.Bucket, key, ct).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Delete of {key} failed with {e.Message}");
                }
            });
    }
}
