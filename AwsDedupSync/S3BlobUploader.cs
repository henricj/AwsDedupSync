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
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using AwsSyncer.AWS;
using AwsSyncer.Types;
using AwsSyncer.Utility;

namespace AwsDedupSync
{
    public class S3BlobUploader
    {
        readonly S3Settings _s3Settings;

        public S3BlobUploader(S3Settings s3Settings)
        {
            _s3Settings = s3Settings ?? throw new ArgumentNullException(nameof(s3Settings));
        }

        public Task UploadBlobsAsync(IAwsManager awsManager,
            ISourceBlock<Tuple<FileFingerprint, AnnotatedPath>> uniqueBlobBlock,
            IReadOnlyDictionary<string, string> knowObjects,
            CancellationToken cancellationToken)
        {
            var blobCount = 0;
            var blobTotalSize = 0L;

            var builderBlock = new TransformBlock<Tuple<FileFingerprint, AnnotatedPath>, S3Blobs.IUploadBlobRequest>(
                tuple =>
                {
                    var exists = knowObjects.TryGetValue(tuple.Item1.Fingerprint.Key(), out var etag);

                    //Debug.WriteLine($"{tuple.Item1.FullFilePath} {(exists ? "already exists" : "scheduled for upload")}");

                    if (exists)
                    {
                        // We can't check multipart uploads this way since we don't know the size
                        // of the individual parts.
                        if (etag.Contains("-"))
                        {
                            Debug.WriteLine($"{tuple.Item1.FullFilePath} is a multi-part upload with ETag {etag} {tuple.Item1.Fingerprint.Key().Substring(0, 12)}");

                            return null;
                        }

                        var expectedETag = tuple.Item1.Fingerprint.S3ETag();

                        if (string.Equals(expectedETag, etag, StringComparison.OrdinalIgnoreCase))
                            return null;

                        Console.WriteLine($"ERROR: {tuple.Item1.FullFilePath} tag mismatch {etag}, expected {expectedETag} {tuple.Item1.Fingerprint.Key().Substring(0, 12)}");
                    }

                    var request = awsManager.BuildUploadBlobRequest(tuple);

                    if (null == request)
                        return null;

                    Interlocked.Increment(ref blobCount);

                    Interlocked.Add(ref blobTotalSize, request.FileFingerprint.Fingerprint.Size);

                    return request;
                },
                new ExecutionDataflowBlockOptions { CancellationToken = cancellationToken, MaxDegreeOfParallelism = Environment.ProcessorCount });

            var uploader = new ActionBlock<S3Blobs.IUploadBlobRequest>(
                blob => UploadBlobAsync(awsManager, blob, cancellationToken),
                new ExecutionDataflowBlockOptions
                {
                    MaxDegreeOfParallelism = 4,
                    CancellationToken = cancellationToken
                });

            builderBlock.LinkTo(uploader, new DataflowLinkOptions { PropagateCompletion = true }, r => null != r);
            builderBlock.LinkTo(DataflowBlock.NullTarget<S3Blobs.IUploadBlobRequest>());

            uniqueBlobBlock.LinkTo(builderBlock, new DataflowLinkOptions { PropagateCompletion = true });

            var tasks = new List<Task> { uploader.Completion };

#if DEBUG
            var uploadDoneTask = uploader.Completion
                .ContinueWith(
                    _ => Console.WriteLine($"Upload complete: {blobCount} items {blobTotalSize.BytesToGiB():F2}GiB"), cancellationToken);

            tasks.Add(uploadDoneTask);
#endif


            return Task.WhenAll(tasks);
        }

        async Task UploadBlobAsync(IAwsManager awsManager, S3Blobs.IUploadBlobRequest uploadBlobRequest, CancellationToken cancellationToken)
        {
            if (null == uploadBlobRequest)
                return;

            Console.WriteLine("Upload {0} as {1}",
                uploadBlobRequest.FileFingerprint.FullFilePath,
                uploadBlobRequest.FileFingerprint.Fingerprint.Key().Substring(0, 12));

            if (!_s3Settings.ActuallyWrite)
                return;

            try
            {
                await awsManager.UploadBlobAsync(uploadBlobRequest, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Upload of {0} failed: {1}", uploadBlobRequest.FileFingerprint.FullFilePath, ex.Message);
            }
        }
    }
}
