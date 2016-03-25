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
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using AwsSyncer;

namespace AwsDedupSync
{
    public class S3BlobUploader
    {
        readonly S3Settings _s3Settings;

        public S3BlobUploader(S3Settings s3Settings)
        {
            _s3Settings = s3Settings;
        }

        public Task UploadBlobsAsync(AwsManager awsManager, ISourceBlock<IBlob> uniqueBlobBlock,
            IReadOnlyDictionary<string, long> knowObjects, CancellationToken cancellationToken)
        {
            var uploader = new ActionBlock<IBlob>(
                async blob =>
                {
                    var delay = 750;
                    for (var retry = 0; retry < 4; ++retry)
                    {
                        try
                        {
                            Console.WriteLine("Upload of {0} starting", blob.FullFilePath);

                            // ReSharper disable once AccessToDisposedClosure
                            await UploadBlobAsync(awsManager, blob, cancellationToken).ConfigureAwait(false);

                            return;
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine("Upload of {0} failed: {1}", blob.FullFilePath, ex.Message);
                        }

                        await Task.Delay(RandomUtil.ThreadLocalRandom.Next(delay, delay * 2), cancellationToken).ConfigureAwait(false);

                        delay += delay;
                    }

                    Console.WriteLine("Giving up upload of {0}", blob.FullFilePath);
                }, new ExecutionDataflowBlockOptions
                {
                    MaxDegreeOfParallelism = 4,
                    CancellationToken = cancellationToken
                });

            var blobCount = 0;
            var blobTotalSize = 0L;

            var uploaderCounter = new TransformBlock<IBlob, IBlob>(blob =>
            {
                Interlocked.Increment(ref blobCount);

                Interlocked.Add(ref blobTotalSize, blob.Fingerprint.Size);

                return blob;
            }, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = DataflowBlockOptions.Unbounded, CancellationToken = cancellationToken });

            var counterCompletionTask = uploaderCounter.Completion.ContinueWith(t =>
                Trace.WriteLine($"Uploader done queueing {blobCount} items {SizeConversion.BytesToGiB(blobTotalSize):F2}GiB"),
                cancellationToken);

            var tasks = new List<Task>();

            tasks.Add(counterCompletionTask);

            uploaderCounter.LinkTo(uploader, S3PathSyncer.DataflowLinkOptionsPropagateEnabled);

            uniqueBlobBlock.LinkTo(uploaderCounter, S3PathSyncer.DataflowLinkOptionsPropagateEnabled,
                blob =>
                {
                    var exists = knowObjects.ContainsKey(blob.Key);

                    //Trace.WriteLine($"{blob.FullFilePath} {(exists ? "already exists" : "scheduled for upload")}");

                    return !exists;
                });

            uniqueBlobBlock.LinkTo(DataflowBlock.NullTarget<IBlob>());

#if DEBUG
            var uploadDoneTask = uploader.Completion.ContinueWith(_ => Debug.WriteLine("Done uploading blobs"), cancellationToken);

            tasks.Add(uploadDoneTask);
#endif

            tasks.Add(uploader.Completion);

            return Task.WhenAll(tasks);
        }

        Task UploadBlobAsync(AwsManager awsManager, IBlob blob, CancellationToken cancellationToken)
        {
            Console.WriteLine("Upload {0} as {1}", blob.FullFilePath, blob.Key.Substring(12));

            if (!_s3Settings.ActuallyWrite)
                return Task.FromResult(false);

            return awsManager.StoreAsync(blob, cancellationToken);
        }
    }
}
