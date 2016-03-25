// Copyright (c) 2014-2016 Henric Jungheim <software@henric.org>
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
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using AwsSyncer;

namespace AwsDedupSync
{
    public class S3PathSyncer
    {
        public static readonly DataflowLinkOptions DataflowLinkOptionsPropagateEnabled = new DataflowLinkOptions
        {
            PropagateCompletion = true
        };

        readonly S3BlobUploader _s3BlobUploader;
        readonly S3LinkCreator _s3LinkCreator;

        public S3PathSyncer()
        {
            _s3BlobUploader = new S3BlobUploader(S3Settings);
            _s3LinkCreator = new S3LinkCreator(S3Settings);
        }

        public S3Settings S3Settings { get; } = new S3Settings();

        public async Task SyncPathsAsync(string bucket, IEnumerable<string> paths, CancellationToken cancellationToken)
        {
            // BEWARE:  This could cause trouble if there are
            // any case-sensitive paths involved.
            var namedPaths = (from arg in paths
                              let split = arg.IndexOf('=')
                              let validSplit = split > 0 && split < arg.Length - 1
                              select new CollectionPath
                              {
                                  Name = validSplit ? arg.Substring(0, split) : null,
                                  Path = PathUtil.ForceTrailingSlash(Path.GetFullPath(validSplit ? arg.Substring(split + 1) : arg))
                              })
                .Distinct()
                .ToArray();

            using (var blobManager = new BlobManager(new StreamFingerprinter(), cancellationToken))
            {
                try
                {
                    using (var awsManager = AwsManagerFactory.Create(bucket))
                    {
                        var uniqueBlobBuffer = new BufferBlock<IBlob>();
                        var linkBlobs = new BufferBlock<IBlob>();

                        var blobBroadcastBlock = new BroadcastBlock<IBlob>(b => b);

                        if (S3Settings.UploadBlobs)
                        {
                            var knownFingerprints = new HashSet<BlobFingerprint>();

                            var uploadBlock = new ActionBlock<IBlob>(
                                async blob =>
                                {
                                    if (knownFingerprints.Add(blob.Fingerprint))
                                    {
                                        //Debug.WriteLine(string.Format("Queueing {0} for upload", blob.FullFilePath));
                                        await uniqueBlobBuffer.SendAsync(blob, cancellationToken).ConfigureAwait(false);
                                    }
                                    //else
                                    //    Debug.WriteLine(string.Format("Skipping {0} for upload", blob.FullFilePath));
                                });

                            blobBroadcastBlock.LinkTo(uploadBlock, DataflowLinkOptionsPropagateEnabled);
                        }

                        if (S3Settings.UpdateLinks)
                            blobBroadcastBlock.LinkTo(linkBlobs, DataflowLinkOptionsPropagateEnabled);

                        // Make sure we have at least one block consuming blobs.
                        if (!S3Settings.UploadBlobs && !S3Settings.UpdateLinks)
                            blobBroadcastBlock.LinkTo(DataflowBlock.NullTarget<IBlob>());

                        var tasks = new List<Task>();

                        tasks.Add(blobBroadcastBlock.Completion);

#if DEBUG
                        var dontWaitTask = blobBroadcastBlock.Completion.ContinueWith(_ => { Debug.WriteLine("S3PathSyncer.SyncPathsAsync() blobBroadcastBlock completed"); });
#endif

                        var loadBlobTask = blobManager.LoadAsync(namedPaths, blobBroadcastBlock);

                        tasks.Add(loadBlobTask);

                        if (S3Settings.UpdateLinks)
                        {
                            var livePaths = namedPaths.Where(p => null != p.Name).Distinct().ToLookup(p => p.Name, p => p.Path);

                            if (livePaths.Count > 0)
                            {
                                var updateLinksTask = _s3LinkCreator.UpdateLinksAsync(awsManager, livePaths, linkBlobs, cancellationToken);

                                tasks.Add(updateLinksTask);
                            }
                            else
                            {
                                // Can we even get here...?
                                linkBlobs.LinkTo(DataflowBlock.NullTarget<IBlob>());
                            }
                        }

                        Task uploadBlobsTask = null;
                        var scanBlobAsync = Task.Run(async () =>
                        {
                            // ReSharper disable once AccessToDisposedClosure
                            var knownObjects = await awsManager.ScanAsync(cancellationToken).ConfigureAwait(false);

                            if (S3Settings.UploadBlobs)
                            {
                                // ReSharper disable once AccessToDisposedClosure
                                uploadBlobsTask = _s3BlobUploader.UploadBlobsAsync(awsManager, uniqueBlobBuffer, knownObjects, cancellationToken);
                            }
                        });

                        tasks.Add(scanBlobAsync);

                        if (S3Settings.UpdateMeta)
                        {
                            var updateMetaTask = UpdateMetaAsync(awsManager, blobManager, blobBroadcastBlock.Completion, cancellationToken);

                            tasks.Add(updateMetaTask);
                        }

                        await WaitAllWithWake(tasks).ConfigureAwait(false);

                        uniqueBlobBuffer.Complete();

                        if (null != uploadBlobsTask)
                            await uploadBlobsTask.ConfigureAwait(false);
                    }
                }
                finally
                {
                    await blobManager.ShutdownAsync().ConfigureAwait(false);
                }
            }
        }

        async Task UpdateMetaAsync(AwsManager awsManager, BlobManager blobManager, Task allBlobsCreatedTask, CancellationToken cancellationToken)
        {
            await allBlobsCreatedTask.ConfigureAwait(false);

            foreach (var fingerprintBlobs in blobManager.AllBlobs)
            {
                // ReSharper disable once AccessToDisposedClosure
                await awsManager.UpdateBlobPaths(fingerprintBlobs.Key, fingerprintBlobs.Value, cancellationToken).ConfigureAwait(false);
            }
            //return Task.CompletedTask;
            //var allBlobs = new Dictionary<IBlobFingerprint, BlobFingerprintPaths>();

            //var captureAllBlock = new ActionBlock<IBlob>(blob =>
            //{
            //    BlobFingerprintPaths blobFingerprintPaths;
            //    if (!allBlobs.TryGetValue(blob.Fingerprint, out blobFingerprintPaths))
            //    {
            //        blobFingerprintPaths = new BlobFingerprintPaths(blob.Fingerprint);
            //    }

            //    blobFingerprintPaths.Add();
            //});
        }

        static async Task WaitAllWithWake(ICollection<Task> tasks)
        {
            for (;;)
            {
                var pendingTasks = tasks.Where(t => !t.IsCompleted).ToArray();

                if (0 == pendingTasks.Length)
                    break;

                await Task.WhenAny(Task.WhenAll(pendingTasks), Task.Delay(TimeSpan.FromSeconds(3))).ConfigureAwait(false);
            }
        }
    }
}
