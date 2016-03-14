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
using System.Collections.Concurrent;
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
        static readonly DataflowLinkOptions DataflowLinkOptionsPropagateEnabled = new DataflowLinkOptions
        {
            PropagateCompletion = true
        };

        public bool ActuallyWrite { get; set; } = true;
        public bool UpdateLinks { get; set; } = true;
        public bool UploadBlobs { get; set; } = true;

        public async Task SyncPathsAsync(string bucket, IEnumerable<string> paths, CancellationToken cancellationToken)
        {
            // BEWARE:  This could cause trouble if there are
            // any case-sensitive paths involved.
            var namedPaths = (from arg in paths
                              let split = arg.IndexOf('=')
                              let validSplit = split > 0 && split < arg.Length - 1
                              select new
                              {
                                  Name = validSplit ? arg.Substring(0, split) : null,
                                  Path = PathUtil.ForceTrailingSlash(Path.GetFullPath(validSplit ? arg.Substring(split + 1) : arg))
                              })
                .Distinct()
                .ToArray();

            using (var awsManager = AwsManagerFactory.Create(bucket))
            {
                var tasks = new List<Task>();

                var blobManager = new BlobManager(new StreamFingerprinter());

                var fingerprints = new ConcurrentDictionary<IBlobFingerprint, IBlob>();
                var uniqueFingerprints = new BufferBlock<IBlob>();
                var allBlobs = new BufferBlock<IBlob>();

                var blobDispatcher = new ActionBlock<IBlob>(
                    async blob =>
                    {
                        if (UpdateLinks)
                            await allBlobs.SendAsync(blob, cancellationToken).ConfigureAwait(false);

                        if (UploadBlobs)
                        {
                            if (fingerprints.TryAdd(blob.Fingerprint, blob))
                            {
                                //Trace.WriteLine(string.Format("Queueing {0} for upload", blob.FullFilePath));
                                await uniqueFingerprints.SendAsync(blob, cancellationToken).ConfigureAwait(false);
                            }
                            //else
                            //    Trace.WriteLine(string.Format("Skipping {0} for upload", blob.FullFilePath));
                        }
                        }
                    });

                var ttt = blobDispatcher.Completion.ContinueWith(t =>
                {
                    allBlobs.Complete();
                    uniqueFingerprints.Complete();
                }, cancellationToken);

                tasks.Add(ttt);

                var distinctPaths = namedPaths.Select(p => p.Path).Distinct(StringComparer.InvariantCultureIgnoreCase).ToArray();

                var loadBlobTask = Task.Run(() => blobManager.Load(distinctPaths, blobDispatcher), cancellationToken);

                tasks.Add(loadBlobTask);

                if (UpdateLinks)
                {
                    var livePaths = namedPaths.Where(p => null != p.Name).Distinct().ToLookup(p => p.Name, p => p.Path);

                    if (livePaths.Count > 0)
                    {
                        var updateLinksTask = UpdateLinksAsync(awsManager, livePaths, linkBlobs, cancellationToken);

                        tasks.Add(updateLinksTask);
                    }
                }

                var knownObjects = await awsManager.ScanAsync(cancellationToken).ConfigureAwait(false);

                if (UploadBlobs)
                {
                    var uploadBlobsTask = UploadBlobsAsync(awsManager, uniqueFingerprints, knownObjects, cancellationToken);

                    tasks.Add(uploadBlobsTask);
                }

                await Task.WhenAll(tasks).ConfigureAwait(false);
            }
        }

        Task UpdateLinksAsync(AwsManager awsManager, ILookup<string, string> livePaths, BufferBlock<IBlob> linkBlobs, CancellationToken cancellationToken)
        {
            var linksTask = CreateLinksAsync(awsManager, livePaths, linkBlobs, cancellationToken);

            var traceTask = linksTask.ContinueWith(t => Trace.WriteLine("Done processing links"), cancellationToken);

            return Task.WhenAll(linksTask, traceTask);
        }

        Task UploadBlobsAsync(AwsManager awsManager, BufferBlock<IBlob> uniqueFingerprints, IReadOnlyDictionary<string, long> knowObjects, CancellationToken cancellationToken)
        {
            var tasks = new List<Task>();

            var uploader = new ActionBlock<IBlob>(
                async blob =>
                {
                    try
                    {
                        // ReSharper disable once AccessToDisposedClosure
                        await UploadBlobAsync(awsManager, blob, cancellationToken).ConfigureAwait(false);

                        return;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Upload of {0} failed (retrying): {1}", blob.FullFilePath, ex.Message);
                    }

                    await uniqueFingerprints.SendAsync(blob, cancellationToken).ConfigureAwait(false);
                }, new ExecutionDataflowBlockOptions
                {
                    MaxDegreeOfParallelism = 4
                });

            var blobCount = 0;
            var blobTotalSize = 0L;

            var uploaderCounter = new TransformBlock<IBlob, IBlob>(blob =>
            {
                Interlocked.Increment(ref blobCount);

                Interlocked.Add(ref blobTotalSize, blob.Fingerprint.Size);

                return blob;
            });

            var counterCompletionTask = uploaderCounter.Completion.ContinueWith(t =>
                Trace.WriteLine($"Uploader done queueing {blobCount} items {SizeConversion.BytesToGiB(blobTotalSize):F2}GiB"),
                cancellationToken);

            tasks.Add(counterCompletionTask);

            uploaderCounter.LinkTo(uploader, DataflowLinkOptionsPropagateEnabled);

            uniqueFingerprints.LinkTo(uploaderCounter, DataflowLinkOptionsPropagateEnabled,
                blob =>
                {
                    var exists = knowObjects.ContainsKey(blob.Key);

                    //Trace.WriteLine($"{blob.FullFilePath} {(exists ? "already exists" : "scheduled for upload")}");

                    return !exists;
                });

            uniqueFingerprints.LinkTo(DataflowBlock.NullTarget<IBlob>());

#if DEBUG
            var uploadDoneTask = uploader.Completion.ContinueWith(t => Debug.WriteLine("Done uploading blobs"), cancellationToken);

            tasks.Add(uploadDoneTask);
#endif

            tasks.Add(uploader.Completion);

            return Task.WhenAll(tasks);
        }

        async Task CreateLinksAsync(AwsManager awsManager, ILookup<string, string> linkPaths, BufferBlock<IBlob> linkBlobs, CancellationToken cancellationToken)
        {
            // ReSharper disable once AccessToDisposedClosure
            var pathTasks = linkPaths.Select(
                namePath => new
                {
                    NamePath = namePath,
                    TreeTask = awsManager.ListTreeAsync(namePath.Key, cancellationToken)
                }).ToArray();

            await Task.WhenAll(pathTasks.Select(pt => pt.TreeTask)).ConfigureAwait(false);

            var linkTrees = pathTasks.Select(pt => new
            {
                pt.NamePath,
                Tree = pt.TreeTask.Result
            }).ToArray();

            var linkDispatcher = new ActionBlock<IBlob>(
                async blob =>
                {
                    var tasks = new List<Task>();

                    try
                    {
                        foreach (var linkTree in linkTrees)
                        {
                            tasks.AddRange(linkTree.NamePath
                                .Select(path => CreateLinkAsync(awsManager, linkTree.NamePath.Key, path, blob, linkTree.Tree, cancellationToken))
                                .Where(task => null != task));
                        }

                        await Task.WhenAll(tasks).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Link creation failed: " + ex.Message);
                    }
                }, new ExecutionDataflowBlockOptions
                {
                    MaxDegreeOfParallelism = 12
                });

            linkBlobs.LinkTo(linkDispatcher, DataflowLinkOptionsPropagateEnabled);

            await linkDispatcher.Completion.ConfigureAwait(false);
        }

#if false
        async Task BuildTreeAsync(AwsManager awsManager, string name, string path, ILookup<IBlobFingerprint, IBlob> uniqueBlobs, CancellationToken cancellationToken)
        {
            if (!path.EndsWith("/", StringComparison.OrdinalIgnoreCase) && !path.EndsWith("\\", StringComparison.OrdinalIgnoreCase))
                path += "\\";

            var tree = await awsManager.ListTreeAsync(name, cancellationToken).ConfigureAwait(false);

            var tasks = uniqueBlobs
                .AsParallel()
                .SelectMany(b => b).Select(blob => CreateLinkAsync(awsManager, name, path, blob, tree, cancellationToken))
                .Where(t => null != t)
                .ToArray();

            await Task.WhenAll(tasks).ConfigureAwait(false);
        }

#endif

        Task CreateLinkAsync(AwsManager awsManager, string name, string path, IBlob blob, IReadOnlyDictionary<string, string> tree, CancellationToken cancellationToken)
        {
            var relativePath = PathUtil.MakeRelativePath(path, blob.FullFilePath);

            if (relativePath.StartsWith(".."))
                return null;

            if (relativePath == blob.FullFilePath)
                return null;

            if (relativePath.StartsWith("file:", StringComparison.OrdinalIgnoreCase))
                return null;

            relativePath = relativePath.Replace('\\', '/');

            if (relativePath.StartsWith("/", StringComparison.Ordinal))
                return null;

            if (tree.ContainsKey(relativePath))
                return null;

            Console.WriteLine("Link {0} {1} -> {2}", name, relativePath, blob.Key.Substring(12));

            if (!ActuallyWrite)
                return null;

            return awsManager.CreateLinkAsync(name, relativePath, blob, cancellationToken);
        }

        Task UploadBlobAsync(AwsManager awsManager, IBlob blob, CancellationToken cancellationToken)
        {
            Console.WriteLine("Upload {0} as {1}", blob.FullFilePath, blob.Key.Substring(12));

            if (!ActuallyWrite)
                return Task.FromResult(false);

            return awsManager.StoreAsync(blob, cancellationToken);
        }

#if false
        Task UploadBlobsAsync(AwsManager awsManager, ILookup<IBlobFingerprint, IBlob> uniqueBlobs, IReadOnlyDictionary<string, long> knowObjects, CancellationToken cancellationToken)
        {
            var queue = new ConcurrentBag<IBlob>(uniqueBlobs
                .Select(blobs => blobs.First())
                .Where(blob => !knowObjects.ContainsKey(blob.Key)));

            var count = Math.Max(Environment.ProcessorCount * 2, 12);

            var workerTasks = Enumerable.Range(1, count)
                .Select(async i =>
                {
                    for (;;)
                    {
                        IBlob blob;
                        if (!queue.TryTake(out blob))
                            return;

                        try
                        {
                            await UploadBlobAsync(awsManager, blob, cancellationToken).ConfigureAwait(false);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine("Upload of {0} failed (retrying): {1}", blob.FullFilePath, ex.Message);
                            queue.Add(blob);
                        }
                    }
                });

            return Task.WhenAll(workerTasks);
        }
#endif
    }
}
