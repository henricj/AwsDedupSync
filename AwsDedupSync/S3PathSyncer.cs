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
using System.Web;
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

        static string ForceTrailingSlash(string path)
        {
            if (!path.EndsWith("/", StringComparison.OrdinalIgnoreCase) && !path.EndsWith("\\", StringComparison.OrdinalIgnoreCase))
                return path + "\\";

            return path;
        }

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
                                  Path = ForceTrailingSlash(Path.GetFullPath(validSplit ? arg.Substring(split + 1) : arg))
                              })
                .Distinct()
                .ToArray();

            using (var s3Manager = new S3Manager(bucket))
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
                                //Trace.WriteLine(string.Format("Queueing {0} for upload", blob.FullPath));
                                await uniqueFingerprints.SendAsync(blob, cancellationToken).ConfigureAwait(false);
                            }
                            //else
                            //    Trace.WriteLine(string.Format("Skipping {0} for upload", blob.FullPath));
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
                        var updateLinksTask = UpdateLinksAsync(s3Manager, livePaths, allBlobs, cancellationToken);

                        tasks.Add(updateLinksTask);
                    }
                }

                await s3Manager.ScanAsync(cancellationToken).ConfigureAwait(false);

                var knowObjects = s3Manager.Keys;

                if (UploadBlobs)
                {
                    var uploadBlobsTask = UploadBlobsAsync(s3Manager, uniqueFingerprints, knowObjects, cancellationToken);

                    tasks.Add(uploadBlobsTask);
                }

                await Task.WhenAll(tasks).ConfigureAwait(false);
            }
        }

        Task UpdateLinksAsync(S3Manager s3Manager, ILookup<string, string> livePaths, BufferBlock<IBlob> allBlobs, CancellationToken cancellationToken)
        {
            var linksTask = CreateLinksAsync(s3Manager, livePaths, allBlobs, cancellationToken);

            var traceTask = linksTask.ContinueWith(t => Trace.WriteLine("Done processing links"), cancellationToken);

            return Task.WhenAll(linksTask, traceTask);
        }

        Task UploadBlobsAsync(S3Manager s3Manager, BufferBlock<IBlob> uniqueFingerprints, IReadOnlyDictionary<string, long> knowObjects, CancellationToken cancellationToken)
        {
            var tasks = new List<Task>();

            var uploader = new ActionBlock<IBlob>(
                async blob =>
                {
                    try
                    {
                        // ReSharper disable once AccessToDisposedClosure
                        await UploadBlobAsync(s3Manager, blob, cancellationToken).ConfigureAwait(false);

                        return;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Upload of {0} failed (retrying): {1}", blob.FullPath, ex.Message);
                    }

                    await uniqueFingerprints.SendAsync(blob, cancellationToken).ConfigureAwait(false);
                }, new ExecutionDataflowBlockOptions
                {
                    MaxDegreeOfParallelism = 2
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
                Trace.WriteLine($"Uploader done queueing {blobCount} items {blobTotalSize * (1.0 / (1024 * 1024 * 1024)):F2}GiB"),
                cancellationToken);

            tasks.Add(counterCompletionTask);

            uploaderCounter.LinkTo(uploader, DataflowLinkOptionsPropagateEnabled);

            uniqueFingerprints.LinkTo(uploaderCounter, DataflowLinkOptionsPropagateEnabled,
                blob =>
                {
                    var exists = knowObjects.ContainsKey(HttpServerUtility.UrlTokenEncode(blob.Fingerprint.Sha3_512));

                    //Trace.WriteLine($"{blob.FullPath} {(exists ? "already exists" : "scheduled for upload")}");

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

        async Task CreateLinksAsync(S3Manager s3Manager, ILookup<string, string> linkPaths, BufferBlock<IBlob> allBlobs, CancellationToken cancellationToken)
        {
            // ReSharper disable once AccessToDisposedClosure
            var pathTasks = linkPaths.Select(
                namePath => new
                {
                    NamePath = namePath,
                    TreeTask = s3Manager.ListTreeAsync(namePath.Key, cancellationToken)
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
                                .Select(path => CreateLinkAsync(s3Manager, linkTree.NamePath.Key, path, blob, linkTree.Tree, cancellationToken))
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

            allBlobs.LinkTo(linkDispatcher, DataflowLinkOptionsPropagateEnabled);

            await linkDispatcher.Completion.ConfigureAwait(false);
        }

        async Task BuildTreeAsync(S3Manager s3Manager, string name, string path, ILookup<IBlobFingerprint, IBlob> uniqueBlobs, CancellationToken cancellationToken)
        {
            if (!path.EndsWith("/", StringComparison.OrdinalIgnoreCase) && !path.EndsWith("\\", StringComparison.OrdinalIgnoreCase))
                path += "\\";

            var tree = await s3Manager.ListTreeAsync(name, cancellationToken).ConfigureAwait(false);

            var tasks = uniqueBlobs
                .AsParallel()
                .SelectMany(b => b).Select(blob => CreateLinkAsync(s3Manager, name, path, blob, tree, cancellationToken))
                .Where(t => null != t)
                .ToArray();

            await Task.WhenAll(tasks).ConfigureAwait(false);
        }

        Task CreateLinkAsync(S3Manager s3Manager, string name, string path, IBlob blob, IReadOnlyDictionary<string, string> tree, CancellationToken cancellationToken)
        {
            var relativePath = PathUtil.MakeRelativePath(path, blob.FullPath);

            if (relativePath.StartsWith(".."))
                return null;

            if (relativePath == blob.FullPath)
                return null;

            if (relativePath.StartsWith("file:", StringComparison.OrdinalIgnoreCase))
                return null;

            relativePath = relativePath.Replace('\\', '/');

            if (relativePath.StartsWith("/", StringComparison.Ordinal))
                return null;

            if (tree.ContainsKey(relativePath))
                return null;

            Console.WriteLine("Link {0} {1} -> {2}", name, relativePath, HttpServerUtility.UrlTokenEncode(blob.Fingerprint.Sha3_512.Take(10).ToArray()));

            if (!ActuallyWrite)
                return null;

            return s3Manager.CreateLinkAsync(name, relativePath, blob, cancellationToken);
        }

        Task UploadBlobAsync(S3Manager s3Manager, IBlob blob, CancellationToken cancellationToken)
        {
            Console.WriteLine("Upload {0} as {1}", blob.FullPath, HttpServerUtility.UrlTokenEncode(blob.Fingerprint.Sha3_512.Take(10).ToArray()));

            if (!ActuallyWrite)
                return Task.FromResult(false);

            return s3Manager.StoreAsync(blob, cancellationToken);
        }

        Task UploadBlobsAsync(S3Manager s3Manager, ILookup<IBlobFingerprint, IBlob> uniqueBlobs, IReadOnlyDictionary<string, long> knowObjects, CancellationToken cancellationToken)
        {
            var queue = new ConcurrentBag<IBlob>(uniqueBlobs
                .Where(blob => !knowObjects.ContainsKey(HttpServerUtility.UrlTokenEncode(blob.Key.Sha3_512)))
                .Select(blobs => blobs.First()));

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
                            await UploadBlobAsync(s3Manager, blob, cancellationToken).ConfigureAwait(false);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine("Upload of {0} failed (retrying): {1}", blob.FullPath, ex.Message);
                            queue.Add(blob);
                        }
                    }
                });

            return Task.WhenAll(workerTasks);
        }
    }
}
