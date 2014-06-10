﻿using System;
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
    class Program
    {
        static bool ActuallyWrite = true;
        static bool UpdateLinks = true;
        static bool UploadBlobs = true;

        static readonly DataflowLinkOptions DataflowLinkOptionsPropagateEnabled = new DataflowLinkOptions
                                                                                  {
                                                                                      PropagateCompletion = true
                                                                                  };

        static string ForceTrailingSlash(string path)
        {
            if (!path.EndsWith("/", StringComparison.OrdinalIgnoreCase) && !path.EndsWith("\\", StringComparison.OrdinalIgnoreCase))
                return path + "\\";

            return path;
        }

        static void Main(string[] args)
        {
            //ServicePointManager.DefaultConnectionLimit = 20;

            var sw = new Stopwatch();

            try
            {
                sw.Start();

                RunAsync(args).Wait();

                sw.Stop();
            }
            catch (Exception ex)
            {
                sw.Stop();

                Console.WriteLine(ex.Message);
            }

            var process = Process.GetCurrentProcess();

            Console.WriteLine("Elapsed: {0} CPU {1} User {2}", sw.Elapsed, process.TotalProcessorTime, process.UserProcessorTime);
        }

        static Task RunAsync(string[] args)
        {
            // BEWARE:  This could cause trouble if there are
            // any case-sensitive paths involved.
            var paths = (from arg in args
                         let split = arg.IndexOf('=')
                         let validSplit = split > 0 && split < arg.Length - 1
                         select new
                                {
                                    Name = validSplit ? arg.Substring(0, split) : null,
                                    Path = ForceTrailingSlash(Path.GetFullPath(validSplit ? arg.Substring(split + 1) : arg))
                                })
                .Distinct()
                .ToArray();

            using (var s3Manager = new S3Manager("images.henric.org"))
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
                            await allBlobs.SendAsync(blob).ConfigureAwait(false);

                        if (UploadBlobs && fingerprints.TryAdd(blob.Fingerprint, blob))
                            await uniqueFingerprints.SendAsync(blob).ConfigureAwait(false);
                    });

                var ttt = blobDispatcher.Completion.ContinueWith(t =>
                                                                 {
                                                                     allBlobs.Complete();
                                                                     uniqueFingerprints.Complete();
                                                                 });

                var distinctPaths = paths.Select(p => p.Path).Distinct(StringComparer.InvariantCultureIgnoreCase).ToArray();

                var loadBlobTask = Task.Run(() => blobManager.Load(distinctPaths, blobDispatcher));

                tasks.Add(loadBlobTask);

                if (UpdateLinks)
                {
                    var livePaths = paths.Where(p => null != p.Name).Distinct().ToLookup(p => p.Name, p => p.Path);

                    if (livePaths.Count > 0)
                    {
                        var linksTask = CreateLinksAsync(s3Manager, livePaths, allBlobs);

                        tasks.Add(linksTask);
                    }
                }

                s3Manager.ScanAsync(CancellationToken.None).Wait();

                var knowObjects = s3Manager.Keys;

                if (UploadBlobs)
                {
                    var uploader = new ActionBlock<IBlob>(
                        async blob =>
                        {
                            if (knowObjects.ContainsKey(HttpServerUtility.UrlTokenEncode(blob.Fingerprint.Sha3_512)))
                                return;

                            try
                            {
                                // ReSharper disable once AccessToDisposedClosure
                                await UploadBlobAsync(s3Manager, blob, CancellationToken.None).ConfigureAwait(false);

                                return;
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine("Upload of {0} failed (retrying): {1}", blob.FullPath, ex.Message);
                            }

                            await uniqueFingerprints.SendAsync(blob).ConfigureAwait(false);
                        }, new ExecutionDataflowBlockOptions
                                 {
                                     MaxDegreeOfParallelism = 12
                                 });

                    uniqueFingerprints.LinkTo(uploader, DataflowLinkOptionsPropagateEnabled);

                    tasks.Add(uploader.Completion);
                }

                return Task.WhenAll(tasks);
            }
        }

        static async Task CreateLinksAsync(S3Manager s3Manager, ILookup<string, string> linkPaths, BufferBlock<IBlob> allBlobs)
        {
            // ReSharper disable once AccessToDisposedClosure
            var pathTasks = linkPaths.Select(
                namePath => new
                            {
                                NamePath = namePath,
                                TreeTask = s3Manager.ListTreeAsync(namePath.Key, CancellationToken.None)
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
                                                   .Select(path => CreateLinkAsync(s3Manager, linkTree.NamePath.Key, path, blob, linkTree.Tree))
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

        static async Task BuildTreeAsync(S3Manager s3Manager, string name, string path, ILookup<IBlobFingerprint, IBlob> uniqueBlobs)
        {
            if (!path.EndsWith("/", StringComparison.OrdinalIgnoreCase) && !path.EndsWith("\\", StringComparison.OrdinalIgnoreCase))
                path += "\\";

            var tree = await s3Manager.ListTreeAsync(name, CancellationToken.None).ConfigureAwait(false);

            var tasks = uniqueBlobs
                .AsParallel()
                .SelectMany(b => b)
                .Select(blob => CreateLinkAsync(s3Manager, name, path, blob, tree))
                .Where(t => null != t)
                .ToArray();

            await Task.WhenAll(tasks).ConfigureAwait(false);
        }

        static Task CreateLinkAsync(S3Manager s3Manager, string name, string path, IBlob blob, IReadOnlyDictionary<string, string> tree)
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

            return s3Manager.CreateLinkAsync(name, relativePath, blob, CancellationToken.None);
        }

        static Task UploadBlobAsync(S3Manager s3Manager, IBlob blob, CancellationToken cancellationToken)
        {
            Console.WriteLine("Upload {0} as {1}", blob.FullPath, HttpServerUtility.UrlTokenEncode(blob.Fingerprint.Sha3_512.Take(10).ToArray()));

            if (!ActuallyWrite)
                return Task.FromResult(false);

            return s3Manager.StoreAsync(blob, cancellationToken);
        }

        static Task UploadBlobsAsync(S3Manager s3Manager, ILookup<IBlobFingerprint, IBlob> uniqueBlobs, IReadOnlyDictionary<string, long> knowObjects)
        {
            var queue = new ConcurrentBag<IBlob>(uniqueBlobs
                .Where(blob => !knowObjects.ContainsKey(HttpServerUtility.UrlTokenEncode(blob.Key.Sha3_512)))
                .Select(blobs => blobs.First()));

            var count = Math.Max(Environment.ProcessorCount * 2, 12);

            var workerTasks = Enumerable.Range(1, count)
                                        .Select(async i =>
                                                      {
                                                          for (; ; )
                                                          {
                                                              IBlob blob;
                                                              if (!queue.TryTake(out blob))
                                                                  return;

                                                              try
                                                              {
                                                                  await UploadBlobAsync(s3Manager, blob, CancellationToken.None).ConfigureAwait(false);
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
