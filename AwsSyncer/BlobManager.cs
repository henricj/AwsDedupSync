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
using System.Security.AccessControl;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace AwsSyncer
{
    public sealed class BlobManager : IDisposable
    {
        const int FlushCount = 1024;
        static readonly TimeSpan FlushInterval = TimeSpan.FromMinutes(10);
        readonly BsonFilePathStore _blobPathStore = new BsonFilePathStore();
        readonly StreamFingerprinter _fingerprinter;

        readonly ConcurrentDictionary<BlobFingerprint, ConcurrentDictionary<Tuple<string, string>, IBlob>> _knownFingerprints
            = new ConcurrentDictionary<BlobFingerprint, ConcurrentDictionary<Tuple<string, string>, IBlob>>();

        IReadOnlyDictionary<string, IBlob> _previouslyCachedBlobs;

        public BlobManager(StreamFingerprinter fingerprinter)
        {
            if (null == fingerprinter)
                throw new ArgumentNullException(nameof(fingerprinter));

            _fingerprinter = fingerprinter;
        }

        public IReadOnlyDictionary<BlobFingerprint, ConcurrentDictionary<Tuple<string, string>, IBlob>> AllBlobs => _knownFingerprints;

        #region IDisposable Members

        public void Dispose()
        {
            try
            { }
            catch (Exception ex)
            {
                Debug.WriteLine("BlobManager.Dispose() CancelWorker() failed: " + ex.Message);
            }

            _blobPathStore.Dispose();
        }

        #endregion

        async Task WriteBlobsAsync(BsonFilePathStore blobPathStore, IList<IBlob> blobs, CancellationToken cancellationToken)
        {
            Debug.WriteLine($"BlobManager writing {blobs.Count} items to db");

            try
            {
                await blobPathStore.StoreBlobsAsync(blobs, cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                Debug.WriteLine($"BlobManager store of {blobs.Count} items failed");
            }
        }

        public async Task LoadAsync(CollectionPath[] paths, ITargetBlock<IBlob> blobTargetBlock, CancellationToken cancellationToken)
        {
            _previouslyCachedBlobs = await _blobPathStore.LoadBlobsAsync(cancellationToken).ConfigureAwait(false);

            Debug.WriteLine($"Loaded {_previouslyCachedBlobs.Count} known blobs");

            foreach (var blob in _previouslyCachedBlobs.Values)
                AddFingerprint(blob);

            await GenerateBlobsAsync(paths, blobTargetBlock, cancellationToken).ConfigureAwait(false);
        }

        public async Task ShutdownAsync(CancellationToken cancellationToken)
        {
            Debug.WriteLine("BlobManager.ShutdownAsync()");

            await _blobPathStore.FlushAsync(cancellationToken).ConfigureAwait(false);
        }

        static string GetHost(string path)
        {
            var uri = new Uri(path, UriKind.RelativeOrAbsolute);

            return uri.IsAbsoluteUri ? uri.Host : string.Empty;
        }

        async Task GenerateBlobsAsync(CollectionPath[] paths, ITargetBlock<IBlob> blobTargetBlock, CancellationToken cancellationToken)
        {
            try
            {
                var blobCacheBlock = new BatchBlock<IBlob>(FlushCount, new GroupingDataflowBlockOptions { CancellationToken = cancellationToken });

                var storeCacheBlock = new ActionBlock<IBlob[]>(async blobs =>
                {
                    try
                    {
                        await WriteBlobsAsync(_blobPathStore, blobs, cancellationToken).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        // Normal...
                    }
                    catch (Exception)
                    {
                        Debug.WriteLine("UpdateBlobsAsync() failed");
                    }
                });

                blobCacheBlock.LinkTo(storeCacheBlock, new DataflowLinkOptions { PropagateCompletion = true });

                var targets = new ConcurrentDictionary<string, TransformBlock<AnnotatedPath, IBlob>>(StringComparer.InvariantCultureIgnoreCase);

                var broadcastBlock = new BroadcastBlock<IBlob>(b => b, new DataflowBlockOptions { CancellationToken = cancellationToken });

                broadcastBlock.LinkTo(blobCacheBlock, new DataflowLinkOptions { PropagateCompletion = true });
                broadcastBlock.LinkTo(blobTargetBlock, new DataflowLinkOptions { PropagateCompletion = true });

                var routeBlock = new ActionBlock<AnnotatedPath[]>(
                    async filenames =>
                    {
                        RandomUtil.Shuffle(filenames);

                        foreach (var filename in filenames)
                        {
                            if (null == filename)
                                continue;

                            var cachedBlob = GetCachedBlob(filename.FileInfo);

                            if (null != cachedBlob)
                            {
                                await blobTargetBlock.SendAsync(cachedBlob, cancellationToken).ConfigureAwait(false);

                                continue;
                            }

                            var host = GetHost(filename.FileInfo.FullName);

                            TransformBlock<AnnotatedPath, IBlob> target;
                            while (!targets.TryGetValue(host, out target))
                            {
                                target = new TransformBlock<AnnotatedPath, IBlob>(annotatedPath => ProcessFileAsync(annotatedPath, cancellationToken),
                                    new ExecutionDataflowBlockOptions
                                    {
                                        MaxDegreeOfParallelism = 5,
                                        CancellationToken = cancellationToken
                                    });

                                if (!targets.TryAdd(host, target))
                                    continue;

                                Debug.WriteLine($"BlobManager.GenerateBlobsAsync() starting reader for host: '{host}'");

                                target.LinkTo(broadcastBlock, blob => null != blob);
                                target.LinkTo(DataflowBlock.NullTarget<IBlob>());

                                break;
                            }

                            //Debug.WriteLine($"BlobManager.GenerateBlobsAsync() Sending {annotatedPath} for host '{host}'");

                            await target.SendAsync(filename, cancellationToken).ConfigureAwait(false);
                        }
                    },
                    new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 16, CancellationToken = cancellationToken });

                var distinctPaths = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase);

                var distinctBlock = new TransformBlock<AnnotatedPath[], AnnotatedPath[]>(
                    annotatedPaths =>
                    {
                        for (var i = 0; i < annotatedPaths.Length; ++i)
                        {
                            if (!distinctPaths.Add(annotatedPaths[i].FileInfo.FullName))
                                annotatedPaths[i] = null;
                        }

                        return annotatedPaths;
                    },
                    new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 1, CancellationToken = cancellationToken });

                distinctBlock.LinkTo(routeBlock, new DataflowLinkOptions { PropagateCompletion = true });

                var batcher = new BatchBlock<AnnotatedPath>(1024, new GroupingDataflowBlockOptions { CancellationToken = cancellationToken });

                batcher.LinkTo(distinctBlock, new DataflowLinkOptions
                {
                    PropagateCompletion = true
                });

                try
                {
                    await PostAllFilePathsAsync(paths, batcher, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                { }
                catch (Exception ex)
                {
                    Console.Write("Path scan failed: " + ex.Message);
                }

                batcher.Complete();

                await routeBlock.Completion.ConfigureAwait(false);

                foreach (var target in targets.Values)
                    target.Complete();

                await Task.WhenAll(targets.Values.Select(target => target.Completion));

                broadcastBlock.Complete();

                await storeCacheBlock.Completion.ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Console.WriteLine("GenerateBlobsAsync() failed: " + ex.Message);
            }
            finally
            {
                Debug.WriteLine("BlobManager.GenerateBlobsAsync() is done");
            }
        }

        IBlob GetCachedBlob(FileInfo fileInfo)
        {
            IBlob blob;
            if (!_previouslyCachedBlobs.TryGetValue(fileInfo.FullName, out blob))
                return null;

            if (blob.Fingerprint.Size != fileInfo.Length || blob.LastModifiedUtc != fileInfo.LastWriteTimeUtc)
            {
                Debug.WriteLine($"{fileInfo.FullName} changed {blob.Fingerprint.Size} != {fileInfo.Length} || {blob.LastModifiedUtc} != {fileInfo.LastAccessTime} ({blob.LastModifiedUtc != fileInfo.LastWriteTimeUtc})");

                return null;
            }

            return blob;
        }

        static Task PostAllFilePathsAsync(CollectionPath[] paths, ITargetBlock<AnnotatedPath> filePathTargetBlock, CancellationToken cancellationToken)
        {
            var scanTasks = paths
                .Select<CollectionPath, Task>(path =>
                    Task.Factory.StartNew(async () =>
                    {
                        foreach (var file in PathUtil.ScanDirectory(path.Path))
                        {
                            if (cancellationToken.IsCancellationRequested)
                                break;

                            var relativePath = PathUtil.MakeRelativePath(path.Path, file.FullName);

                            var annotatedPath = new AnnotatedPath { FileInfo = file, Collection = path.Name ?? path.Path, RelativePath = relativePath };

                            await filePathTargetBlock.SendAsync(annotatedPath, cancellationToken).ConfigureAwait(false);
                        }
                    },
                        cancellationToken,
                        TaskCreationOptions.DenyChildAttach | TaskCreationOptions.LongRunning | TaskCreationOptions.RunContinuationsAsynchronously,
                        TaskScheduler.Default));

            return Task.WhenAll(scanTasks);
        }

        async Task<IBlob> ProcessFileAsync(AnnotatedPath annotatedPath, CancellationToken cancellationToken)
        {
            //Debug.WriteLine($"BlobManager.ProcessFileAsync({annotatedPath})");

            if (cancellationToken.IsCancellationRequested)
                return null;

            var fp = _fingerprinter;

            try
            {
                var fileInfo = annotatedPath.FileInfo;

                fileInfo.Refresh();

                if (!fileInfo.Exists)
                    return null;

                BlobFingerprint fingerprint;

                var sw = Stopwatch.StartNew();

                using (var s = new FileStream(fileInfo.FullName, FileMode.Open, FileSystemRights.Read, FileShare.Read,
                    8192, FileOptions.Asynchronous | FileOptions.SequentialScan))
                {
                    fingerprint = await fp.GetFingerprintAsync(s, cancellationToken).ConfigureAwait(false);
                }

                sw.Stop();

                if (fingerprint.Size != fileInfo.Length)
                    return null;

                Debug.WriteLine($"BlobManager.ProcessFileAsync({annotatedPath.FileInfo.FullName}) scanned {SizeConversion.BytesToMiB(fingerprint.Size):F3}MiB in {sw.Elapsed}");

                var blob = new Blob(fileInfo.FullName, fileInfo.LastWriteTimeUtc, fingerprint, annotatedPath.Collection, annotatedPath.RelativePath);

                fileInfo.Refresh();

                if (fileInfo.LastWriteTimeUtc != blob.LastModifiedUtc || fileInfo.Length != blob.Fingerprint.Size)
                {
                    Debug.WriteLine($"BlobManager.ProcessFileAsync() {annotatedPath.FileInfo.FullName} changed during scan");

                    return null;
                }

                AddFingerprint(blob);

                return blob;
            }
            catch (Exception ex)
            {
                Debug.WriteLine("File {0} failed: {1}", annotatedPath.FileInfo.FullName, ex.Message);
                return null;
            }
        }

        void AddFingerprint(IBlob blob)
        {
            var key = Tuple.Create(blob.Collection, blob.RelativePath);

            ConcurrentDictionary<Tuple<string, string>, IBlob> blobs;

            while (!_knownFingerprints.TryGetValue(blob.Fingerprint, out blobs))
            {
                blobs = new ConcurrentDictionary<Tuple<string, string>, IBlob>();

                if (_knownFingerprints.TryAdd(blob.Fingerprint, blobs))
                    break;
            }

            blobs[key] = blob;
        }

        class AnnotatedPath
        {
            public FileInfo FileInfo { get; set; }
            public string Collection { get; set; }
            public string RelativePath { get; set; }
        }
    }
}
