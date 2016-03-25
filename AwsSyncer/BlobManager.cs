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
        const int FlushCount = 5000;
        static readonly TimeSpan FlushInterval = TimeSpan.FromMinutes(10);
        readonly BsonFilePathStore _blobPathStore = new BsonFilePathStore();
        readonly Task _cacheManager;
        readonly CancellationTokenSource _cancellationTokenSource;
        readonly StreamFingerprinter _fingerprinter;

        readonly ConcurrentDictionary<BlobFingerprint, ConcurrentBag<IBlob>> _knownFingerprints
            = new ConcurrentDictionary<BlobFingerprint, ConcurrentBag<IBlob>>();

        readonly ConcurrentQueue<IBlob> _updateKnownBlobs = new ConcurrentQueue<IBlob>();

        IReadOnlyDictionary<string, IBlob> _previouslyCachedBlobs;

        public BlobManager(StreamFingerprinter fingerprinter, CancellationToken cancellationToken)
        {
            if (null == fingerprinter)
                throw new ArgumentNullException(nameof(fingerprinter));

            _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

            _fingerprinter = fingerprinter;
            _cacheManager = new Task(ManageCache);
        }

        public IReadOnlyDictionary<BlobFingerprint, ConcurrentBag<IBlob>> AllBlobs => _knownFingerprints;

        #region IDisposable Members

        public void Dispose()
        {
            ShutdownAsync().Wait();

            _blobPathStore.Dispose();

            _cancellationTokenSource.Dispose();
        }

        #endregion

        async void ManageCache()
        {
            var blobUpdateCount = 0;
            var blobUpdateSize = 0L;

            try
            {
                var rng = RandomUtil.CreateRandom();

                var timeSinceFlush = Stopwatch.StartNew();
                var countSinceFlush = 0;

                while (!_cancellationTokenSource.Token.IsCancellationRequested)
                {
                    await Task.Delay(7500 + rng.Next(2500), _cancellationTokenSource.Token).ConfigureAwait(false);

                    var count = _updateKnownBlobs.Count;

                    if (count <= 0)
                        continue;

                    try
                    {
                        UpdateBlobs(_blobPathStore);

                        countSinceFlush += count;
                    }
                    catch (OperationCanceledException)
                    { }
                    catch (Exception ex)
                    {
                        Debug.WriteLine("Cache update failed: " + ex.Message);
                    }

                    if (countSinceFlush > FlushCount || (countSinceFlush > 0 && timeSinceFlush.Elapsed > FlushInterval))
                    {
                        try
                        {
                            Debug.WriteLine($"Flushing cache to disk after {countSinceFlush} files and {timeSinceFlush.Elapsed}");

                            _blobPathStore.Flush();

                            timeSinceFlush = Stopwatch.StartNew();
                            countSinceFlush = 0;
                        }
                        catch (OperationCanceledException)
                        { }
                        catch (Exception ex)
                        {
                            Debug.WriteLine("Cache flush to disk failed: " + ex.Message);
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Normal...
            }
            catch (Exception ex)
            {
                Console.WriteLine("BlobManager's cache task failed: " + ex.Message);
            }

            blobUpdateCount = _blobPathStore.BlobUpdateCount;
            blobUpdateSize = _blobPathStore.BlobUpdateSize;

            Console.WriteLine($"Shutting down updates after scanning {blobUpdateCount} files of {SizeConversion.BytesToGiB(blobUpdateSize):F3}GiB");
        }

        void UpdateBlobs(BsonFilePathStore blobPathStore)
        {
            var blobs = GetBlobs();

            Trace.WriteLine($"BlobManager writing {blobs.Length} items to db");

            try
            {
                blobPathStore.StoreBlobs(blobs, _cancellationTokenSource.Token);
            }
            catch
            {
                Trace.WriteLine($"BlobManager returning {blobs.Length} items to queue");

                foreach (var blob in blobs)
                    _updateKnownBlobs.Enqueue(blob);

                throw;
            }
        }

        IBlob[] GetBlobs()
        {
            var list = new List<IBlob>(_updateKnownBlobs.Count);

            IBlob blob;
            while (_updateKnownBlobs.TryDequeue(out blob))
                list.Add(blob);

            _cancellationTokenSource.Token.ThrowIfCancellationRequested();

            return list.OrderBy(b => b.FullFilePath).ToArray();
        }

        public async Task LoadAsync(CollectionPath[] paths, ITargetBlock<IBlob> blobTargetBlock)
        {
            _previouslyCachedBlobs = await _blobPathStore.LoadBlobsAsync(_cancellationTokenSource.Token).ConfigureAwait(false);

            Debug.WriteLine($"Loaded {_previouslyCachedBlobs.Count} known blobs");

            foreach (var blob in _previouslyCachedBlobs.Values)
                AddFingerprint(blob);

            if (_cacheManager.Status == TaskStatus.Created)
                _cacheManager.Start();

            await GenerateBlobsAsync(paths, blobTargetBlock).ConfigureAwait(false);
        }

        public Task ShutdownAsync()
        {
            if (!_cancellationTokenSource.IsCancellationRequested)
                _cancellationTokenSource.Cancel();

            if (_cacheManager.Status == TaskStatus.Created)
                return Task.CompletedTask;

            return _cacheManager;
        }

        static string GetHost(string path)
        {
            var uri = new Uri(path, UriKind.RelativeOrAbsolute);

            return uri.IsAbsoluteUri ? uri.Host : string.Empty;
        }

        async Task GenerateBlobsAsync(CollectionPath[] paths, ITargetBlock<IBlob> blobTargetBlock)
        {
            try
            {
                var targets = new ConcurrentDictionary<string, TransformBlock<AnnotatedPath, IBlob>>(StringComparer.InvariantCultureIgnoreCase);

                var cancellationToken = _cancellationTokenSource.Token;

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
                                target = new TransformBlock<AnnotatedPath, IBlob>((Func<AnnotatedPath, Task<IBlob>>)ProcessFileAsync,
                                    new ExecutionDataflowBlockOptions
                                    {
                                        MaxDegreeOfParallelism = 5,
                                        CancellationToken = cancellationToken
                                    });

                                if (!targets.TryAdd(host, target))
                                    continue;

                                Debug.WriteLine($"BlobManager.GenerateBlobsAsync() starting reader for host: '{host}'");

                                target.LinkTo(blobTargetBlock, blob => null != blob);
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

                var completeTask = routeBlock.Completion.ContinueWith(
                    _ =>
                    {
                        Task.WhenAll(targets.Values.Select(target => target.Completion))
                            .ContinueWith(__ => blobTargetBlock.Complete());

                        foreach (var target in targets.Values)
                            target.Complete();
                    });

                try
                {
                    await PostAllFilePathsAsync(paths, batcher).ConfigureAwait(false);
                }
                finally
                {
                    batcher.Complete();
                }

                await routeBlock.Completion.ConfigureAwait(false);
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

        Task PostAllFilePathsAsync(CollectionPath[] paths, ITargetBlock<AnnotatedPath> filePathTargetBlock)
        {
            var scanTasks = paths
                .Select<CollectionPath, Task>(path =>
                    Task.Factory.StartNew(async () =>
                    {
                        foreach (var file in PathUtil.ScanDirectory(path.Path))
                        {
                            if (_cancellationTokenSource.Token.IsCancellationRequested)
                                break;

                            var relativePath = PathUtil.MakeRelativePath(path.Path, file.FullName);

                            var annotatedPath = new AnnotatedPath { FileInfo = file, Collection = path.Name ?? path.Path, RelativePath = relativePath };

                            await filePathTargetBlock.SendAsync(annotatedPath).ConfigureAwait(false);
                        }
                    },
                        _cancellationTokenSource.Token,
                        TaskCreationOptions.DenyChildAttach | TaskCreationOptions.LongRunning | TaskCreationOptions.RunContinuationsAsynchronously,
                        TaskScheduler.Default));

            return Task.WhenAll(scanTasks);
        }

        async Task<IBlob> ProcessFileAsync(AnnotatedPath annotatedPath)
        {
            //Debug.WriteLine($"BlobManager.ProcessFileAsync({annotatedPath})");

            if (_cancellationTokenSource.Token.IsCancellationRequested)
                return null;

            var fp = _fingerprinter;

            try
            {
                var fileInfo = annotatedPath.FileInfo;

                fileInfo.Refresh();

                if (!fileInfo.Exists)
                    return null;

                var knownBlob = GetCachedBlob(fileInfo);

                if (null != knownBlob)
                    return knownBlob;

                BlobFingerprint fingerprint;

                var sw = Stopwatch.StartNew();

                using (var s = new FileStream(fileInfo.FullName, FileMode.Open, FileSystemRights.Read, FileShare.Read,
                    8192, FileOptions.Asynchronous | FileOptions.SequentialScan))
                {
                    fingerprint = await fp.GetFingerprintAsync(s, _cancellationTokenSource.Token).ConfigureAwait(false);
                }

                sw.Stop();

                if (fingerprint.Size != fileInfo.Length)
                    return null;

                Debug.WriteLine($"BlobManager.ProcessFileAsync({annotatedPath.FileInfo.FullName}) created {SizeConversion.BytesToMiB(fingerprint.Size):F3}MiB in {sw.Elapsed}");

                var blob = new Blob(fileInfo.FullName, fileInfo.LastWriteTimeUtc, fingerprint, annotatedPath.Collection, annotatedPath.RelativePath);

                AddFingerprint(blob);

                _updateKnownBlobs.Enqueue(blob);

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
            ConcurrentBag<IBlob> blobs;
            while (!_knownFingerprints.TryGetValue(blob.Fingerprint, out blobs))
            {
                blobs = new ConcurrentBag<IBlob>();
                if (_knownFingerprints.TryAdd(blob.Fingerprint, blobs))
                    break;
            }

            blobs.Add(blob);
        }

        class AnnotatedPath
        {
            public FileInfo FileInfo { get; set; }
            public string Collection { get; set; }
            public string RelativePath { get; set; }
        }
    }
}
