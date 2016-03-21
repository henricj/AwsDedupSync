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
        readonly Task _cacheManager;
        readonly CancellationTokenSource _cancellationTokenSource;
        readonly StreamFingerprinter _fingerprinter;
        readonly TaskCompletionSource<object> _managerDone = new TaskCompletionSource<object>();
        readonly ConcurrentQueue<IBlob> _updateKnownBlobs = new ConcurrentQueue<IBlob>();
        readonly ConcurrentDictionary<BlobFingerprint, ConcurrentBag<IBlob>> _knownFingerprints
            = new ConcurrentDictionary<BlobFingerprint, ConcurrentBag<IBlob>>();

        Dictionary<string, IBlob> _previouslyCachedBlobs;

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
            if (!_cancellationTokenSource.IsCancellationRequested)
                _cancellationTokenSource.Cancel();

            _managerDone.Task.Wait();

            _cancellationTokenSource.Dispose();
        }

        #endregion

        async void ManageCache()
        {
            try
            {
                while (!_cancellationTokenSource.Token.IsCancellationRequested)
                {
                    await Task.Delay(5000, _cancellationTokenSource.Token).ConfigureAwait(false);

                    if (_updateKnownBlobs.Count > 0)
                        UpdateBlobs();
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
            finally
            {
                _managerDone.TrySetResult(string.Empty);
            }
        }

        void UpdateBlobs()
        {
            var blobs = GetBlobs();

            Trace.WriteLine($"BlobManager writing {blobs.Length} items to db");

            try
            {
                DBreezePathStore.StoreBlobs(blobs, _cancellationTokenSource.Token);
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
            _previouslyCachedBlobs = await DBreezePathStore.LoadBlobsAsync(_cancellationTokenSource.Token).ConfigureAwait(false);

            Debug.WriteLine($"Loaded {_previouslyCachedBlobs.Count} known blobs");

            foreach (var blob in _previouslyCachedBlobs.Values)
                AddFingerprint(blob);

            if (_cacheManager.Status == TaskStatus.Created)
                _cacheManager.Start();

            await GenerateBlobsAsync(paths, blobTargetBlock).ConfigureAwait(false);
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

                var routeBlock = new ActionBlock<AnnotatedPath[]>(
                    async filenames =>
                    {
                        RandomUtil.Shuffle(filenames);

                        foreach (var filename in filenames)
                        {
                            var host = GetHost(filename.FullPath);

                            TransformBlock<AnnotatedPath, IBlob> target;
                            while (!targets.TryGetValue(host, out target))
                            {
                                target = new TransformBlock<AnnotatedPath, IBlob>((Func<AnnotatedPath, Task<IBlob>>)ProcessFileAsync,
                                    new ExecutionDataflowBlockOptions
                                    {
                                        MaxDegreeOfParallelism = 5
                                    });

                                if (!targets.TryAdd(host, target))
                                    continue;

                                target.LinkTo(blobTargetBlock, blob => null != blob);
                                target.LinkTo(DataflowBlock.NullTarget<IBlob>());

                                break;
                            }

                            //Debug.WriteLine($"BlobManager.GenerateBlobsAsync() Sending {filename} for host '{host}'");

                            await target.SendAsync(filename).ConfigureAwait(false);
                        }
                    },
                    new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = DataflowBlockOptions.Unbounded });

                var batcher = new BatchBlock<AnnotatedPath>(1024);

                batcher.LinkTo(routeBlock, new DataflowLinkOptions
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
                    routeBlock.Complete();
                }

                await routeBlock.Completion.ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Console.WriteLine("GenerateBlobsAsync() failed: " + ex.Message);
            }
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

                            var relativePath = PathUtil.MakeRelativePath(path.Path, file);

                            var annotatedPath = new AnnotatedPath { FullPath = file, Collection = path.Name ?? path.Path, RelativePath = relativePath };

                            await filePathTargetBlock.SendAsync(annotatedPath).ConfigureAwait(false);
                        }
                    },
                        _cancellationTokenSource.Token,
                        TaskCreationOptions.DenyChildAttach | TaskCreationOptions.LongRunning | TaskCreationOptions.RunContinuationsAsynchronously,
                        TaskScheduler.Default));

            return Task.WhenAll(scanTasks);
        }

        async Task<IBlob> ProcessFileAsync(AnnotatedPath filename)
        {
            //Debug.WriteLine($"BlobManager.ProcessFileAsync({filename})");

            if (_cancellationTokenSource.Token.IsCancellationRequested)
                return null;

            var fp = _fingerprinter;

            try
            {
                var fi = new FileInfo(filename.FullPath);

                if (!fi.Exists)
                    return null;

                IBlob knownBlob;
                if (_previouslyCachedBlobs.TryGetValue(fi.FullName, out knownBlob))
                {
                    if (knownBlob.Fingerprint.Size == fi.Length && knownBlob.LastModifiedUtc == fi.LastWriteTimeUtc)
                        return knownBlob;
                }

                BlobFingerprint fingerprint;

                using (var s = new FileStream(filename.FullPath, FileMode.Open, FileSystemRights.Read, FileShare.Read,
                    8192, FileOptions.Asynchronous | FileOptions.SequentialScan))
                {
                    fingerprint = await fp.GetFingerprintAsync(s, _cancellationTokenSource.Token).ConfigureAwait(false);
                }

                if (fingerprint.Size != fi.Length)
                    return null;

                var blob = new Blob(fi.FullName, fi.LastWriteTimeUtc, fingerprint, filename.Collection, filename.RelativePath);

                AddFingerprint(blob);

                _updateKnownBlobs.Enqueue(blob);

                return blob;
            }
            catch (Exception ex)
            {
                Debug.WriteLine("File {0} failed: {1}", filename, ex.Message);
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
            public string FullPath { get; set; }
            public string Collection { get; set; }
            public string RelativePath { get; set; }
        }
    }
}
