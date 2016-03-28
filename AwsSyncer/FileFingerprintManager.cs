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
    public sealed class FileFingerprintManager : IDisposable
    {
        const int FlushCount = 1024;
        static readonly TimeSpan FlushInterval = TimeSpan.FromMinutes(10);
        readonly BsonFileFingerprintStore _blobPathStore = new BsonFileFingerprintStore();
        readonly StreamFingerprinter _fingerprinter;
        IReadOnlyDictionary<string, IFileFingerprint> _previouslyCachedFileFingerprints;

        public FileFingerprintManager(StreamFingerprinter fingerprinter)
        {
            _fingerprinter = fingerprinter;
        }

        public void Dispose()
        {
            try
            {
                ShutdownAsync(CancellationToken.None).Wait();
            }
            catch (Exception ex)
            {
                Debug.WriteLine("FileFingerprintManager.Dispose() CancelWorker() failed: " + ex.Message);
            }

            _blobPathStore.Dispose();
        }

        public async Task ShutdownAsync(CancellationToken cancellationToken)
        {
            Debug.WriteLine("FileFingerprintManager.ShutdownAsync()");

            await _blobPathStore.FlushAsync(cancellationToken).ConfigureAwait(false);
        }

        IFileFingerprint GetCachedFileFingerprint(FileInfo fileInfo)
        {
            IFileFingerprint fileFingerprint;
            if (!_previouslyCachedFileFingerprints.TryGetValue(fileInfo.FullName, out fileFingerprint))
                return null;

            if (fileFingerprint.Fingerprint.Size != fileInfo.Length || fileFingerprint.LastModifiedUtc != fileInfo.LastWriteTimeUtc)
            {
                Debug.WriteLine($"{fileInfo.FullName} changed {fileFingerprint.Fingerprint.Size} != {fileInfo.Length} || {fileFingerprint.LastModifiedUtc} != {fileInfo.LastAccessTime} ({fileFingerprint.LastModifiedUtc != fileInfo.LastWriteTimeUtc})");

                return null;
            }

            return fileFingerprint;
        }

        public async Task LoadAsync(CancellationToken cancellationToken)
        {
            //var task = GenerateFileFingerprintsAsync(annotatedPathSourceBlock, fileFingerprintTargetBlock, cancellationToken);

            // Most of the time, the load will happen synchronously.  We do async over sync here so our
            // caller will not block for too long.  We do need to make sure that the annotatedPathSourceBlock
            // has been linked to something before we await (GenerateFileFingerprintsAsync() takes care
            // of this before its first await).
            _previouslyCachedFileFingerprints = await Task.Run(() => _blobPathStore.LoadBlobsAsync(cancellationToken), cancellationToken).ConfigureAwait(false);

            Debug.WriteLine($"Loaded {_previouslyCachedFileFingerprints.Count} known files");
        }

        async Task<IFileFingerprint> ProcessFileAsync(FileInfo fileInfo, CancellationToken cancellationToken)
        {
            //Debug.WriteLine($"FileFingerprintManager.ProcessFileAsync({annotatedPath})");

            if (cancellationToken.IsCancellationRequested)
                return null;

            var fp = _fingerprinter;

            try
            {
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

                Debug.WriteLine($"FileFingerprintManager.ProcessFileAsync({fileInfo.FullName}) scanned {SizeConversion.BytesToMiB(fingerprint.Size):F3}MiB in {sw.Elapsed}");

                var fileFingerprint = new FileFingerprint(fileInfo.FullName, fileInfo.LastWriteTimeUtc, fingerprint);

                fileInfo.Refresh();

                if (fileInfo.LastWriteTimeUtc != fileFingerprint.LastModifiedUtc || fileInfo.Length != fileFingerprint.Fingerprint.Size)
                {
                    Debug.WriteLine($"FileFingerprintManager.ProcessFileAsync() {fileInfo.FullName} changed during scan");

                    return null;
                }

                return fileFingerprint;
            }
            catch (Exception ex)
            {
                Debug.WriteLine("FileFingerprintManager.ProcessFileAsync() File {0} failed: {1}", fileInfo.FullName, ex.Message);
                return null;
            }
        }

        public Task GenerateFileFingerprintsAsync(ISourceBlock<AnnotatedPath[]> annotatedPathSourceBlock,
            ITargetBlock<IFileFingerprint> fileFingerprintTargetBlock, CancellationToken cancellationToken)
        {
            var bufferBlock = new BufferBlock<IFileFingerprint>(new DataflowBlockOptions { CancellationToken = cancellationToken });

            bufferBlock.LinkTo(fileFingerprintTargetBlock, new DataflowLinkOptions { PropagateCompletion = true });

            var storeBatchBlock = new BatchBlock<IFileFingerprint>(FlushCount, new GroupingDataflowBlockOptions { CancellationToken = cancellationToken });

            var broadcastBlock = new BroadcastBlock<IFileFingerprint>(ff => ff, new ExecutionDataflowBlockOptions { CancellationToken = cancellationToken });

            broadcastBlock.LinkTo(storeBatchBlock, new DataflowLinkOptions { PropagateCompletion = true }, ff => !ff.WasCached);
            broadcastBlock.LinkTo(bufferBlock, new DataflowLinkOptions { PropagateCompletion = true });

#if DEBUG
            var task = storeBatchBlock.Completion
                .ContinueWith(_ => Debug.WriteLine("FileFingerprintManager.GenerateFileFingerprintsAsync() storeBatchBlock completed"));

            TaskCollector.Default.Add(task, "FileFingerprintManager.GenerateFileFingerprintsAsync storeBatchBlock");

            task = bufferBlock.Completion
                .ContinueWith(_ => Debug.WriteLine("FileFingerprintManager.GenerateFileFingerprintsAsync() bufferBlock completed"));

            TaskCollector.Default.Add(task, "FileFingerprintManager.GenerateFileFingerprintsAsync bufferBlock");
#endif // DEBUG

            var storeTask = StoreFileFingerprintsAsync(storeBatchBlock, cancellationToken);

            var transformTask = TransformAnnotatedPathsToFileFingerprint(annotatedPathSourceBlock, broadcastBlock, cancellationToken);

            return Task.WhenAll(storeTask, transformTask);
        }

        Task StoreFileFingerprintsAsync(ISourceBlock<IFileFingerprint[]> storeBatchBlock, CancellationToken cancellationToken)
        {
            var block = new ActionBlock<IFileFingerprint[]>(
                fileFingerprints => WriteBlobsAsync(fileFingerprints, cancellationToken),
                new ExecutionDataflowBlockOptions { CancellationToken = cancellationToken });

            storeBatchBlock.LinkTo(block, new DataflowLinkOptions { PropagateCompletion = true });

            return block.Completion;
        }

        async Task WriteBlobsAsync(ICollection<IFileFingerprint> fileFingerprints, CancellationToken cancellationToken)
        {
            Debug.WriteLine($"FileFingerprintManager writing {fileFingerprints.Count} items to db");

            try
            {
                await _blobPathStore.StoreBlobsAsync(fileFingerprints, cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                Debug.WriteLine($"FileFingerprintManager store of {fileFingerprints.Count} items failed");
            }
        }

        async Task TransformAnnotatedPathsToFileFingerprint(ISourceBlock<AnnotatedPath[]> annotatedPathSourceBlock,
            ITargetBlock<IFileFingerprint> fileFingerprintTargetBlock, CancellationToken cancellationToken)
        {
            try
            {
                var targets = new ConcurrentDictionary<string, TransformBlock<AnnotatedPath, IFileFingerprint>>(StringComparer.InvariantCultureIgnoreCase);

                var routeBlock = new ActionBlock<AnnotatedPath[]>(
                    async filenames =>
                    {
                        foreach (var filename in filenames)
                        {
                            if (null == filename)
                                continue;

                            var cachedBlob = GetCachedFileFingerprint(filename.FileInfo);

                            if (null != cachedBlob)
                            {
                                await fileFingerprintTargetBlock.SendAsync(cachedBlob, cancellationToken).ConfigureAwait(false);

                                continue;
                            }

                            var host = PathUtil.GetHost(filename.FileInfo.FullName);

                            TransformBlock<AnnotatedPath, IFileFingerprint> target;
                            while (!targets.TryGetValue(host, out target))
                            {
                                target = new TransformBlock<AnnotatedPath, IFileFingerprint>(annotatedPath => ProcessFileAsync(annotatedPath.FileInfo, cancellationToken),
                                    new ExecutionDataflowBlockOptions
                                    {
                                        MaxDegreeOfParallelism = 5,
                                        CancellationToken = cancellationToken
                                    });

                                if (!targets.TryAdd(host, target))
                                    continue;

                                Debug.WriteLine($"FileFingerprintManager.GenerateBlobsAsync() starting reader for host: '{host}'");

                                target.LinkTo(fileFingerprintTargetBlock, blob => null != blob);
                                target.LinkTo(DataflowBlock.NullTarget<IFileFingerprint>());

                                break;
                            }

                            //Debug.WriteLine($"FileFingerprintManager.GenerateFileFingerprintsAsync() Sending {annotatedPath} for host '{host}'");

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

                annotatedPathSourceBlock.LinkTo(distinctBlock, new DataflowLinkOptions { PropagateCompletion = true });

                await routeBlock.Completion.ConfigureAwait(false);

                foreach (var target in targets.Values)
                    target.Complete();

                await Task.WhenAll(targets.Values.Select(target => target.Completion));
            }
            catch (Exception ex)
            {
                Console.WriteLine("FileFingerprintManager.GenerateFileFingerprintsAsync() failed: " + ex.Message);
            }
            finally
            {
                Debug.WriteLine("FileFingerprintManager.GenerateFileFingerprintsAsync() is done");

                fileFingerprintTargetBlock.Complete();
            }
        }
    }
}
