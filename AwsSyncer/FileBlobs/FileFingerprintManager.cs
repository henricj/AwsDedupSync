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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using AwsSyncer.FingerprintStore;
using AwsSyncer.Types;
using AwsSyncer.Utility;
using Microsoft.Win32.SafeHandles;

namespace AwsSyncer.FileBlobs;

public interface IFileFingerprintManager : IAsyncDisposable
{
    Task ShutdownAsync(CancellationToken cancellationToken);
    Task LoadAsync(CancellationToken cancellationToken);

    Task GenerateFileFingerprintsAsync(ISourceBlock<AnnotatedPath[]> annotatedPathSourceBlock,
        ITargetBlock<FileFingerprint> fileFingerprintTargetBlock,
        CancellationToken cancellationToken);

    void InvalidateFingerprint(FileFingerprint fingerprint);
}

public sealed class FileFingerprintManager(IFileFingerprintStore fileFingerprintStore, IStreamFingerprinter fingerprinter)
    : IFileFingerprintManager
{
    const int FlushCount = 1024;
    static readonly TimeSpan ModifiedDelay = TimeSpan.FromMilliseconds(15);
    static readonly TimeSpan ModifiedRandomDelay = TimeSpan.FromMilliseconds(20);

    //static readonly TimeSpan FlushInterval = TimeSpan.FromMinutes(10);
    readonly IFileFingerprintStore _blobPathStore = fileFingerprintStore
        ?? throw new ArgumentNullException(nameof(fileFingerprintStore));

    readonly IStreamFingerprinter _fingerprinter = fingerprinter
        ?? throw new ArgumentNullException(nameof(fingerprinter));

    readonly ConcurrentBag<FileFingerprint> _invalidFingerprints = [];

    IReadOnlyDictionary<string, FileFingerprint> _previouslyCachedFileFingerprints;

    public async ValueTask DisposeAsync()
    {
        try
        {
            await ShutdownAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Debug.WriteLine("FileFingerprintManager.Dispose() CancelWorker() failed: " + ex.Message);
        }

        await _blobPathStore.DisposeAsync();
    }

    public async Task ShutdownAsync(CancellationToken cancellationToken)
    {
        Debug.WriteLine("FileFingerprintManager.ShutdownAsync()");

        if (!_invalidFingerprints.IsEmpty)
        {
            await _blobPathStore.StoreBlobsAsync(_invalidFingerprints, cancellationToken).ConfigureAwait(false);
            _invalidFingerprints.Clear();
        }

        await _blobPathStore.CloseAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task LoadAsync(CancellationToken cancellationToken)
    {
        // Most of the time, the load will happen synchronously.  We do async over sync here so our
        // caller will not block for too long.  We do need to make sure that the annotatedPathSourceBlock
        // has been linked to something before we await (GenerateFileFingerprintsAsync() takes care
        // of this before its first await).
        _previouslyCachedFileFingerprints = await Task
            .Run(() => _blobPathStore.LoadBlobsAsync(cancellationToken), cancellationToken).ConfigureAwait(false);

        Debug.WriteLine($"Loaded {_previouslyCachedFileFingerprints.Count} known files");
    }

    public Task GenerateFileFingerprintsAsync(ISourceBlock<AnnotatedPath[]> annotatedPathSourceBlock,
        ITargetBlock<FileFingerprint> fileFingerprintTargetBlock,
        CancellationToken cancellationToken)
    {
        var bufferBlock = new BufferBlock<FileFingerprint>(new() { CancellationToken = cancellationToken });

        bufferBlock.LinkTo(fileFingerprintTargetBlock, new() { PropagateCompletion = true });

        var storeBatchBlock = new BatchBlock<FileFingerprint>(FlushCount,
            new() { CancellationToken = cancellationToken });

        var broadcastBlock = new BroadcastBlock<FileFingerprint>(ff => ff,
            new ExecutionDataflowBlockOptions { CancellationToken = cancellationToken });

        broadcastBlock.LinkTo(storeBatchBlock, new() { PropagateCompletion = true }, ff => !ff.WasCached);
        broadcastBlock.LinkTo(bufferBlock, new() { PropagateCompletion = true });

#if DEBUG
        var task = storeBatchBlock.Completion
            .ContinueWith(_ => Debug.WriteLine("FileFingerprintManager.GenerateFileFingerprintsAsync() storeBatchBlock completed"),
                CancellationToken.None);

        TaskCollector.Default.Add(task, "FileFingerprintManager.GenerateFileFingerprintsAsync storeBatchBlock");

        task = bufferBlock.Completion
            .ContinueWith(_ => Debug.WriteLine("FileFingerprintManager.GenerateFileFingerprintsAsync() bufferBlock completed"),
                CancellationToken.None);

        TaskCollector.Default.Add(task, "FileFingerprintManager.GenerateFileFingerprintsAsync bufferBlock");
#endif // DEBUG

        var storeTask = StoreFileFingerprintsAsync(storeBatchBlock, cancellationToken);

        var transformTask =
            TransformAnnotatedPathsToFileFingerprint(annotatedPathSourceBlock, broadcastBlock, cancellationToken);

        return Task.WhenAll(storeTask, transformTask);
    }

    public void InvalidateFingerprint(FileFingerprint fingerprint)
    {
        fingerprint.Invalidate();
        _invalidFingerprints.Add(fingerprint);
    }

    FileFingerprint GetCachedFileFingerprint(FileInfo fileInfo)
    {
        if (!_previouslyCachedFileFingerprints.TryGetValue(fileInfo.FullName, out var fileFingerprint))
            return null;

        if (fileFingerprint.Invalid)
        {
            Debug.WriteLine($"FileFingerprintManager.ProcessFileAsync() {fileInfo.FullName} is invalid");
            return null;
        }

        if (fileFingerprint.Fingerprint.Size != fileInfo.Length || fileFingerprint.LastModifiedUtc != fileInfo.LastWriteTimeUtc)
        {
            Debug.WriteLine(
                $"{fileInfo.FullName} changed {fileFingerprint.Fingerprint.Size} != {fileInfo.Length} || {fileFingerprint.LastModifiedUtc} != {fileInfo.LastAccessTime} ({fileFingerprint.LastModifiedUtc != fileInfo.LastWriteTimeUtc})");

            return null;
        }

        return fileFingerprint;
    }

    async Task<FileFingerprint> ProcessFileAsync(FileInfo fileInfo, CancellationToken cancellationToken)
    {
        //Debug.WriteLine($"FileFingerprintManager.ProcessFileAsync({annotatedPath})");

        if (cancellationToken.IsCancellationRequested)
            return null;

        for (var retry = 0; retry < 3; ++retry)
        {
            var fullName = fileInfo.FullName;

            try
            {
                if (!Path.Exists(fullName))
                {
                    Debug.WriteLine($"File \"{fullName}\" has disappeared.");
                    return null;
                }

                BlobFingerprint fingerprint;

                var sw = Stopwatch.StartNew();

                var s = new FileStream(fullName, FileMode.Open, FileAccess.Read, FileShare.Read,
                    0, FileOptions.Asynchronous | FileOptions.SequentialScan);

                DateTime modified0;
                await using (s.ConfigureAwait(false))
                {
                    var handle = s.SafeFileHandle;
                    Debug.Assert(handle is not null);

                    modified0 = await RobustTimestampAsync(handle, cancellationToken).ConfigureAwait(false);

                    var length0 = s.Length;
                    fingerprint = await _fingerprinter.GetFingerprintAsync(s, cancellationToken).ConfigureAwait(false);
                    var length1 = s.Length;

                    if (length0 != length1)
                    {
                        Debug.WriteLine($"FileFingerprintManager.ProcessFileAsync() \"{fullName}\" length changed during scan.");
                        continue;
                    }

                    if (length0 != fileInfo.Length)
                    {
                        Debug.WriteLine(
                            $"FileFingerprintManager.ProcessFileAsync() \"{fullName}\" length changed since file info was updated.");
                    }

                    Debug.Assert(handle == s.SafeFileHandle);

                    var modified1 = await RobustTimestampAsync(handle, cancellationToken).ConfigureAwait(false);
                    if (modified0 != modified1)
                    {
                        Debug.WriteLine(
                            $"FileFingerprintManager.ProcessFileAsync() {fullName} modification time changed during scan");
                        continue;
                    }
                }

                sw.Stop();

                Debug.WriteLine(
                    $"FileFingerprintManager.ProcessFileAsync() \"{fullName}\" scanned {fingerprint.Size.BytesToMiB():F3}MiB in {sw.Elapsed}");

                return new(fullName, modified0, fingerprint);
            }
            catch (OperationCanceledException)
            {
                return null;
            }
            catch (Exception ex)
            {
                Debug.WriteLine("FileFingerprintManager.ProcessFileAsync() File {0} failed: {1}", fullName, ex.Message);
            }
        }

        return null;
    }

    Task StoreFileFingerprintsAsync(BatchBlock<FileFingerprint> storeBatchBlock, CancellationToken cancellationToken)
    {
        var block = new ActionBlock<FileFingerprint[]>(
            fileFingerprints => WriteBlobsAsync(fileFingerprints, cancellationToken),
            new() { CancellationToken = cancellationToken });

        storeBatchBlock.LinkTo(block, new() { PropagateCompletion = true });

        return block.Completion;
    }

    async Task WriteBlobsAsync(IReadOnlyCollection<FileFingerprint> fileFingerprints, CancellationToken cancellationToken)
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

    /// <summary>
    ///     Get a timestamp that is repeatable even in the face of an apparent Samba bug sometimes
    ///     retuning the timestamp for a file other than the one asked for.
    /// </summary>
    /// <param name="handle"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="IOException"></exception>
    static async ValueTask<DateTime> RobustTimestampAsync(SafeFileHandle handle, CancellationToken cancellationToken)
    {
        var match = 0;
        var last = File.GetLastWriteTimeUtc(handle);
        for (var retry = 0; retry < 25; ++retry)
        {
            var modified = File.GetLastWriteTimeUtc(handle);
            if (modified == last)
            {
                if (++match >= 5)
                {
                    if (retry + 1 > match)
                        Debug.WriteLine($"FileFingerprintManager.RobustTimestamp() {retry + 1} tries to get repeatable timestamp");

                    return modified;
                }
            }
            else
                match = 0;

            last = modified;

            await Task.Delay(ModifiedDelay + TimeSpan.FromTicks(Random.Shared.NextInt64(ModifiedRandomDelay.Ticks)),
                cancellationToken);
        }

        throw new IOException("Unable to get a repeatable timestamp.");
    }

    async Task TransformAnnotatedPathsToFileFingerprint(ISourceBlock<AnnotatedPath[]> annotatedPathSourceBlock,
        BroadcastBlock<FileFingerprint> fileFingerprintTargetBlock,
        CancellationToken cancellationToken)
    {
        try
        {
            var targets =
                new ConcurrentDictionary<string, TransformBlock<AnnotatedPath, FileFingerprint>>(StringComparer
                    .InvariantCultureIgnoreCase);

            var routeBlock = new ActionBlock<AnnotatedPath[]>(
                async filenames =>
                {
                    foreach (var filename in filenames)
                    {
                        if (filename is null)
                            continue;

                        var cachedBlob = GetCachedFileFingerprint(filename.FileInfo);

                        if (cachedBlob is not null)
                        {
                            await fileFingerprintTargetBlock.SendAsync(cachedBlob, cancellationToken).ConfigureAwait(false);

                            continue;
                        }

                        var host = PathUtil.GetHost(filename.FileInfo.FullName);

                        var target = targets.GetOrAdd(host, h =>
                        {
                            var ret = new TransformBlock<AnnotatedPath, FileFingerprint>(
                                annotatedPath => ProcessFileAsync(annotatedPath.FileInfo, cancellationToken),
                                new()
                                {
                                    MaxDegreeOfParallelism = 64,
                                    CancellationToken = cancellationToken
                                });

                            Debug.WriteLine($"FileFingerprintManager.GenerateBlobsAsync() starting reader for host: '{host}'");

                            ret.LinkTo(fileFingerprintTargetBlock, blob => null != blob);
                            ret.LinkTo(DataflowBlock.NullTarget<FileFingerprint>());

                            return ret;
                        });

                        //Debug.WriteLine($"FileFingerprintManager.GenerateFileFingerprintsAsync() Sending {annotatedPath} for host '{host}'");

                        await target.SendAsync(filename, cancellationToken).ConfigureAwait(false);
                    }
                },
                new()
                {
                    MaxDegreeOfParallelism = 64,
                    CancellationToken = cancellationToken
                });

            var distinctPaths = new HashSet<string>(StringComparer.Ordinal);

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
                new()
                {
                    MaxDegreeOfParallelism = 1,
                    CancellationToken = cancellationToken
                });

            distinctBlock.LinkTo(routeBlock, new() { PropagateCompletion = true });

            annotatedPathSourceBlock.LinkTo(distinctBlock, new() { PropagateCompletion = true });

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
