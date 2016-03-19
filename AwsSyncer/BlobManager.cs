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
using DBreeze;

namespace AwsSyncer
{
    public sealed class BlobManager : IDisposable
    {
        readonly Task _cacheManager;
        readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
        readonly StreamFingerprinter _fingerprinter;
        readonly TaskCompletionSource<object> _managerDone = new TaskCompletionSource<object>();
        readonly ConcurrentQueue<IBlob> _updateKnownBlobs = new ConcurrentQueue<IBlob>();

        Dictionary<string, IBlob> _knownBlobs;

        public BlobManager(StreamFingerprinter fingerprinter)
        {
            if (null == fingerprinter)
                throw new ArgumentNullException(nameof(fingerprinter));

            _fingerprinter = fingerprinter;
            _cacheManager = new Task(ManageCache);
        }

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
            using (var ms = new MemoryStream())
            using (var writer = new BinaryWriter(ms))
            using (var dbe = CreateEngine())
            {
                var blobs = GetBlobs();

                Trace.WriteLine($"BlobManager writing {blobs.Length} items to db");

                try
                {
                    using (var tran = dbe.GetTransaction())
                    {
                        foreach (var blob in blobs)
                        {
                            ms.SetLength(0);

                            WriteBlob(writer, blob);

                            var buffer = ms.ToArray();

                            tran.Insert("Path", blob.FullFilePath, buffer);
                        }

                        tran.Commit();

                        blobs = null;
                    }
                }
                finally
                {
                    if (null != blobs)
                    {
                        Trace.WriteLine($"BlobManager returning {blobs.Length} items to queue");

                        foreach (var blob in blobs)
                            _updateKnownBlobs.Enqueue(blob);
                    }
                }
            }
        }

        IBlob[] GetBlobs()
        {
            var list = new List<IBlob>(_updateKnownBlobs.Count);

            IBlob blob;
            while (_updateKnownBlobs.TryDequeue(out blob))
                list.Add(blob);

            return list.OrderBy(b => b.FullFilePath).ToArray();
        }

        static DBreezeEngine CreateEngine()
        {
            var localApplicationData = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData,
                Environment.SpecialFolderOption.Create);

            var path = Path.Combine(localApplicationData, "AwsSyncer", "PathsDBreeze");

            var di = new DirectoryInfo(path);

            if (!di.Exists)
                di.Create();

            return new DBreezeEngine(di.FullName);
        }

        static void WriteBlob(BinaryWriter writer, IBlob blob)
        {
            writer.Write(blob.LastModifiedUtc.Ticks);
            writer.Write(blob.Fingerprint.Size);
            writer.Write(blob.Fingerprint.Sha3_512);
            writer.Write(blob.Fingerprint.Sha2_256);
            writer.Write(blob.Fingerprint.Md5);
        }

        static IBlob ReadBlob(BinaryReader reader, string path)
        {
            var utcTicks = reader.ReadInt64();
            var size = reader.ReadInt64();
            var sha3 = reader.ReadBytes(512 / 8);
            var sha2 = reader.ReadBytes(256 / 8);
            var md5 = reader.ReadBytes(128 / 8);

            return new Blob(path, new DateTime(utcTicks, DateTimeKind.Utc), new BlobFingerprint(size, sha3, sha2, md5));
        }

        public async Task LoadAsync(string[] paths, ITargetBlock<IBlob> blobTargetBlock)
        {
            // WARNING: async over sync
            _knownBlobs = await Task.Run(() => LoadBlobCache()).ConfigureAwait(false);

            Debug.WriteLine($"Loaded {_knownBlobs.Count} known blobs");

            if (_cacheManager.Status == TaskStatus.Created)
                _cacheManager.Start();

            await GenerateBlobsAsync(paths, blobTargetBlock).ConfigureAwait(false);
        }

        static Dictionary<string, IBlob> LoadBlobCache()
        {
            using (var ms = new MemoryStream())
            using (var br = new BinaryReader(ms))
            using (var dbe = CreateEngine())
            using (var tran = dbe.GetTransaction())
            {
                var blobs = new Dictionary<string, IBlob>((int)tran.Count("Path"));

                foreach (var row in tran.SelectForward<string, byte[]>("Path"))
                {
                    var buffer = row.Value;

                    if (ms.Capacity < buffer.Length)
                        ms.Capacity = buffer.Length;

                    ms.Position = 0;
                    ms.SetLength(buffer.Length);

                    Array.Copy(buffer, ms.GetBuffer(), buffer.Length);

                    var blob = ReadBlob(br, row.Key);

                    blobs[row.Key] = blob;
                }

                return blobs;
            }
        }

        static string GetHost(string path)
        {
            var uri = new Uri(path, UriKind.RelativeOrAbsolute);

            return uri.IsAbsoluteUri ? uri.Host : string.Empty;
        }

        async Task GenerateBlobsAsync(string[] paths, ITargetBlock<IBlob> blobTargetBlock)
        {
            try
            {
                var targets = new ConcurrentDictionary<string, TransformBlock<string, IBlob>>(StringComparer.InvariantCultureIgnoreCase);

                var routeBlock = new ActionBlock<string[]>(
                    async filenames =>
                    {
                        RandomUtil.Shuffle(filenames);

                        foreach (var filename in filenames)
                        {
                            var host = GetHost(filename);

                            TransformBlock<string, IBlob> target;
                            while (!targets.TryGetValue(host, out target))
                            {
                                target = new TransformBlock<string, IBlob>((Func<string, Task<IBlob>>)ProcessFileAsync,
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

                var batcher = new BatchBlock<string>(1024);

                batcher.LinkTo(routeBlock, new DataflowLinkOptions
                {
                    PropagateCompletion = true
                });

                var completeTask = routeBlock.Completion.ContinueWith(
                    _ =>
                    {
                        Task.WhenAll(targets.Values.Select(target => target.Completion))
                            .ContinueWith(_ => blobTargetBlock.Complete());

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

        static Task PostAllFilePathsAsync(IEnumerable<string> paths, ITargetBlock<string> filePathTargetBlock)
        {
            var partitions = paths
                .GroupBy(GetHost, StringComparer.InvariantCultureIgnoreCase)
                .ToArray();

            var scanTasks = partitions
                .Select(partitionPaths =>
                    Task.Factory.StartNew(async () =>
                    {
                        foreach (var file in partitionPaths.SelectMany(PathUtil.ScanDirectory))
                        {
                            await filePathTargetBlock.SendAsync(file).ConfigureAwait(false);
                        }
                    },
                        CancellationToken.None,
                        TaskCreationOptions.DenyChildAttach | TaskCreationOptions.LongRunning | TaskCreationOptions.RunContinuationsAsynchronously,
                        TaskScheduler.Default));

            return Task.WhenAll(scanTasks);
        }

        async Task<IBlob> ProcessFileAsync(string filename)
        {
            //Debug.WriteLine($"BlobManager.ProcessFileAsync({filename})");

            var fp = _fingerprinter;

            try
            {
                var fi = new FileInfo(filename);

                if (!fi.Exists)
                    return null;

                IBlob knownBlob;
                if (_knownBlobs.TryGetValue(fi.FullName, out knownBlob))
                {
                    if (knownBlob.Fingerprint.Size == fi.Length && knownBlob.LastModifiedUtc == fi.LastWriteTimeUtc)
                        return knownBlob;
                }

                IBlobFingerprint fingerprint;

                using (var s = new FileStream(filename, FileMode.Open, FileSystemRights.Read, FileShare.Read,
                    8192, FileOptions.Asynchronous | FileOptions.SequentialScan))
                {
                    fingerprint = await fp.GetFingerprintAsync(s).ConfigureAwait(false);
                }

                if (fingerprint.Size != fi.Length)
                    return null;

                var blob = new Blob(fi.FullName, fi.LastWriteTimeUtc, fingerprint);

                _updateKnownBlobs.Enqueue(blob);

                return blob;
            }
            catch (Exception ex)
            {
                Debug.WriteLine("File {0} failed: {1}", filename, ex.Message);
                return null;
            }
        }
    }
}
