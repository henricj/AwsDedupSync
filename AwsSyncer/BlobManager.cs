using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.AccessControl;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using DBreeze;

namespace AwsSyncer
{
    public sealed class BlobManager : IDisposable
    {
        readonly ConcurrentDictionary<string, object> _allPaths = new ConcurrentDictionary<string, object>();
        readonly Task _cacheManager;
        readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
        readonly StreamFingerprinter _fingerprinter;
        readonly TaskCompletionSource<object> _managerDone = new TaskCompletionSource<object>();
        readonly Random _random = CreateRandom();
        readonly ConcurrentQueue<IBlob> _updateKnownBlobs = new ConcurrentQueue<IBlob>();

        Dictionary<string, IBlob> _knownBlobs;

        public BlobManager(StreamFingerprinter fingerprinter)
        {
            if (null == fingerprinter)
                throw new ArgumentNullException("fingerprinter");

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

                Trace.WriteLine(string.Format("BlobManager writing {0} items to db", blobs.Length));

                try
                {
                    using (var tran = dbe.GetTransaction())
                    {
                        foreach (var blob in blobs)
                        {
                            ms.SetLength(0);

                            WriteBlob(writer, blob);

                            var buffer = ms.ToArray();

                            tran.Insert("Path", blob.FullPath, buffer);
                        }

                        tran.Commit();

                        blobs = null;
                    }
                }
                finally
                {
                    if (null != blobs)
                    {
                        Trace.WriteLine(string.Format("BlobManager returning {0} items to queue", blobs.Length));

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

            return list.OrderBy(b => b.FullPath).ToArray();
        }

        static DBreezeEngine CreateEngine()
        {
            var localApplicationData = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData, Environment.SpecialFolderOption.Create);

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

        public void Load(string[] paths, ITargetBlock<IBlob> blobTargetBlock)
        {
            _knownBlobs = LoadBlobCache();

            if (_cacheManager.Status == TaskStatus.Created)
                _cacheManager.Start();

            GenerateBlobs(paths, blobTargetBlock);
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

        IEnumerable<string> ScanDirectory(string arg)
        {
            var attr = File.GetAttributes(arg);

            if (FileAttributes.Directory == (attr & FileAttributes.Directory))
            {
                foreach (var path in Directory.EnumerateFiles(arg, "*", SearchOption.AllDirectories))
                {
                    if (string.IsNullOrWhiteSpace(path))
                        continue;

                    if (!_allPaths.TryAdd(path, string.Empty))
                        continue;

                    yield return path;
                }
            }
            else if (0 == (attr & (FileAttributes.Offline | FileAttributes.ReparsePoint)))
            {
                var fileInfo = new FileInfo(arg);

                var path = fileInfo.FullName;

                if (!string.IsNullOrWhiteSpace(path) && _allPaths.TryAdd(path, string.Empty))
                    yield return path;
            }
        }

        static string GetHost(string path)
        {
            var uri = new Uri(path, UriKind.RelativeOrAbsolute);

            return uri.IsAbsoluteUri ? uri.Host : string.Empty;
        }

        /// <summary>
        ///     Fisher–Yates shuffle
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="list"></param>
        void Shuffle<T>(IList<T> list)
        {
            lock (_random)
            {
                for (var i = list.Count - 1; i >= 1; --i)
                {
                    var j = _random.Next(i + 1);

                    var tmp = list[i];
                    list[i] = list[j];
                    list[j] = tmp;
                }
            }
        }

        static Random CreateRandom()
        {
            using (var rng = RandomNumberGenerator.Create())
            {
                var buffer = new byte[4];

                rng.GetBytes(buffer);

                return new Random(BitConverter.ToInt32(buffer, 0));
            }
        }

        void GenerateBlobs(string[] args, ITargetBlock<IBlob> blobTargetBlock)
        {
            try
            {
                var targets = new ConcurrentDictionary<string, TransformBlock<string, IBlob>>(StringComparer.InvariantCultureIgnoreCase);

                var routeBlock = new ActionBlock<string[]>(
                    async filenames =>
                    {
                        Shuffle(filenames);

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

                            await target.SendAsync(filename).ConfigureAwait(false);
                        }
                    });

                var batcher = new BatchBlock<string>(1024);

                batcher.LinkTo(routeBlock, new DataflowLinkOptions
                                           {
                                               PropagateCompletion = true
                                           });

                routeBlock.Completion.ContinueWith(t =>
                                                   {
                                                       Task.WhenAll(targets.Values.Select(target => target.Completion))
                                                           .ContinueWith(_ => blobTargetBlock.Complete());

                                                       foreach (var target in targets.Values)
                                                           target.Complete();
                                                   });

                try
                {
                    var partitions = args.GroupBy(GetHost).ToArray();

                    partitions.AsParallel()
                              .WithDegreeOfParallelism(partitions.Length)
                              .WithExecutionMode(ParallelExecutionMode.ForceParallelism)
                              .SelectMany(p => p)
                              .SelectMany(ScanDirectory)
                              .Distinct(StringComparer.InvariantCultureIgnoreCase)
                              .ForAll(f => batcher.Post(f));
                }
                finally
                {
                    routeBlock.Complete();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("DistinctFiles failed: " + ex.Message);
            }
        }

        async Task<IBlob> ProcessFileAsync(string filename)
        {
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

                using (var s = new FileStream(filename, FileMode.Open, FileSystemRights.Read, FileShare.Read, 8192, FileOptions.Asynchronous | FileOptions.SequentialScan))
                {
                    fingerprint = await fp.GetFingerprintAsync(s).ConfigureAwait(false);
                }

                if (fingerprint.Size != fi.Length)
                    return null;

                var blob = new Blob(fi.FullName, fi.LastWriteTimeUtc, fingerprint);

                UpdateKnownBlobs(blob);

                return blob;
            }
            catch (Exception ex)
            {
                Debug.WriteLine("File {0} failed: {1}", filename, ex.Message);
                return null;
            }
        }

        void UpdateKnownBlobs(IBlob blob)
        {
            _updateKnownBlobs.Enqueue(blob);
        }
    }
}
