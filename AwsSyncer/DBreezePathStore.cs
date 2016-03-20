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
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using DBreeze;
using Newtonsoft.Json;
using Newtonsoft.Json.Bson;

namespace AwsSyncer
{
    public static class DBreezePathStore
    {
        static readonly string PathTableName = "Path";
        static readonly string DbPath = GetDbPath();

        static DBreezeEngine CreateEngine()
        {
            var di = new DirectoryInfo(DbPath);

            if (!di.Exists)
                di.Create();

            return new DBreezeEngine(di.FullName);
        }

        static string GetDbPath()
        {
            var localApplicationData = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData,
                Environment.SpecialFolderOption.Create);

            var path = Path.Combine(localApplicationData, "AwsSyncer", "PathsDBreeze");

            return path;
        }

        public static async Task<Dictionary<string, IBlob>> LoadBlobsAsync()
        {
            try
            {
                var ret = await LoadBlobCacheRetryAsync().ConfigureAwait(false);

                if (null != ret)
                    return ret;
            }
            catch (JsonReaderException)
            {
                // The schema has changed?
            }

            try
            {
                using (var dbe = CreateEngine())
                {
                    dbe.Scheme.DeleteTable(PathTableName);
                }
            }
            catch (IOException ex)
            {
                Console.WriteLine("LoadBlobsAsync() delete failed: " + ex.Message);
            }

            return new Dictionary<string, IBlob>();
        }

        static async Task<Dictionary<string, IBlob>> LoadBlobCacheRetryAsync()
        {
            var delay = 3.0;

            for (var retry = 0; retry < 3; ++retry)
            {
                try
                {
                    return LoadBlobsImpl();
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Cache read failed: " + ex.Message);
                }

                var rnd = 0.5 * delay * RandomUtil.ThreadLocalRandom.NextDouble();

                await Task.Delay(TimeSpan.FromSeconds(delay + rnd)).ConfigureAwait(false);

                delay *= 2;
            }

            return null;
        }

        static Dictionary<string, IBlob> LoadBlobsImpl()
        {
            using (var ms = new MemoryStream())
            using (var br = new BsonReader(ms))
            using (var dbe = CreateEngine())
            using (var tran = dbe.GetTransaction())
            {
                var serializer = new JsonSerializer();

                var blobs = new Dictionary<string, IBlob>((int)tran.Count(PathTableName));

                foreach (var row in tran.SelectForward<string, byte[]>(PathTableName))
                {
                    if (!row.Exists)
                        continue;

                    var buffer = row.Value;

                    if (ms.Capacity < buffer.Length)
                        ms.Capacity = buffer.Length;

                    ms.Position = 0;
                    ms.SetLength(buffer.Length);

                    Array.Copy(buffer, ms.GetBuffer(), buffer.Length);

                    try
                    {
                        var blob = serializer.Deserialize<Blob>(br);

                        blobs[row.Key] = blob;
                    }
                    catch (IOException ex)
                    {
                        // The entry might or might not be valid.
                        Debug.WriteLine("BlobManager.LoadBlobsImpl() read failed: " + ex.Message);
                    }
                }

                return blobs;
            }
        }

        public static void StoreBlobs(ICollection<IBlob> blobs)
        {
            using (var ms = new MemoryStream())
            using (var writer = new BsonWriter(ms))
            using (var dbe = CreateEngine())
            {
                var serializer = new JsonSerializer();

                using (var tran = dbe.GetTransaction())
                {
                    foreach (var blob in blobs)
                    {
                        ms.SetLength(0);

                        serializer.Serialize(writer, blob);

                        var buffer = ms.ToArray();

                        tran.Insert(PathTableName, blob.FullFilePath, buffer);
                    }

                    tran.Commit();
                }
            }
        }
    }
}
