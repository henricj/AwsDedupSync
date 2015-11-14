// Copyright (c) 2014 Henric Jungheim <software@henric.org>
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
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.AccessControl;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using Amazon.S3;
using Amazon.S3.Model;

namespace AwsSyncer
{
    public sealed class S3Manager : IDisposable
    {
        const double ToGiB = 1.0 / (1024 * 1024 * 1024);
        readonly IAmazonS3 _amazon;
        readonly string _bucket;

        public S3Manager(string bucket)
        {
            if (bucket == null)
                throw new ArgumentNullException(nameof(bucket));

            _bucket = bucket;
            _amazon = new AmazonS3Client();
        }

        public IReadOnlyDictionary<string, long> Keys { get; private set; }

        #region IDisposable Members

        public void Dispose()
        {
            _amazon.Dispose();
        }

        #endregion

        static double BytesToGiB(long value)
        {
            return value * ToGiB;
        }

        public async Task ScanAsync(CancellationToken cancellationToken)
        {
            var keys = new Dictionary<string, long>();

            var request = new ListObjectsRequest
            {
                BucketName = _bucket,
                Prefix = "b/",
                Delimiter = "/"
            };

            for (;;)
            {
                var response = await _amazon.ListObjectsAsync(request, cancellationToken).ConfigureAwait(false);

                foreach (var x in response.S3Objects)
                {
                    var key = x.Key.Substring(request.Prefix.Length);

                    keys[key] = x.Size;
                }

                if (!response.IsTruncated)
                {
                    Keys = keys;

                    Trace.WriteLine($"Bucket b/ contains {Keys.Count} items {BytesToGiB(Keys.Values.Sum()):F2}GiB");

                    return;
                }

                request.Marker = response.NextMarker;
            }
        }

        public async Task<string> StoreAsync(IBlob blob, CancellationToken cancellationToken)
        {
            Debug.WriteLine("S3Manager.StoreaAsync " + blob.FullPath);

            using (var s = new FileStream(blob.FullPath, FileMode.Open, FileSystemRights.Read, FileShare.Read, 8192,
                FileOptions.Asynchronous | FileOptions.SequentialScan))
            {
                var fi = new FileInfo(blob.FullPath);

                var fingerprint = blob.Fingerprint;

                if (fi.Length != fingerprint.Size || fi.LastWriteTimeUtc != blob.LastModifiedUtc)
                    return null;

                var md5Digest = Convert.ToBase64String(fingerprint.Md5);

                var request = new PutObjectRequest
                {
                    BucketName = _bucket,
                    InputStream = s,
                    Key = "b/" + HttpServerUtility.UrlTokenEncode(fingerprint.Sha3_512),
                    MD5Digest = md5Digest,
                    Headers =
                    {
                        ContentType = MimeDetector.Default.GetMimeType(blob.FullPath),
                        ContentLength = fingerprint.Size,
                        ContentMD5 = md5Digest
                    },
                    AutoCloseStream = false,
                    AutoResetStreamPosition = false
                };

                request.Headers["x-amz-meta-SHA2-256"] = Convert.ToBase64String(fingerprint.Sha2_256);
                request.Headers["x-amz-meta-SHA3-512"] = Convert.ToBase64String(fingerprint.Sha3_512);

                var fileName = Path.GetFileName(blob.FullPath);

                if (null != fileName)
                {
                    fileName = Encoding.ASCII.GetString(Encoding.ASCII.GetBytes(fileName));

                    request.Headers["x-amz-meta-original-name"] = fileName;

                    request.Headers.ContentDisposition = "attachment; filename=" + fileName;
                }

                var response = await _amazon.PutObjectAsync(request, cancellationToken).ConfigureAwait(false);

                return request.Key;
            }
        }

        public async Task<IReadOnlyDictionary<string, string>> ListTreeAsync(string name, CancellationToken cancellationToken)
        {
            var files = new Dictionary<string, string>();

            var request = new ListObjectsRequest
            {
                BucketName = _bucket,
                Prefix = "t/" + name + "/"
            };

            for (;;)
            {
                var response = await _amazon.ListObjectsAsync(request, cancellationToken).ConfigureAwait(false);

                foreach (var x in response.S3Objects)
                {
                    var key = x.Key.Substring(request.Prefix.Length);

                    files[key] = "TODO";
                }

                if (!response.IsTruncated)
                {
                    Trace.WriteLine($"Links {files.Count} in tree {name}");

                    return new ReadOnlyDictionary<string, string>(files);
                }

                request.Marker = response.NextMarker;
            }
        }

        public async Task CreateLinkAsync(string name, string path, IBlob blob, CancellationToken cancellationToken)
        {
            var treeKey = "t/" + name + "/";
            var link = "/b/" + HttpServerUtility.UrlTokenEncode(blob.Fingerprint.Sha3_512);

            string md5Digest;

            using (var md5 = MD5.Create())
            {
                md5Digest = Convert.ToBase64String(md5.ComputeHash(Encoding.ASCII.GetBytes(link)));
            }

            var request = new PutObjectRequest
            {
                BucketName = _bucket,
                ContentBody = link,
                Key = treeKey + path,
                MD5Digest = md5Digest,
                WebsiteRedirectLocation = link,
                ContentType = MimeDetector.Default.GetMimeType(blob.FullPath)
            };

            var response = await _amazon.PutObjectAsync(request, cancellationToken).ConfigureAwait(false);
        }
    }
}
