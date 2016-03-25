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
using System.Net;
using System.Security.AccessControl;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;

namespace AwsSyncer
{
    public sealed class S3Blobs
    {
        readonly IAmazonS3 _amazon;
        readonly IPathManager _pathManager;

        public S3Blobs(IAmazonS3 amazon, IPathManager pathManager)
        {
            if (null == amazon)
                throw new ArgumentNullException(nameof(amazon));
            if (null == pathManager)
                throw new ArgumentNullException(nameof(pathManager));

            _amazon = amazon;
            _pathManager = pathManager;
        }

        public async Task<IReadOnlyDictionary<string, long>> ListAsync(CancellationToken cancellationToken)
        {
            var keys = new Dictionary<string, long>();

            var request = new ListObjectsRequest
            {
                BucketName = _pathManager.Bucket,
                Prefix = _pathManager.BlobPrefix,
                Delimiter = "/"
            };

            for (;;)
            {
                var response = await _amazon.ListObjectsAsync(request, cancellationToken).ConfigureAwait(false);

                foreach (var x in response.S3Objects)
                {
                    var key = _pathManager.GetKeyFromBlobPath(x.Key);

                    // Ignore the folder itself (if there is an empty object there).
                    if (null == key)
                        continue;

                    keys[key] = x.Size;
                }

                if (!response.IsTruncated)
                    return keys;

                request.Marker = response.NextMarker;
            }
        }

        public async Task<string> StoreAsync(IBlob blob, CancellationToken cancellationToken)
        {
            Debug.WriteLine("S3Blobs.StoreAsync " + blob.FullFilePath);

            using (var s = new FileStream(blob.FullFilePath, FileMode.Open, FileSystemRights.Read, FileShare.Read, 8192,
                FileOptions.Asynchronous | FileOptions.SequentialScan))
            {
                var fi = new FileInfo(blob.FullFilePath);

                var fingerprint = blob.Fingerprint;

                if (fi.Length != fingerprint.Size || fi.LastWriteTimeUtc != blob.LastModifiedUtc)
                    return null;

                var md5Digest = Convert.ToBase64String(fingerprint.Md5);

                var request = new PutObjectRequest
                {
                    BucketName = _pathManager.Bucket,
                    InputStream = s,
                    Key = _pathManager.GetBlobPath(blob),
                    MD5Digest = md5Digest,
                    Headers =
                    {
                        ContentType = MimeDetector.Default.GetMimeType(blob.FullFilePath),
                        ContentLength = fingerprint.Size,
                        ContentMD5 = md5Digest
                    },
                    AutoCloseStream = false,
                    AutoResetStreamPosition = false
                };

                request.Headers["x-amz-meta-SHA2-256"] = Convert.ToBase64String(fingerprint.Sha2_256);
                request.Headers["x-amz-meta-SHA3-512"] = Convert.ToBase64String(fingerprint.Sha3_512);

                var fileName = Path.GetFileName(blob.FullFilePath);

                if (null != fileName)
                {
                    fileName = Encoding.ASCII.GetString(Encoding.ASCII.GetBytes(fileName));

                    request.Headers["x-amz-meta-original-name"] = fileName;

                    request.Headers.ContentDisposition = "attachment; filename=" + fileName;
                }

                var response = await _amazon.PutObjectAsync(request, cancellationToken).ConfigureAwait(false);

                if (response.HttpStatusCode != HttpStatusCode.OK)
                    Debug.WriteLine("now what?");

                return request.Key;
            }
        }
    }
}
