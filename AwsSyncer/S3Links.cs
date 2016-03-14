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
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;

namespace AwsSyncer
{
    public sealed class S3Links
    {
        readonly IAmazonS3 _amazon;
        readonly IPathManager _pathManager;

        public S3Links(IAmazonS3 amazon, IPathManager pathManager)
        {
            _amazon = amazon;
            _pathManager = pathManager;
        }

        public async Task<IReadOnlyDictionary<string, string>> ListAsync(string name, CancellationToken cancellationToken)
        {
            var files = new Dictionary<string, string>();

            var request = new ListObjectsRequest
            {
                BucketName = _pathManager.Bucket,
                Prefix = _pathManager.GetTreeNamePrefix(name)
            };

            for (;;)
            {
                var response = await _amazon.ListObjectsAsync(request, cancellationToken).ConfigureAwait(false);

                foreach (var x in response.S3Objects)
                {
                    var key = _pathManager.GetKeyFromTreeNamePath(name, x.Key);

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
            var treeKey = _pathManager.GetTreeNamePath(name, path);
            var link = '/' + _pathManager.GetBlobPath(blob);

            string md5Digest;

            using (var md5 = MD5.Create())
            {
                md5Digest = Convert.ToBase64String(md5.ComputeHash(Encoding.ASCII.GetBytes(link)));
            }

            var request = new PutObjectRequest
            {
                BucketName = _pathManager.Bucket,
                ContentBody = link,
                Key = treeKey,
                MD5Digest = md5Digest,
                WebsiteRedirectLocation = link,
                ContentType = MimeDetector.Default.GetMimeType(blob.FullFilePath)
            };

            var response = await _amazon.PutObjectAsync(request, cancellationToken).ConfigureAwait(false);

            if (response.HttpStatusCode != HttpStatusCode.OK)
                Debug.WriteLine("now what?");
        }
    }
}
