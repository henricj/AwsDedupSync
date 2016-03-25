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
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.S3;

namespace AwsSyncer
{
    public sealed class AwsManager : IDisposable
    {
        readonly IAmazonS3 _amazonS3;
        readonly IPathManager _pathManager;
        readonly S3Blobs _s3Blobs;
        readonly S3Links _s3Links;

        public AwsManager(IAmazonS3 amazonS3, IPathManager pathManager)
        {
            if (null == amazonS3)
                throw new ArgumentNullException(nameof(amazonS3));
            if (null == pathManager)
                throw new ArgumentNullException(nameof(pathManager));

            _pathManager = pathManager;
            _amazonS3 = amazonS3;

            var storageClass = S3StorageClass.Standard;
            var storageClassString = ConfigurationManager.AppSettings["AwsStorageClass"];

            if (!string.IsNullOrWhiteSpace(storageClassString))
                storageClass = S3StorageClass.FindValue(storageClassString);

            var blobStorageClass = storageClass;
            var blobStorageClassString = ConfigurationManager.AppSettings["AwsBlobStorageClass"];

            if (!string.IsNullOrWhiteSpace(blobStorageClassString))
                blobStorageClass = S3StorageClass.FindValue(blobStorageClassString);

            var linkStorageClass = storageClass;
            var linkStorageClassString = ConfigurationManager.AppSettings["AwsLinkStorageClass"];

            if (!string.IsNullOrWhiteSpace(linkStorageClassString))
                linkStorageClass = S3StorageClass.FindValue(linkStorageClassString);

            _s3Blobs = new S3Blobs(amazonS3, pathManager, blobStorageClass);
            _s3Links = new S3Links(amazonS3, pathManager, linkStorageClass);
        }

        #region IDisposable Members

        public void Dispose()
        {
            _amazonS3.Dispose();
        }

        #endregion

        public async Task<IReadOnlyDictionary<string, long>> ScanAsync(CancellationToken cancellationToken)
        {
            var s3Blobs = await _s3Blobs.ListAsync(cancellationToken).ConfigureAwait(false);

            Trace.WriteLine($"Bucket {_pathManager.Bucket} contains {s3Blobs.Count} items {SizeConversion.BytesToGiB(s3Blobs.Values.Sum()):F2}GiB");

            return s3Blobs;
        }

        public Task<string> StoreAsync(IBlob blob, CancellationToken cancellationToken)
        {
            return _s3Blobs.StoreAsync(blob, cancellationToken);
        }

        public Task<ICollection<string>> GetLinksAsync(string name, CancellationToken cancellationToken)
        {
            return _s3Links.ListAsync(name, cancellationToken);
        }

        public Task CreateLinkAsync(string name, string path, IBlob blob, CancellationToken cancellationToken)
        {
            return _s3Links.CreateLinkAsync(name, path, blob, cancellationToken);
        }

        public Task UpdateBlobPaths(BlobFingerprint fingerprint, IReadOnlyCollection<IBlob> blobs, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}
