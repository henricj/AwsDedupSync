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

using Amazon.S3;
using Amazon.S3.Model;
using AwsSyncer.Types;
using AwsSyncer.Utility;
using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace AwsSyncer.AWS
{
    public sealed class S3Links : S3PutBase
    {
        readonly IPathManager _pathManager;
        readonly S3StorageClass _s3StorageClass;

        public S3Links(IAmazonS3 amazonS3, IPathManager pathManager, S3StorageClass s3StorageClass)
            : base(amazonS3)
        {
            _pathManager = pathManager ?? throw new ArgumentNullException(nameof(pathManager));
            _s3StorageClass = s3StorageClass ?? throw new ArgumentNullException(nameof(s3StorageClass));
        }

        public async Task<IReadOnlyDictionary<string, string>> ListAsync(string name, CancellationToken cancellationToken)
        {
            var files = new Dictionary<string, string>();

            var request = new ListObjectsV2Request
            {
                BucketName = _pathManager.Bucket,
                Prefix = _pathManager.GetTreeNamePrefix(name)
            };

            for (; ; )
            {
                var response = await AmazonS3.ListObjectsV2Async(request, cancellationToken).ConfigureAwait(false);

                foreach (var s3Object in response.S3Objects)
                {
                    var key = _pathManager.GetKeyFromTreeNamePath(name, s3Object.Key);

                    files[key] = s3Object.ETag;
                }

                if (!response.IsTruncated)
                    return files;

                request.ContinuationToken = response.NextContinuationToken;
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Security", "CA5351:Do Not Use Broken Cryptographic Algorithms", Justification = "MD5 is required for external API compatibility.")]
        public ICreateLinkRequest BuildCreateLinkRequest(string collection, string relativePath, FileFingerprint fileFingerprint, string existingETag)
        {
            var link = '/' + _pathManager.GetBlobPath(fileFingerprint);

            byte[] md5Digest;

            using (var md5 = MD5.Create())
            {
                md5Digest = md5.ComputeHash(Encoding.UTF8.GetBytes(link));
            }

            var etag = S3Util.ComputeS3Etag(md5Digest);

            if (!string.IsNullOrEmpty(existingETag))
            {
                var match = string.Equals(etag, existingETag, StringComparison.OrdinalIgnoreCase);

                if (match)
                {
                    //Debug.WriteLine($"{collection} \"{relativePath}\" already exists and equals {link}");

                    return null;
                }
            }

            var md5DigestString = Convert.ToBase64String(md5Digest);

            var treeKey = _pathManager.GetTreeNamePath(collection, relativePath);

            var request = new PutObjectRequest
            {
                BucketName = _pathManager.Bucket,
                ContentBody = link,
                Key = treeKey,
                MD5Digest = md5DigestString,
                WebsiteRedirectLocation = link,
                ContentType = MimeDetector.GetMimeType(fileFingerprint.FullFilePath),
                StorageClass = _s3StorageClass,
                Headers =
                {
                    ["x-amz-meta-lastModified"] = fileFingerprint.LastModifiedUtc.ToString("O")
                }
            };

            return new CreateLinkRequest
            {
                Collection = collection,
                RelativePath = relativePath,
                BlobLink = link,
                FileFingerprint = fileFingerprint,
                Request = request,
                ETag = etag
            };
        }

        public async Task CreateLinkAsync(ICreateLinkRequest createLinkRequest, CancellationToken cancellationToken)
        {
            var request = (S3PutRequest)createLinkRequest;

            await PutAsync(request, cancellationToken);
        }

        public interface ICreateLinkRequest
        {
            string Collection { get; }
            string RelativePath { get; }
            string BlobLink { get; }
            string ETag { get; }
            FileFingerprint FileFingerprint { get; }
        }

        class CreateLinkRequest : S3PutRequest, ICreateLinkRequest
        {
            public string Collection { get; set; }
            public string RelativePath { get; set; }
            public string BlobLink { get; set; }
            public FileFingerprint FileFingerprint { get; set; }
        }
    }
}
