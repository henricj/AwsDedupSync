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

namespace AwsSyncer
{
    public interface IPathManager
    {
        string Bucket { get; }
        Uri AwsS3Url { get; }
        string BlobPrefix { get; }
        string TreePrefix { get; }

        string GetBlobPath(IFileFingerprint fileFingerprint);
        string GetKeyFromBlobPath(string blobPath);

        Uri GetBlobUrl(IBlob blob);

        string GetTreeNamePrefix(string name);
        string GetTreeNamePath(string name, string key);
        string GetKeyFromTreeNamePath(string name, string treeNamePath);
    }

    public class PathManager : IPathManager
    {
        string _blobPrefix = "b/";
        readonly Uri _bucketUrl;
        string _treePrefix = "t/";

        public PathManager(Uri awsS3Url, string bucket)
        {
            if (awsS3Url == null)
                throw new ArgumentNullException(nameof(awsS3Url));
            if (!awsS3Url.IsAbsoluteUri)
                throw new ArgumentException("the url must be absolute", nameof(awsS3Url));
            if (string.IsNullOrWhiteSpace(bucket))
                throw new ArgumentException("Argument is null or whitespace", nameof(bucket));

            AwsS3Url = awsS3Url;
            Bucket = bucket;

            var builder = new UriBuilder(awsS3Url);

            builder.Host = bucket + "." + builder.Host;

            _bucketUrl = builder.Uri;
        }

        public string BlobPrefix
        {
            get { return _blobPrefix; }
            set
            {
                if (string.IsNullOrWhiteSpace(value))
                    throw new ArgumentNullException(nameof(value));

                _blobPrefix = value;
            }
        }

        public string TreePrefix
        {
            get { return _treePrefix; }

            set
            {
                if (string.IsNullOrWhiteSpace(value))
                    throw new ArgumentNullException(nameof(value));

                var normalized = PathUtil.NormalizeAsciiName(value);

                if (!string.Equals(normalized, value, StringComparison.Ordinal))
                    throw new ArgumentException("non-normalized string");

                _treePrefix = value;
            }
        }

        public string Bucket { get; }
        public Uri AwsS3Url { get; }

        public string GetBlobPath(IFileFingerprint fileFingerprint)
        {
            return BlobPrefix + fileFingerprint.Fingerprint.Key();
        }

        public string GetKeyFromBlobPath(string blobPath)
        {
            if (string.IsNullOrEmpty(blobPath))
                throw new ArgumentNullException(nameof(blobPath));
            if (!blobPath.StartsWith(BlobPrefix))
                throw new ArgumentException("path must start with " + BlobPrefix, nameof(blobPath));

            var key = blobPath.Substring(BlobPrefix.Length);

            if (string.IsNullOrEmpty(key))
                return null;

            PathUtil.RequireNormalizedAsciiName(key);

            return key;
        }

        public Uri GetBlobUrl(IBlob blob)
        {
            return new Uri(_bucketUrl, '/' + GetBlobPath(blob));
        }

        public string GetTreeNamePrefix(string name)
        {
            PathUtil.RequireNormalizedAsciiName(name);

            return TreePrefix + name + '/';
        }

        public string GetTreeNamePath(string name, string key)
        {
            return GetTreeNamePrefix(name) + key;
        }

        public string GetKeyFromTreeNamePath(string name, string treeNamePath)
        {
            if (string.IsNullOrEmpty(name))
                throw new ArgumentNullException(nameof(name));
            if (string.IsNullOrEmpty(treeNamePath))
                throw new ArgumentNullException(nameof(treeNamePath));

            var prefix = GetTreeNamePrefix(name);

            if (!treeNamePath.StartsWith(prefix))
                throw new ArgumentException("path must start with " + prefix);

            var key = treeNamePath.Substring(prefix.Length);

            return key;
        }
    }
}
