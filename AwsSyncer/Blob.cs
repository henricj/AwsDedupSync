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
using System.Web;

namespace AwsSyncer
{
    public interface IBlob : IFileFingerprint, IEquatable<IBlob>
    {
        string Collection { get; }
        string RelativePath { get; }
        string Key { get; }
    }

    public class Blob : IBlob
    {
        public Blob(string fullFilePath, DateTime lastModifiedUtc, BlobFingerprint fingerprint, string collection, string relativePath, bool wasCached = false)
        {
            if (string.IsNullOrWhiteSpace(fullFilePath))
                throw new ArgumentNullException(nameof(fullFilePath));
            if (null == fingerprint)
                throw new ArgumentNullException(nameof(fingerprint));
            if (string.IsNullOrWhiteSpace(fullFilePath))
                throw new ArgumentOutOfRangeException(nameof(fullFilePath));
            if (lastModifiedUtc.Kind != DateTimeKind.Utc)
                throw new ArgumentException("time must be UTC", nameof(lastModifiedUtc));
            if (string.IsNullOrEmpty(collection))
                throw new ArgumentException("Argument is null or empty", nameof(collection));
            if (string.IsNullOrEmpty(relativePath))
                throw new ArgumentException("Argument is null or empty", nameof(relativePath));

            FullFilePath = fullFilePath;
            Fingerprint = fingerprint;
            Collection = collection;
            RelativePath = relativePath;
            WasCached = wasCached;
            LastModifiedUtc = lastModifiedUtc;
            Key = fingerprint.Key();
        }

        #region IBlob Members

        public string FullFilePath { get; }
        public string Collection { get; }
        public string RelativePath { get; }
        public bool WasCached { get; }
        public DateTime LastModifiedUtc { get; }
        public BlobFingerprint Fingerprint { get; }
        public string Key { get; }

        public bool Equals(IBlob other)
        {
            if (ReferenceEquals(this, other))
                return true;

            if (ReferenceEquals(null, other))
                return false;

            return FullFilePath == other.FullFilePath
                   && Fingerprint.Equals(other.Fingerprint);
        }

        #endregion

        #region Object

        public override bool Equals(object obj)
        {
            return Equals(obj as IBlob);
        }

        public override int GetHashCode()
        {
            return FullFilePath.GetHashCode() ^ Fingerprint.GetHashCode();
        }

        #endregion
    }
}
