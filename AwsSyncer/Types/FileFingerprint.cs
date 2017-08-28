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

using System;

namespace AwsSyncer.Types
{
    public sealed class FileFingerprint : IEquatable<FileFingerprint>
    {
        public FileFingerprint(string fullFilePath, DateTime lastModifiedUtc, BlobFingerprint fingerprint, bool wasCached = false)
        {
            Fingerprint = fingerprint ?? throw new ArgumentNullException(nameof(fingerprint));
            WasCached = wasCached;
            FullFilePath = fullFilePath ?? throw new ArgumentNullException(nameof(fullFilePath));
            LastModifiedUtc = lastModifiedUtc;
        }

        public string FullFilePath { get; }
        public DateTime LastModifiedUtc { get; }
        public BlobFingerprint Fingerprint { get; }
        public bool WasCached { get; }

        public bool Equals(FileFingerprint other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;

            return string.Equals(FullFilePath, other.FullFilePath, StringComparison.OrdinalIgnoreCase)
                   && LastModifiedUtc.Equals(other.LastModifiedUtc)
                   && Fingerprint.Equals(other.Fingerprint);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;

            return obj is FileFingerprint ff && Equals(ff);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = StringComparer.OrdinalIgnoreCase.GetHashCode(FullFilePath);
                hashCode = (hashCode * 397) ^ LastModifiedUtc.GetHashCode();
                hashCode = (hashCode * 397) ^ Fingerprint.GetHashCode();
                hashCode = (hashCode * 397) ^ WasCached.GetHashCode();
                return hashCode;
            }
        }

        public static bool operator ==(FileFingerprint left, FileFingerprint right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(FileFingerprint left, FileFingerprint right)
        {
            return !Equals(left, right);
        }
    }
}
