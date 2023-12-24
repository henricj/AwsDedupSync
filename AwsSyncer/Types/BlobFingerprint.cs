// Copyright (c) 2014-2017 Henric Jungheim <software@henric.org>
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
using System.Linq;
using System.Runtime.Serialization;
using AwsSyncer.AWS;

namespace AwsSyncer.Types;

[DataContract]
public sealed class BlobFingerprint : IEquatable<BlobFingerprint>
{
    [DataMember(Order = 0)] public long Size { get; }

    [DataMember(Order = 1)] public byte[] Sha3_512 { get; }

    [DataMember(Order = 2)] public byte[] Sha2_256 { get; }

    [DataMember(Order = 3)] public byte[] Md5 { get; }

    public BlobFingerprint(long size, byte[] sha3_512, byte[] sha2_256, byte[] md5)
    {
        if (size < 0)
            throw new ArgumentOutOfRangeException(nameof(size), size, "size cannot be negative");

        if (null == sha3_512)
            throw new ArgumentNullException(nameof(sha3_512));

        if (sha3_512.Length != 512 / 8)
            throw new ArgumentException("invalid SHA3-512 length", nameof(sha3_512));

        if (null == sha2_256)
            throw new ArgumentNullException(nameof(sha2_256));

        if (sha2_256.Length != 256 / 8)
            throw new ArgumentException("invalid SHA2-256 length", nameof(sha2_256));

        if (null == md5)
            throw new ArgumentNullException(nameof(md5));

        if (md5.Length != 128 / 8)
            throw new ArgumentException("invalid MD5 length", nameof(md5));

        Size = size;
        Sha3_512 = sha3_512;
        Sha2_256 = sha2_256;
        Md5 = md5;
    }

    public bool Equals(BlobFingerprint other)
    {
        if (ReferenceEquals(this, other))
            return true;

        if (other is null)
            return false;

        return Size == other.Size
            && Sha3_512.SequenceEqual(other.Sha3_512)
            && Sha2_256.SequenceEqual(other.Sha2_256)
            && Md5.SequenceEqual(other.Md5);
    }

    public static bool operator ==(BlobFingerprint left, BlobFingerprint right) => Equals(left, right);

    public static bool operator !=(BlobFingerprint left, BlobFingerprint right) => !Equals(left, right);

    #region Object

    public override bool Equals(object obj) => Equals(obj as BlobFingerprint);

    public override int GetHashCode() => BitConverter.ToInt32(Sha3_512, 0);

    #endregion // Object
}

public static class BlobFingerprintExtensions
{
    public static string Key(this BlobFingerprint fingerprint) => S3Util.S3EncodeKey(fingerprint.Sha3_512);

    public static string S3ETag(this BlobFingerprint fingerprint) => S3Util.ComputeS3Etag(fingerprint.Md5);
}
