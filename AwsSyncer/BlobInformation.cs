using System;
using System.Linq;

namespace AwsSyncer
{
    public interface IBlobFingerprint : IEquatable<IBlobFingerprint>
    {
        long Size { get; }
        byte[] Sha3_512 { get; }
        byte[] Sha2_256 { get; }
        byte[] Md5 { get; }
    }

    class BlobFingerprint : IBlobFingerprint
    {
        readonly byte[] _md5;
        readonly byte[] _sha2_256;
        readonly byte[] _sha3_512;
        readonly long _size;

        public BlobFingerprint(long size, byte[] sha3_512, byte[] sha2_256, byte[] md5)
        {
            if (size < 0)
                throw new ArgumentOutOfRangeException("size", size, "size cannot be negative");

            if (null == sha3_512)
                throw new ArgumentNullException("sha3_512");

            if (sha3_512.Length != 512 / 8)
                throw new ArgumentException("invalid SHA3-512 length", "sha3_512");

            if (null == sha2_256)
                throw new ArgumentNullException("sha2_256");

            if (sha2_256.Length != 256 / 8)
                throw new ArgumentException("invalid SHA2-256 length", "sha2_256");

            if (null == md5)
                throw new ArgumentNullException("md5");

            if (md5.Length != 128 / 8)
                throw new ArgumentException("invalid MD5 length", "md5");

            _size = size;
            _sha3_512 = sha3_512;
            _sha2_256 = sha2_256;
            _md5 = md5;
        }

        #region IBlobFingerprint Members

        public long Size
        {
            get { return _size; }
        }

        public byte[] Sha3_512
        {
            get { return _sha3_512; }
        }

        public byte[] Sha2_256
        {
            get { return _sha2_256; }
        }

        public byte[] Md5
        {
            get { return _md5; }
        }

        public bool Equals(IBlobFingerprint other)
        {
            if (ReferenceEquals(this, other))
                return true;

            if (ReferenceEquals(null, other))
                return false;

            return Size == other.Size
                   && Sha3_512.SequenceEqual(other.Sha3_512)
                   && Sha2_256.SequenceEqual(other.Sha2_256)
                   && Md5.SequenceEqual(other.Md5);
        }

        #endregion

        public override bool Equals(object obj)
        {
            return Equals(obj as IBlobFingerprint);
        }

        public override int GetHashCode()
        {
            return BitConverter.ToInt32(Sha3_512, 0);
        }
    }
}
