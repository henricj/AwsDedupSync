using System;

namespace AwsSyncer
{
    public interface IBlob : IEquatable<IBlob>
    {
        string FullPath { get; }
        DateTime LastModifiedUtc { get; }
        IBlobFingerprint Fingerprint { get; }
    }

    public class Blob : IBlob
    {
        readonly IBlobFingerprint _fingerprint;
        readonly string _fullPath;
        readonly DateTime _lastModifiedUtc;

        public Blob(string fullPath, DateTime lastModifiedUtc, IBlobFingerprint fingerprint)
        {
            if (string.IsNullOrWhiteSpace(fullPath))
                throw new ArgumentNullException("fullPath");
            if (null == fingerprint)
                throw new ArgumentNullException("fingerprint");
            if (string.IsNullOrWhiteSpace(fullPath))
                throw new ArgumentOutOfRangeException("fullPath");
            if (lastModifiedUtc.Kind != DateTimeKind.Utc)
                throw new ArgumentException("time must be UTC", "lastModifiedUtc");

            _fullPath = fullPath;
            _fingerprint = fingerprint;
            _lastModifiedUtc = lastModifiedUtc;
        }

        #region IBlob Members

        public string FullPath
        {
            get { return _fullPath; }
        }

        public DateTime LastModifiedUtc
        {
            get { return _lastModifiedUtc; }
        }

        public IBlobFingerprint Fingerprint
        {
            get { return _fingerprint; }
        }

        public bool Equals(IBlob other)
        {
            if (ReferenceEquals(this, other))
                return true;

            if (ReferenceEquals(null, other))
                return false;

            return FullPath == other.FullPath
                   && Fingerprint.Equals(other.Fingerprint);
        }

        #endregion

        public override bool Equals(object obj)
        {
            return Equals(obj as IBlob);
        }

        public override int GetHashCode()
        {
            return FullPath.GetHashCode() ^ Fingerprint.GetHashCode();
        }
    }
}
