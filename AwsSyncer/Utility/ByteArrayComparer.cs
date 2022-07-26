using System;
using System.Collections.Generic;

namespace AwsSyncer.Utility
{
    public sealed class ByteArrayComparer : IEqualityComparer<byte[]>
    {
        public static readonly ByteArrayComparer Instance = new();

        public bool Equals(byte[] x, byte[] y)
        {
            if (x == y)
                return true;
            if (null == x || null == y)
                return false;
            if (x.Length != y.Length)
                return false;

            for (var i = 0; i < x.Length; ++i)
            {
                if (x[i] != y[i])
                    return false;
            }

            return true;
        }

        public int GetHashCode(byte[] obj)
        {
            if (obj == null)
                throw new ArgumentNullException(nameof(obj));

            var hash = 1422328907;
            foreach (var e in obj)
                hash = hash * 31 + e;

            return hash;
        }
    }
}
