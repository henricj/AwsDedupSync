using System;
using System.Collections.Generic;
using System.IO.Hashing;
using System.Linq;

namespace AwsSyncer.Utility;

public sealed class ByteArrayComparer : IEqualityComparer<byte[]>
{
    // Note: We can't actually get long.MaxValue since it is an exclusive upper bound, but
    // this isn't for a crypto hash so let's not worry about it.
    static readonly long Seed = Random.Shared.NextInt64(long.MinValue, long.MaxValue);
    public static readonly ByteArrayComparer Instance = new();

    public bool Equals(byte[] x, byte[] y)
    {
        if (x == y)
            return true;
        if (null == x || null == y)
            return false;
        if (x.Length != y.Length)
            return false;

        return x.SequenceEqual(y);
    }

    public int GetHashCode(byte[] obj)
    {
        ArgumentNullException.ThrowIfNull(obj);

        return unchecked((int)XxHash3.HashToUInt64(obj, Seed));
    }
}
