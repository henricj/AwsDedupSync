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

namespace AwsSyncer.Types;

public sealed class CollectionPath : IEquatable<CollectionPath>
{
    public string Collection { get; }
    public string Path { get; }

    public CollectionPath(string collection, string path)
    {
        Collection = collection ?? throw new ArgumentNullException(nameof(collection));
        Path = path ?? throw new ArgumentNullException(nameof(path));
    }

    public bool Equals(CollectionPath other)
    {
        if (other is null)
            return false;
        if (ReferenceEquals(this, other))
            return true;

        return string.Equals(Collection, other.Collection, StringComparison.OrdinalIgnoreCase) &&
            string.Equals(Path, other.Path, StringComparison.OrdinalIgnoreCase);
    }

    public override string ToString() => '[' + Collection + ']' + Path;

    public override bool Equals(object obj)
    {
        if (obj is null)
            return false;
        if (ReferenceEquals(this, obj))
            return true;

        return obj is CollectionPath path && Equals(path);
    }

    public override int GetHashCode()
    {
        unchecked
        {
            return (StringComparer.OrdinalIgnoreCase.GetHashCode(Collection) * 397) ^
                StringComparer.OrdinalIgnoreCase.GetHashCode(Path);
        }
    }

    public static bool operator ==(CollectionPath left, CollectionPath right) => Equals(left, right);

    public static bool operator !=(CollectionPath left, CollectionPath right) => !Equals(left, right);
}
