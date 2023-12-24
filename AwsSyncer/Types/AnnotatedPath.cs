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
using System.IO;

namespace AwsSyncer.Types;

public sealed class AnnotatedPath : IEquatable<AnnotatedPath>
{
    public FileInfo FileInfo { get; }
    public string Collection { get; }
    public string RelativePath { get; }

    public AnnotatedPath(FileInfo fileInfo, string collection, string relativePath)
    {
        FileInfo = fileInfo ?? throw new ArgumentNullException(nameof(fileInfo));
        Collection = collection ?? throw new ArgumentNullException(nameof(collection));
        RelativePath = relativePath ?? throw new ArgumentNullException(nameof(relativePath));
    }

    public bool Equals(AnnotatedPath other)
    {
        if (other is null)
            return false;
        if (ReferenceEquals(this, other))
            return true;

        return FileInfo.Equals(other.FileInfo)
            && string.Equals(Collection, other.Collection, StringComparison.OrdinalIgnoreCase)
            && string.Equals(RelativePath, other.RelativePath, StringComparison.OrdinalIgnoreCase);
    }

    public override string ToString() => '[' + Collection + ']' + RelativePath;

    public override bool Equals(object obj)
    {
        if (obj is null)
            return false;
        if (ReferenceEquals(this, obj))
            return true;

        return obj is AnnotatedPath path && Equals(path);
    }

    public override int GetHashCode()
    {
        unchecked
        {
            var hashCode = FileInfo.GetHashCode();
            hashCode = (hashCode * 397) ^ StringComparer.OrdinalIgnoreCase.GetHashCode(Collection);
            hashCode = (hashCode * 397) ^ StringComparer.OrdinalIgnoreCase.GetHashCode(RelativePath);

            return hashCode;
        }
    }

    public static bool operator ==(AnnotatedPath left, AnnotatedPath right) => Equals(left, right);

    public static bool operator !=(AnnotatedPath left, AnnotatedPath right) => !Equals(left, right);
}
