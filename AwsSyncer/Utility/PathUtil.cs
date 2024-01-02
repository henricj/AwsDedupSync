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
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;

namespace AwsSyncer.Utility;

public static class PathUtil
{
    // From http://stackoverflow.com/a/340454/2875705
    /// <summary>
    ///     Creates a relative path from one file or folder to another.
    /// </summary>
    /// <param name="fromPath">Contains the directory that defines the start of the relative path.</param>
    /// <param name="toPath">Contains the path that defines the endpoint of the relative path.</param>
    /// <returns>The relative path from the start directory to the end path.</returns>
    /// <exception cref="ArgumentNullException"></exception>
    /// <exception cref="UriFormatException"></exception>
    /// <exception cref="InvalidOperationException"></exception>
    public static string MakeRelativePath(string fromPath, string toPath)
    {
        if (string.IsNullOrWhiteSpace(fromPath))
            throw new ArgumentNullException(nameof(fromPath));
        if (string.IsNullOrWhiteSpace(toPath))
            throw new ArgumentNullException(nameof(toPath));

        var fromUri = new Uri(fromPath);
        var toUri = new Uri(toPath);

        if (fromUri.Scheme != toUri.Scheme)
            return toPath;

        var relativeUri = fromUri.MakeRelativeUri(toUri);
        var relativePath = Uri.UnescapeDataString(relativeUri.ToString());

        if (string.Equals(toUri.Scheme.ToUpperInvariant(), "FILE", StringComparison.OrdinalIgnoreCase))
            relativePath = relativePath.Replace(Path.AltDirectorySeparatorChar, Path.DirectorySeparatorChar);

        return relativePath;
    }

    /// <summary>
    ///     Remove non-ASCII characters.  Remove leading and trailing white-space.
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public static string NormalizeAsciiName(string value)
    {
        if (string.IsNullOrWhiteSpace(value) || value.Length > 1024)
            return null;

        if (value.All(c => c > 32 && c < 127))
            return value;

        // This really isn't enough.  We should be much more restrictive
        // than simply looking for ASCII.
        value = value.Normalize();
        var chars = value.AsSpan().Trim();

        // Encoding.ASCII.GetByteCount() can be at most chars.Length.
        Span<byte> buffer = stackalloc byte[chars.Length];
        if (!Encoding.ASCII.TryGetBytes(chars, buffer, out var written))
            return null;

        return Encoding.ASCII.GetString(buffer[..written]);
    }

    /// <summary>
    ///     Require that the string is normalized.  Throw an exception otherwise.
    /// </summary>
    /// <param name="value"></param>
    public static void RequireNormalizedAsciiName(string value)
    {
        var normalized = NormalizeAsciiName(value);

        if (!string.Equals(normalized, value, StringComparison.Ordinal))
            throw new ArgumentException("non-normalized string");
    }

    public static string ForceTrailingSlash(string path)
    {
        if (!path.EndsWith("/", StringComparison.OrdinalIgnoreCase) && !path.EndsWith("\\", StringComparison.OrdinalIgnoreCase))
            return path + "\\";

        return path;
    }

    public static IEnumerable<FileInfo> ScanDirectory(string path, Func<FileInfo, bool> filePredicate)
    {
        Debug.WriteLine($"PathUtil.ScanDirectory({path})");

        var timer = Stopwatch.StartNew();

        var attr = File.GetAttributes(path);

        if (FileAttributes.Directory == (attr & FileAttributes.Directory))
        {
            var dir = new DirectoryInfo(path);

            if (dir.Exists)
            {
                foreach (var fileInfo in dir.EnumerateFiles("*", SearchOption.AllDirectories))
                {
                    if (filePredicate(fileInfo))
                        yield return fileInfo;
                    else
                        Debug.WriteLine($"PathUtil.ScanDirectory({path}) excluding {fileInfo.FullName}");
                }
            }
        }
        else if (0 == (attr & (FileAttributes.Offline | FileAttributes.ReparsePoint)))
        {
            var fileInfo = new FileInfo(path);

            if (fileInfo.Exists && filePredicate(fileInfo))
                yield return fileInfo;
            else
                Debug.WriteLine($"PathUtil.ScanDirectory({path}) excluding {fileInfo.FullName}");
        }

        timer.Stop();

        Debug.WriteLine($"PathUtil.ScanDirectory({path}) after {timer.Elapsed}");
    }

    public static string GetHost(string path)
    {
        var uri = new Uri(path, UriKind.RelativeOrAbsolute);

        return uri.IsAbsoluteUri ? uri.Host : string.Empty;
    }
}
