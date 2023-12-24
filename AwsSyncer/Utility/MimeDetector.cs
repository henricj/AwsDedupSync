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
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.Versioning;
using Microsoft.AspNetCore.StaticFiles;
using Microsoft.Win32;

namespace AwsSyncer.Utility;

// ReSharper disable once ClassNeverInstantiated.Global
public partial class MimeDetector
{
    static readonly FileExtensionContentTypeProvider FileExtensionContentTypeProvider = new();

    static readonly string DefaultMapping = "application/octet-stream";

    static readonly Lazy<MimeDetector> DefaultInstance = new();

    static readonly IReadOnlyDictionary<string, string> MimeTypes = CreateMimeTypes();

    static IReadOnlyDictionary<string, string> CreateMimeTypes()
    {
        IReadOnlyDictionary<string, string> mimeTypes;

        if (OperatingSystem.IsWindows())
        {
            try
            {
                mimeTypes = BaseMimeTypes.Union(ScanRegistry()).ToFrozenDictionary(StringComparer.OrdinalIgnoreCase);
                return mimeTypes;
            }
            catch (Exception ex)
            {
                Debug.WriteLine("MimeDetector: registry scan failed: " + ex.Message);
            }
        }

        mimeTypes = BaseMimeTypes.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase);
        return mimeTypes;
    }

    [SupportedOSPlatform("windows")]
    static IEnumerable<KeyValuePair<string, string>> ScanRegistry()
    {
        foreach (var extension in Registry.ClassesRoot
            .GetSubKeyNames()
            .Where(sk => !string.IsNullOrWhiteSpace(sk) && sk[0] == '.'))
        {
            using var key = Registry.ClassesRoot.OpenSubKey(extension);
            var mimeType = key?.GetValue("Content Type") as string;

            if (string.IsNullOrWhiteSpace(mimeType))
                continue;

            yield return new(extension, mimeType);
        }
    }

    public static string GetMimeType(string filename)
    {
        if (FileExtensionContentTypeProvider.TryGetContentType(filename, out var mimeType))
            return mimeType;

        var ext = Path.GetExtension(filename);

        if (string.IsNullOrEmpty(ext))
            return DefaultMapping;

        if (MimeTypes.TryGetValue(ext, out mimeType))
            return mimeType;

        return DefaultMapping;
    }
}
