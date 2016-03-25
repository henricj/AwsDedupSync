// Copyright (c) 2016 Henric Jungheim <software@henric.org>
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
using System.Globalization;
using System.IO;
using System.Linq;

namespace AwsSyncer
{
    public class FileSequence
    {
        static readonly List<FileInfo> EmptyFiles = new List<FileInfo>();
        readonly DirectoryInfo _directory;
        List<FileInfo> _files = EmptyFiles;

        public FileSequence(DirectoryInfo directory)
        {
            if (null == directory)
                throw new ArgumentNullException(nameof(directory));

            _directory = directory;
        }

        public IEnumerable<FileInfo> Files => _files;

        public void Rescan()
        {
            _files = _directory.EnumerateFiles()
                .Select(file =>
                {
                    int fileNumber;
                    if (!int.TryParse(file.Name, NumberStyles.Integer, CultureInfo.InvariantCulture, out fileNumber))
                        return null;

                    if (fileNumber < 1)
                        return null;

                    return new { File = file, Number = fileNumber };
                })
                .Where(fn => null != fn)
                .OrderBy(fn => fn.Number)
                .Select(fn => fn.File)
                .ToList();
        }

        public FileInfo NewFile()
        {
            Rescan();

            Prune();

            var count = 1;

            if (_files.Count > 0)
            {
                count = int.Parse(_files[_files.Count - 1].Name) + 1;
            }

            var fileName = Path.Combine(_directory.FullName, count.ToString(CultureInfo.InvariantCulture));

            var fileInfo = new FileInfo(fileName);

            _files.Add(fileInfo);

            return fileInfo;
        }

        public void Prune()
        {
            var change = false;

            foreach (var file in _files)
            {
                if (file.Exists && file.Length >= 5)
                    continue;

                // Make sure the file information is not stale.
                file.Refresh();

                if (file.Exists && file.Length >= 5)
                    continue;

                try
                {
                    file.Delete();
                    change = true;
                }
                catch (IOException ex)
                {
                    Debug.WriteLine($"Unable to delete file {file.FullName}: {ex.Message}");
                }
            }

            if (change)
                Rescan();
        }
    }
}
