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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using AwsSyncer.Types;
using AwsSyncer.Utility;

namespace AwsSyncer;

sealed class LinkFingerprintJoiner
{
    readonly ConcurrentDictionary<string, PathFingerprint> _join = new(StringComparer.OrdinalIgnoreCase);

    readonly ITargetBlock<(AnnotatedPath path, FileFingerprint fingerprint)> _targetBlock;

    public ITargetBlock<AnnotatedPath[]> AnnotatedPathsBlock { get; }
    public ITargetBlock<FileFingerprint> FileFingerprintBlock { get; }

    public LinkFingerprintJoiner(ITargetBlock<(AnnotatedPath path, FileFingerprint fingerprint)> targetBlock)
    {
        _targetBlock = targetBlock ?? throw new ArgumentNullException(nameof(targetBlock));

        AnnotatedPathsBlock = new ActionBlock<AnnotatedPath[]>(
            aps => Task.WhenAll(aps.Select(SetAnnotatedPathAsync)),
            new() { MaxDegreeOfParallelism = DataflowBlockOptions.Unbounded });

        FileFingerprintBlock = new ActionBlock<FileFingerprint>(SetFileFingerprintAsync,
            new() { MaxDegreeOfParallelism = DataflowBlockOptions.Unbounded });

        var task = Task.WhenAll(AnnotatedPathsBlock.Completion, FileFingerprintBlock.Completion)
            .ContinueWith(_ => { targetBlock.Complete(); });

        TaskCollector.Default.Add(task, "Joiner completion");
    }

    PathFingerprint GetPathFingerprint(string fullFilePath)
    {
        PathFingerprint pathFingerprint;
        while (!_join.TryGetValue(fullFilePath, out pathFingerprint))
        {
            pathFingerprint = new();

            if (_join.TryAdd(fullFilePath, pathFingerprint))
                break;
        }

        return pathFingerprint;
    }

    Task SetFileFingerprintAsync(FileFingerprint fileFingerprint)
    {
        var pathFingerprint = GetPathFingerprint(fileFingerprint.FullFilePath);

        AnnotatedPath[] annotatedPaths;

        lock (pathFingerprint)
        {
            if (null != pathFingerprint.FileFingerprint)
                throw new InvalidOperationException("Duplicate file fingerprint for " + fileFingerprint.FullFilePath);

            pathFingerprint.FileFingerprint = fileFingerprint;

            if (0 == pathFingerprint.AnnotatedPaths.Count)
                return Task.CompletedTask;

            annotatedPaths = [.. pathFingerprint.AnnotatedPaths.Values];
        }

        var tasks = annotatedPaths.Select(ap => _targetBlock.SendAsync((ap, fileFingerprint)));

        return Task.WhenAll(tasks);
    }

    Task SetAnnotatedPathAsync(AnnotatedPath annotatedPath)
    {
        var pathFingerprint = GetPathFingerprint(annotatedPath.FileInfo.FullName);

        var key = (annotatedPath.Collection, annotatedPath.RelativePath);

        FileFingerprint fileFingerprint;

        lock (pathFingerprint)
        {
            if (!pathFingerprint.AnnotatedPaths.TryAdd(key, annotatedPath))
            {
                throw new InvalidOperationException("Duplicate collection/relative path for " +
                    annotatedPath.FileInfo.FullName);
            }

            fileFingerprint = pathFingerprint.FileFingerprint;
        }

        if (fileFingerprint is null)
            return Task.CompletedTask;

        return _targetBlock.SendAsync((annotatedPath, fileFingerprint));
    }

    sealed class CollectionPathComparer : IEqualityComparer<(string collection, string path)>
    {
        public bool Equals((string collection, string path) x, (string collection, string path) y)
            => string.Equals(x.collection, y.collection, StringComparison.OrdinalIgnoreCase)
                && string.Equals(x.path, y.path, StringComparison.OrdinalIgnoreCase);

        public int GetHashCode((string collection, string path) obj) => obj.GetHashCode();
    }

    sealed class PathFingerprint
    {
        public readonly Dictionary<(string collection, string path), AnnotatedPath> AnnotatedPaths =
            new(new CollectionPathComparer());

        public FileFingerprint FileFingerprint;
    }
}
