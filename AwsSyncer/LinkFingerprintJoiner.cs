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

namespace AwsSyncer
{
    class LinkFingerprintJoiner
    {
        readonly ConcurrentDictionary<string, PathFingerprint> _join
            = new ConcurrentDictionary<string, PathFingerprint>(StringComparer.CurrentCultureIgnoreCase);

        readonly ITargetBlock<Tuple<AnnotatedPath, FileFingerprint>> _targetBlock;

        public LinkFingerprintJoiner(ITargetBlock<Tuple<AnnotatedPath, FileFingerprint>> targetBlock)
        {
            _targetBlock = targetBlock ?? throw new ArgumentNullException(nameof(targetBlock));

            AnnotatedPathsBlock = new ActionBlock<AnnotatedPath[]>(
                aps => Task.WhenAll(aps.Select(SetAnnotatedPathAsync)),
                new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = DataflowBlockOptions.Unbounded });

            FileFingerprintBlock = new ActionBlock<FileFingerprint>(SetFileFingerprintAsync,
                new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = DataflowBlockOptions.Unbounded });

            var task = Task.WhenAll(AnnotatedPathsBlock.Completion, FileFingerprintBlock.Completion)
                .ContinueWith(_ => { targetBlock.Complete(); });

            TaskCollector.Default.Add(task, "Joiner completion");
        }

        public ITargetBlock<AnnotatedPath[]> AnnotatedPathsBlock { get; }
        public ITargetBlock<FileFingerprint> FileFingerprintBlock { get; }

        PathFingerprint GetPathFingerprint(string fullFilePath)
        {
            PathFingerprint pathFingerprint;
            while (!_join.TryGetValue(fullFilePath, out pathFingerprint))
            {
                pathFingerprint = new PathFingerprint();

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

                annotatedPaths = pathFingerprint.AnnotatedPaths.Values.ToArray();
            }

            var tasks = annotatedPaths.Select(ap => _targetBlock.SendAsync(Tuple.Create(ap, fileFingerprint)));

            return Task.WhenAll(tasks);
        }

        Task SetAnnotatedPathAsync(AnnotatedPath annotatedPath)
        {
            var pathFingerprint = GetPathFingerprint(annotatedPath.FileInfo.FullName);

            var key = Tuple.Create(annotatedPath.Collection, annotatedPath.RelativePath);

            FileFingerprint fileFingerprint;

            lock (pathFingerprint)
            {
                if (pathFingerprint.AnnotatedPaths.ContainsKey(key))
                    throw new InvalidOperationException("Duplicate collection/relative path for " + annotatedPath.FileInfo.FullName);

                pathFingerprint.AnnotatedPaths[key] = annotatedPath;

                fileFingerprint = pathFingerprint.FileFingerprint;
            }

            if (null == fileFingerprint)
                return Task.CompletedTask;

            return _targetBlock.SendAsync(Tuple.Create(annotatedPath, fileFingerprint));
        }

        class PathFingerprint
        {
            public readonly Dictionary<Tuple<string, string>, AnnotatedPath> AnnotatedPaths
                = new Dictionary<Tuple<string, string>, AnnotatedPath>();

            public FileFingerprint FileFingerprint;
        }
    }
}
