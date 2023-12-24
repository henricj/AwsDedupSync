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
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using AwsSyncer.Types;
using AwsSyncer.Utility;

namespace AwsSyncer.FileBlobs;

public static class DirectoryScanner
{
    public static Task GenerateAnnotatedPathsAsync(IEnumerable<CollectionPath> paths,
        Func<FileInfo, bool> filePredicate,
        ITargetBlock<AnnotatedPath[]> filePathTargetBlock,
        CancellationToken cancellationToken)
    {
        var shuffleBlock = new TransformBlock<AnnotatedPath[], AnnotatedPath[]>(
            filenames =>
            {
                // Sequential names tend to fall into the same AWS S3 partition, so we
                // shuffle things around.
                RandomUtil.Shuffle(filenames);

                return filenames;
            },
            new()
            {
                CancellationToken = cancellationToken,
                MaxDegreeOfParallelism = Environment.ProcessorCount
            });

        shuffleBlock.LinkTo(filePathTargetBlock, new() { PropagateCompletion = true });

        var batcher = new BatchBlock<AnnotatedPath>(2048,
            new() { CancellationToken = cancellationToken });

        batcher.LinkTo(shuffleBlock, new() { PropagateCompletion = true });

        var scanTasks = paths
            .Select<CollectionPath, Task>(path =>
                Task.Factory.StartNew(
                    async () =>
                    {
                        foreach (var file in PathUtil.ScanDirectory(path.Path, filePredicate))
                        {
                            if (cancellationToken.IsCancellationRequested)
                                break;

                            var relativePath = PathUtil.MakeRelativePath(path.Path, file.FullName);

                            var annotatedPath = new AnnotatedPath(file, path.Collection ?? path.Path, relativePath);

                            await batcher.SendAsync(annotatedPath, cancellationToken).ConfigureAwait(false);
                        }
                    },
                    cancellationToken,
                    TaskCreationOptions.DenyChildAttach | TaskCreationOptions.LongRunning |
                    TaskCreationOptions.RunContinuationsAsynchronously,
                    TaskScheduler.Default));

        var task = Task.WhenAll(scanTasks);

        var localFilePathTargetBlock = (ITargetBlock<AnnotatedPath>)batcher;

        var completeTask = task.ContinueWith(_ => localFilePathTargetBlock.Complete(), cancellationToken);

        TaskCollector.Default.Add(completeTask, "GenerateAnnotatedPathsAsync");

        return task;
    }
}
