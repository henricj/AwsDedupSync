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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace AwsSyncer
{
    public sealed class DirectoryScanner
    {
        public static Task GenerateAnnotatedPathsAsync(IEnumerable<CollectionPath> paths,
            ITargetBlock<AnnotatedPath[]> filePathTargetBlock, CancellationToken cancellationToken)
        {
            var shuffleBlock = new TransformBlock<AnnotatedPath[], AnnotatedPath[]>(
                filenames =>
                {
                    // Sequential names tend to fall into the same AWS S3 partition, so we
                    // shuffle things around.
                    RandomUtil.Shuffle(filenames);

                    return filenames;
                }, new ExecutionDataflowBlockOptions { CancellationToken = cancellationToken, MaxDegreeOfParallelism = Environment.ProcessorCount });

            shuffleBlock.LinkTo(filePathTargetBlock, new DataflowLinkOptions { PropagateCompletion = true });

            var batcher = new BatchBlock<AnnotatedPath>(2048, new GroupingDataflowBlockOptions { CancellationToken = cancellationToken });

            batcher.LinkTo(shuffleBlock, new DataflowLinkOptions
            {
                PropagateCompletion = true
            });

            return PostAllFilePathsAsync(paths, batcher, cancellationToken);
        }

        public static Task PostAllFilePathsAsync(IEnumerable<CollectionPath> paths,
            ITargetBlock<AnnotatedPath> filePathTargetBlock, CancellationToken cancellationToken)
        {
            var scanTasks = paths
                .Select<CollectionPath, Task>(path =>
                    Task.Factory.StartNew(async () =>
                    {
                        foreach (var file in PathUtil.ScanDirectory(path.Path))
                        {
                            if (cancellationToken.IsCancellationRequested)
                                break;

                            var relativePath = PathUtil.MakeRelativePath(path.Path, file.FullName);

                            var annotatedPath = new AnnotatedPath { FileInfo = file, Collection = path.Collection ?? path.Path, RelativePath = relativePath };

                            await filePathTargetBlock.SendAsync(annotatedPath, cancellationToken).ConfigureAwait(false);
                        }
                    },
                        cancellationToken,
                        TaskCreationOptions.DenyChildAttach | TaskCreationOptions.LongRunning | TaskCreationOptions.RunContinuationsAsynchronously,
                        TaskScheduler.Default));

            var task = Task.WhenAll(scanTasks);

            var completeTask = task.ContinueWith(_ => filePathTargetBlock.Complete());

            TaskCollector.Default.Add(completeTask, "PostallFilePathsAsync");

            return task;
        }
    }
}
