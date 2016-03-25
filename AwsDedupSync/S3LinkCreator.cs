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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using AwsSyncer;

namespace AwsDedupSync
{
    public class S3LinkCreator
    {
        readonly S3Settings _s3Settings;

        public S3LinkCreator(S3Settings s3Settings)
        {
            _s3Settings = s3Settings;
        }

        public Task UpdateLinksAsync(AwsManager awsManager, ILookup<string, string> livePaths, ISourceBlock<IBlob> linkBlobs, CancellationToken cancellationToken)
        {
            var linksTask = CreateLinksAsync(awsManager, livePaths, linkBlobs, cancellationToken);

            var traceTask = linksTask.ContinueWith(t => Trace.WriteLine("Done processing links"), cancellationToken);

            return Task.WhenAll(linksTask, traceTask);
        }

        async Task CreateLinksAsync(AwsManager awsManager, ILookup<string, string> linkPaths,
            ISourceBlock<IBlob> blobSourceBlock, CancellationToken cancellationToken)
        {
            // ReSharper disable once AccessToDisposedClosure
            var pathTasks = linkPaths.Select(
                namePath => new
                {
                    NamePath = namePath,
                    LinksTask = awsManager.GetLinksAsync(namePath.Key, cancellationToken)
                }).ToArray();

            await Task.WhenAll(pathTasks.Select(pt => pt.LinksTask)).ConfigureAwait(false);

            var linkTrees = pathTasks.Select(pt => new
            {
                pt.NamePath,
                Tree = pt.LinksTask.Result
            }).ToArray();

            var broadcastBlock = new BroadcastBlock<IBlob>(b => b);

            var tasks = new List<Task>();

            foreach (var linkTree in linkTrees)
            {
                foreach (var path in linkTree.NamePath)
                {
                    var key = linkTree.NamePath.Key;
                    var links = linkTree.Tree;

                    var createLinkBlock = new ActionBlock<IBlob>(
                        blob => CreateLinkAsync(awsManager, key, blob, links, cancellationToken));

                    var bufferBlock = new BufferBlock<IBlob>();

                    bufferBlock.LinkTo(createLinkBlock, S3PathSyncer.DataflowLinkOptionsPropagateEnabled);
                    broadcastBlock.LinkTo(bufferBlock, S3PathSyncer.DataflowLinkOptionsPropagateEnabled);

                    tasks.Add(createLinkBlock.Completion);
                }
            }

            blobSourceBlock.LinkTo(broadcastBlock, S3PathSyncer.DataflowLinkOptionsPropagateEnabled);

            await Task.WhenAll(tasks).ConfigureAwait(false);
        }

        Task CreateLinkAsync(AwsManager awsManager, string name, IBlob blob, ICollection<string> tree, CancellationToken cancellationToken)
        {
            if (name != blob.Collection)
                return Task.CompletedTask;

            var relativePath = blob.RelativePath;

            if (relativePath.StartsWith(".."))
                return Task.CompletedTask;

            if (relativePath == blob.FullFilePath)
                return Task.CompletedTask;

            if (relativePath.StartsWith("file:", StringComparison.OrdinalIgnoreCase))
                return Task.CompletedTask;

            relativePath = relativePath.Replace('\\', '/');

            if (relativePath.StartsWith("/", StringComparison.Ordinal))
                return Task.CompletedTask;

            if (tree.Contains(relativePath))
                return Task.CompletedTask;

            Console.WriteLine("Link {0} {1} -> {2}", name, relativePath, blob.Key.Substring(12));

            if (!_s3Settings.ActuallyWrite)
                return Task.CompletedTask;

            return awsManager.CreateLinkAsync(name, relativePath, blob, cancellationToken);
        }
    }
}
