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

        public async Task UpdateLinksAsync(AwsManager awsManager, ILookup<string, string> livePaths, ISourceBlock<IBlob> linkBlobs, CancellationToken cancellationToken)
        {
            try
            {
                await CreateLinksAsync(awsManager, livePaths, linkBlobs, cancellationToken).ConfigureAwait(false);

                Debug.WriteLine("Done processing links");
            }
            catch (Exception ex)
            {
                Debug.WriteLine("Processing links failed: " + ex.Message);
            }
        }

        async Task CreateLinksAsync(AwsManager awsManager, ILookup<string, string> linkPaths,
            ISourceBlock<IBlob> blobSourceBlock, CancellationToken cancellationToken)
        {
            var collectionBlocks = new Dictionary<string, ITargetBlock<IBlob>>();
            var tasks = new List<Task>();

            var routeBlock = new ActionBlock<IBlob>(async blob =>
            {
                var collection = blob.Collection;

                if (string.IsNullOrEmpty(collection))
                    return;

                ITargetBlock<IBlob> collectionBlock;
                if (!collectionBlocks.TryGetValue(collection, out collectionBlock))
                {
                    var bufferBlock = new BufferBlock<IBlob>();

                    collectionBlock = bufferBlock;

                    collectionBlocks[collection] = collectionBlock;

                    var task = CreateLinksBlockAsync(awsManager, collection, bufferBlock, cancellationToken);

                    tasks.Add(task);
                }

                await collectionBlock.SendAsync(blob, cancellationToken).ConfigureAwait(false);
            });

            blobSourceBlock.LinkTo(routeBlock, S3PathSyncer.DataflowLinkOptionsPropagateEnabled);

            await routeBlock.Completion.ConfigureAwait(false);

            Debug.WriteLine("S3LinkCreateor.CreateLinkAsync() routeBlock is done");

            foreach (var block in collectionBlocks.Values)
                block.Complete();

            await Task.WhenAll(tasks).ConfigureAwait(false);

            Debug.WriteLine("S3LinkCreateor.CreateLinkAsync() all link blocks are done");
        }

        async Task CreateLinksBlockAsync(AwsManager awsManager, string collection, ISourceBlock<IBlob> collectionBlock, CancellationToken cancellationToken)
        {
            var links = await awsManager.GetLinksAsync(collection, cancellationToken).ConfigureAwait(false);

            Debug.WriteLine($"Link handler for {collection} found {links.Count} existing links");

            var createLinkBlock = new ActionBlock<IBlob>(blob => CreateLinkAsync(awsManager, collection, blob, links, cancellationToken),
                new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 256, CancellationToken = cancellationToken });

            collectionBlock.LinkTo(createLinkBlock, S3PathSyncer.DataflowLinkOptionsPropagateEnabled);

            await createLinkBlock.Completion.ConfigureAwait(false);

            Debug.WriteLine($"Link handler for {collection} is done");
        }

        async Task CreateLinkAsync(AwsManager awsManager, string name, IBlob blob, ICollection<string> tree, CancellationToken cancellationToken)
        {
            if (name != blob.Collection)
                return;

            var relativePath = blob.RelativePath;

            if (relativePath.StartsWith(".."))
                return;

            if (relativePath == blob.FullFilePath)
                return;

            if (relativePath.StartsWith("file:", StringComparison.OrdinalIgnoreCase))
                return;

            relativePath = relativePath.Replace('\\', '/');

            if (relativePath.StartsWith("/", StringComparison.Ordinal))
                return;

            if (tree.Contains(relativePath))
                return;

            Console.WriteLine("Link {0} {1} -> {2}", name, relativePath, blob.Key.Substring(0, 12));

            if (!_s3Settings.ActuallyWrite)
                return;

            try
            {
                await awsManager.CreateLinkAsync(name, relativePath, blob, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Link {0} {1} -> {2} failed: {3}", name, relativePath, blob.Key.Substring(0, 12), ex.Message);
            }
        }
    }
}
