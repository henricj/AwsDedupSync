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
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace AwsSyncer
{
    public class LinkManager
    {
        public async Task CreateLinksAsync(AwsManager awsManager, ISourceBlock<Tuple<AnnotatedPath, IFileFingerprint>> blobSourceBlock,
            bool actuallyWrite, CancellationToken cancellationToken)
        {
            var collectionBlocks = new Dictionary<string, ITargetBlock<Tuple<AnnotatedPath, IFileFingerprint>>>();
            var tasks = new List<Task>();

            var routeBlock = new ActionBlock<Tuple<AnnotatedPath, IFileFingerprint>>(async blob =>
            {
                var collection = blob.Item1.Collection;

                if (string.IsNullOrEmpty(collection))
                    return;

                ITargetBlock<Tuple<AnnotatedPath, IFileFingerprint>> collectionBlock;
                if (!collectionBlocks.TryGetValue(collection, out collectionBlock))
                {
                    var bufferBlock = new BufferBlock<Tuple<AnnotatedPath, IFileFingerprint>>();

                    collectionBlock = bufferBlock;

                    collectionBlocks[collection] = collectionBlock;

                    var task = CreateLinksBlockAsync(awsManager, collection, bufferBlock, actuallyWrite, cancellationToken);

                    tasks.Add(task);
                }

                await collectionBlock.SendAsync(blob, cancellationToken).ConfigureAwait(false);
            });

            blobSourceBlock.LinkTo(routeBlock, new DataflowLinkOptions { PropagateCompletion = true });

            await routeBlock.Completion.ConfigureAwait(false);

            Debug.WriteLine("S3LinkCreateor.CreateLinkAsync() routeBlock is done");

            foreach (var block in collectionBlocks.Values)
                block.Complete();

            await Task.WhenAll(tasks).ConfigureAwait(false);

            Debug.WriteLine("S3LinkCreateor.CreateLinkAsync() all link blocks are done");
        }

        async Task CreateLinksBlockAsync(AwsManager awsManager, string collection,
            ISourceBlock<Tuple<AnnotatedPath, IFileFingerprint>> collectionBlock, bool actuallyWrite, CancellationToken cancellationToken)
        {
            var links = await awsManager.GetLinksAsync(collection, cancellationToken).ConfigureAwait(false);

            Debug.WriteLine($"Link handler for {collection} found {links.Count} existing links");

            var createLinkBlock = new ActionBlock<S3Links.ICreateLinkRequest>(
                link => CreateLinkAsync(awsManager, link, actuallyWrite, cancellationToken),
                new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 512, CancellationToken = cancellationToken });

            var makeLinkBlock = new TransformBlock<Tuple<AnnotatedPath, IFileFingerprint>, S3Links.ICreateLinkRequest>(
                tuple =>
                {
                    var path = tuple.Item1;
                    var file = tuple.Item2;

                    if (collection != path.Collection)
                        throw new InvalidOperationException($"Create link for {path.Collection} on {collection}");

                    var relativePath = path.RelativePath;

                    if (relativePath.StartsWith(".."))
                        throw new InvalidOperationException($"Create link for invalid path {relativePath}");

                    if (relativePath.StartsWith("file:", StringComparison.OrdinalIgnoreCase))
                        throw new InvalidOperationException($"Create link for invalid path {relativePath}");

                    relativePath = relativePath.Replace('\\', '/');

                    if (relativePath.StartsWith("/", StringComparison.Ordinal))
                        throw new InvalidOperationException($"Create link for invalid path {relativePath}");

                    string eTag;
                    links.TryGetValue(relativePath, out eTag);

                    return awsManager.BuildLinkRequest(collection, relativePath, file, eTag);
                },
                new ExecutionDataflowBlockOptions { CancellationToken = cancellationToken, MaxDegreeOfParallelism = Environment.ProcessorCount });

            makeLinkBlock.LinkTo(createLinkBlock, new DataflowLinkOptions { PropagateCompletion = true }, link => null != link);
            makeLinkBlock.LinkTo(DataflowBlock.NullTarget<S3Links.ICreateLinkRequest>());

            collectionBlock.LinkTo(makeLinkBlock, new DataflowLinkOptions { PropagateCompletion = true });

            await createLinkBlock.Completion.ConfigureAwait(false);

            Debug.WriteLine($"Link handler for {collection} is done");
        }

        async Task CreateLinkAsync(AwsManager awsManager, S3Links.ICreateLinkRequest createLinkRequest, bool actuallyWrite, CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
                return;

            var relativePath = createLinkRequest.RelativePath;
            var key = createLinkRequest.FileFingerprint.Fingerprint.Key();

            Console.WriteLine("Link {0} \"{1}\" -> {2} ({3})",
                createLinkRequest.Collection, relativePath, key.Substring(0, 12),
                createLinkRequest.FileFingerprint.WasCached ? "cached" : "new");

            if (!actuallyWrite)
                return;

            try
            {
                await awsManager.CreateLinkAsync(createLinkRequest, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            { }
            catch (Exception ex)
            {
                Console.WriteLine("Link {0} {1} -> {2} failed: {3}", createLinkRequest.Collection, relativePath, key.Substring(0, 12), ex.Message);
            }
        }
    }
}
