// Copyright (c) 2014-2017, 2023 Henric Jungheim <software@henric.org>
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
using AwsSyncer.AWS;
using AwsSyncer.FileBlobs;
using AwsSyncer.FingerprintStore;
using AwsSyncer.Types;
using AwsSyncer.Utility;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.ObjectPool;

namespace AwsDedupSync;

public class S3PathSyncer
{
    static readonly DataflowLinkOptions DataflowLinkOptionsPropagateEnabled
        = new() { PropagateCompletion = true };

    readonly S3BlobUploader _s3BlobUploader;
    readonly S3LinkCreator _s3LinkCreator;

    public S3Settings S3Settings { get; } = new()
    {
        ActuallyWrite = false,
        UpdateLinks = false,
        UploadBlobs = false
    };

    public S3PathSyncer()
    {
        _s3BlobUploader = new(S3Settings);
        _s3LinkCreator = new(S3Settings);
    }

    public async Task SyncPathsAsync(IConfiguration config, IEnumerable<string> paths, Func<FileInfo, bool> filePredicate,
        CancellationToken cancellationToken)
    {
        var bucket = config["Bucket"];

        if (string.IsNullOrWhiteSpace(bucket))
            throw new KeyNotFoundException("No bucket name found in the application settings");

        Console.WriteLine($"Targeting bucket {bucket}");

        // BEWARE:  This could cause trouble if there are
        // any case-sensitive paths involved.
        var namedPaths = (from arg in paths
                let split = arg.IndexOf('=')
                let validSplit = split > 0 && split < arg.Length - 1
                select new CollectionPath(validSplit ? arg[..split] : null,
                    PathUtil.ForceTrailingSlash(Path.GetFullPath(validSplit ? arg[(split + 1)..] : arg))))
            .Distinct()
            .ToArray();

        var objectPoolProvider = new DefaultObjectPoolProvider();

        using var blobManager =
            new BlobManager(new FileFingerprintManager(new MessagePackFileFingerprintStore(bucket),
                new StreamFingerprinter(objectPoolProvider)));
        try
        {
            using var awsManager = AwsManagerFactory.Create(bucket, config);
            var uniqueFingerprintBlock = new BufferBlock<Tuple<FileFingerprint, AnnotatedPath>>();
            var linkBlock = new BufferBlock<Tuple<AnnotatedPath, FileFingerprint>>();

            var uniqueFingerprints = new HashSet<BlobFingerprint>();

            var uniqueFingerprintFilterBlock = new ActionBlock<Tuple<AnnotatedPath, FileFingerprint>>(
                t =>
                {
                    if (!uniqueFingerprints.Add(t.Item2.Fingerprint))
                        return Task.CompletedTask;

                    return uniqueFingerprintBlock.SendAsync(Tuple.Create(t.Item2, t.Item1), cancellationToken);
                }, new() { CancellationToken = cancellationToken });

            var uniqueCompletionTask = uniqueFingerprintFilterBlock
                .Completion.ContinueWith(_ => { uniqueFingerprintBlock.Complete(); }, CancellationToken.None);

            TaskCollector.Default.Add(uniqueCompletionTask, "Unique filter completion");

            var joinedBroadcastBlock = new BroadcastBlock<Tuple<AnnotatedPath, FileFingerprint>>(t => t,
                new() { CancellationToken = cancellationToken });

            joinedBroadcastBlock.LinkTo(linkBlock, DataflowLinkOptionsPropagateEnabled);
            joinedBroadcastBlock.LinkTo(uniqueFingerprintFilterBlock, DataflowLinkOptionsPropagateEnabled);

            var tasks = new List<Task>();

            var loadBlobTask = blobManager.LoadAsync(namedPaths, filePredicate, joinedBroadcastBlock, cancellationToken);

            tasks.Add(loadBlobTask);

            if (S3Settings.UpdateLinks)
            {
                var updateLinksTask = _s3LinkCreator.UpdateLinksAsync(awsManager, linkBlock, cancellationToken);

                tasks.Add(updateLinksTask);
            }

            Task uploadBlobsTask = null;
            var scanBlobAsync = Task.Run(async () =>
            {
                // ReSharper disable once AccessToDisposedClosure
                var knownObjects = await awsManager.ScanAsync(cancellationToken).ConfigureAwait(false);

                if (S3Settings.UploadBlobs)
                {
                    // ReSharper disable once AccessToDisposedClosure
                    uploadBlobsTask = _s3BlobUploader.UploadBlobsAsync(awsManager, uniqueFingerprintBlock, knownObjects,
                        cancellationToken);
                }
            }, cancellationToken);

            tasks.Add(scanBlobAsync);

            await WaitAllWithWake(tasks).ConfigureAwait(false);

            if (null != uploadBlobsTask)
                await uploadBlobsTask.ConfigureAwait(false);
        }
        finally
        {
            await blobManager.ShutdownAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    static async Task WaitAllWithWake(ICollection<Task> tasks)
    {
        for (;;)
        {
            var pendingTasks = tasks.Where(t => !t.IsCompleted).ToArray();

            if (0 == pendingTasks.Length)
                break;

            await Task.WhenAny(Task.WhenAll(pendingTasks), Task.Delay(TimeSpan.FromSeconds(3))).ConfigureAwait(false);
        }
    }
}
