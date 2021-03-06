﻿// Copyright (c) 2014-2017 Henric Jungheim <software@henric.org>
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
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using AwsSyncer.Types;

namespace AwsSyncer.FileBlobs
{
    public interface IBlobManager : IDisposable
    {
        Task LoadAsync(CollectionPath[] paths,
            Func<FileInfo, bool> filePredicate,
            ITargetBlock<Tuple<AnnotatedPath, FileFingerprint>> joinedTargetBlock,
            CancellationToken cancellationToken);

        Task ShutdownAsync(CancellationToken cancellationToken);
    }

    public sealed class BlobManager : IBlobManager
    {
        readonly IFileFingerprintManager _fileFingerprintManager;

        public BlobManager(IFileFingerprintManager fileFingerprintManager)
        {
            _fileFingerprintManager = fileFingerprintManager ?? throw new ArgumentNullException(nameof(fileFingerprintManager));
        }

        #region IDisposable Members

        public void Dispose()
        {
            _fileFingerprintManager.Dispose();
        }

        #endregion

        public async Task LoadAsync(CollectionPath[] paths,
            Func<FileInfo, bool> filePredicate,
            ITargetBlock<Tuple<AnnotatedPath, FileFingerprint>> joinedTargetBlock,
            CancellationToken cancellationToken)
        {
            await _fileFingerprintManager.LoadAsync(cancellationToken).ConfigureAwait(false);

            await GenerateFileFingerprintsAsync(paths, filePredicate, joinedTargetBlock, cancellationToken)
                .ConfigureAwait(false);
        }

        public Task ShutdownAsync(CancellationToken cancellationToken)
        {
            Debug.WriteLine("BlobManager.ShutdownAsync()");

            return _fileFingerprintManager.ShutdownAsync(cancellationToken);
        }

        async Task GenerateFileFingerprintsAsync(CollectionPath[] paths,
            Func<FileInfo, bool> filePredicate,
            ITargetBlock<Tuple<AnnotatedPath, FileFingerprint>> joinedTargetBlock,
            CancellationToken cancellationToken)
        {
            try
            {
                var annotatedPathBroadcastBlock = new BroadcastBlock<AnnotatedPath[]>(aps => aps,
                    new DataflowBlockOptions { CancellationToken = cancellationToken });

                var joiner = new LinkFingerprintJoiner(joinedTargetBlock);

                annotatedPathBroadcastBlock.LinkTo(joiner.AnnotatedPathsBlock, new DataflowLinkOptions { PropagateCompletion = true });

                var fileFingerprintBroadcastBlock = new BroadcastBlock<FileFingerprint>(ff => ff,
                    new DataflowBlockOptions { CancellationToken = cancellationToken });

                fileFingerprintBroadcastBlock.LinkTo(joiner.FileFingerprintBlock, new DataflowLinkOptions { PropagateCompletion = true });

                var fingerprintGeneratorTask = _fileFingerprintManager
                    .GenerateFileFingerprintsAsync(annotatedPathBroadcastBlock, fileFingerprintBroadcastBlock, cancellationToken);

                try
                {
                    await DirectoryScanner.GenerateAnnotatedPathsAsync(paths, filePredicate, annotatedPathBroadcastBlock, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                { }
                catch (Exception ex)
                {
                    Console.Write("Path scan failed: " + ex.Message);
                }

                await fingerprintGeneratorTask.ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Console.WriteLine("GenerateFileFingerprintsAsync() failed: " + ex.Message);
            }
            finally
            {
                Debug.WriteLine("BlobManager.GenerateFileFingerprintsAsync() is done");
            }
        }
    }
}
