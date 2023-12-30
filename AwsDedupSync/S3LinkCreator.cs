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
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using AwsSyncer;
using AwsSyncer.AWS;
using AwsSyncer.Types;

namespace AwsDedupSync;

public class S3LinkCreator(S3Settings s3Settings)
{
    readonly S3Settings _s3Settings = s3Settings;

    public async Task UpdateLinksAsync(IAwsManager awsManager,
        ISourceBlock<(AnnotatedPath path, FileFingerprint fingerprint)> linkBlobs,
        CancellationToken cancellationToken)
    {
        try
        {
            await LinkManager.CreateLinksAsync(awsManager, linkBlobs, _s3Settings.ActuallyWrite, cancellationToken)
                .ConfigureAwait(false);

            Debug.WriteLine("Done processing links");
        }
        catch (Exception ex)
        {
            Debug.WriteLine("Processing links failed: " + ex.Message);
        }
    }
}
