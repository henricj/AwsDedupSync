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
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;

namespace AwsSyncer.AWS
{
    public class S3PutBase
    {
        protected readonly IAmazonS3 AmazonS3;

        protected S3PutBase(IAmazonS3 amazonS3)
        {
            AmazonS3 = amazonS3 ?? throw new ArgumentNullException(nameof(amazonS3));
        }

        protected async Task<PutObjectResponse> PutAsync(IS3PutRequest request, CancellationToken cancellationToken)
        {
            var response = await AmazonS3.PutObjectAsync(request.Request, cancellationToken).ConfigureAwait(false);

            if (response.HttpStatusCode != HttpStatusCode.OK)
                Debug.WriteLine("now what?");

            if (!string.IsNullOrEmpty(request.ETag) && !string.Equals(response.ETag, request.ETag, StringComparison.CurrentCultureIgnoreCase))
            {
                Debug.WriteLine($"Unexpected ETag mismatch: {response.ETag} != {request.ETag}");
            }

            return response;
        }

        protected interface IS3PutRequest
        {
            string ETag { get; }
            PutObjectRequest Request { get; }
        }

        protected class S3PutRequest : IS3PutRequest
        {
            public string ETag { get; set; }
            public PutObjectRequest Request { get; set; }
        }
    }
}
