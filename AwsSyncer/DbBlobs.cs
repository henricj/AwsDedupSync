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
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace AwsSyncer
{
    public class DbBlobs
    {
        readonly IAmazonDynamoDB _amazonDb;
        readonly IPathManager _pathManager;

        public DbBlobs(IAmazonDynamoDB amazonDb, IPathManager pathManager)
        {
            if (null == amazonDb)
                throw new ArgumentNullException(nameof(amazonDb));
            if (null == pathManager)
                throw new ArgumentNullException(nameof(pathManager));

            _amazonDb = amazonDb;
            _pathManager = pathManager;
        }

        public async Task AddPathAsync(IBlob blob, ILookup<string, string> namePaths)
        {
            var names = new Dictionary<string, string>
            {
                { "#l", "Length" },
                { "#url", "Url" },
                { "#ct", "Content-Type" },
                { "#sha2", "SHA2-256" },
                { "#sha3", "SHA3-512" }
            };

            var values = new Dictionary<string, AttributeValue>
            {
                { ":md5", new AttributeValue { B = new MemoryStream(blob.Fingerprint.Md5) } },
                { ":sha2", new AttributeValue { B = new MemoryStream(blob.Fingerprint.Sha2_256) } },
                { ":sha3", new AttributeValue { B = new MemoryStream(blob.Fingerprint.Sha3_512) } },
                { ":l", new AttributeValue { N = blob.Fingerprint.Size.ToString(CultureInfo.InvariantCulture) } },
                { ":ct", new AttributeValue { S = MimeDetector.Default.GetMimeType(blob.FullPath) } },
                { ":url", new AttributeValue { S = _pathManager.GetBlobUrl(blob).ToString() } }
            };

            var updateExpression = new StringBuilder("SET #sha3 = :sha3, #sha2 = :sha2, MD5 = :md5, #l = :l, #url = :url");

            var pathIndex = 0;
            foreach (var namePath in namePaths)
            {
                if (string.IsNullOrEmpty(namePath.Key))
                    continue;

                var paths = namePath.ToList();

                if (paths.Count < 1)
                    continue;

                ++pathIndex;

                var indexIdentifier = pathIndex.ToString(CultureInfo.InvariantCulture);

                var pathName = "#paths" + indexIdentifier;
                names[pathName] = '/' + namePath.Key;

                var valueName = ":p" + indexIdentifier;
                values[valueName] = new AttributeValue(paths);

                updateExpression.Append(pathIndex <= 1 ? " ADD " : ", ");

                updateExpression.Append(pathName + " " + valueName);
            }

            var request = new UpdateItemRequest
            {
                TableName = "Blobs",
                Key = new Dictionary<string, AttributeValue> { { "Key", new AttributeValue { S = blob.Key } } },
                ExpressionAttributeNames = names,
                ExpressionAttributeValues = values,
                UpdateExpression = updateExpression.ToString(),
                ConditionExpression = "(attribute_not_exists(#sha3) OR #sha3 = :sha3) "
                                      + "AND (attribute_not_exists(#sha2) OR #sha2 = :sha2) "
                                      + "AND (attribute_not_exists(MD5) OR MD5 = :md5) "
                                      + "AND (attribute_not_exists(#l) OR #l = :l) "
                                      + "AND (attribute_not_exists(#url) OR #url = :url) "
            };

            var response = await _amazonDb.UpdateItemAsync(request);

            if (response.HttpStatusCode != HttpStatusCode.OK)
                Debug.WriteLine("now what?");
        }
    }
}
