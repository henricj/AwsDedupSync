// Copyright (c) 2014 Henric Jungheim <software@henric.org>
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
using System.Configuration;
using System.Diagnostics;

namespace AwsDedupSync
{
    static class Program
    {
        static void Main(string[] args)
        {
            //ServicePointManager.DefaultConnectionLimit = 20;

            var bucket = ConfigurationManager.AppSettings["Bucket"];

            if (string.IsNullOrWhiteSpace(bucket))
            {
                Console.WriteLine("No bucket name found in the application settings");
                return;
            }

            var sw = new Stopwatch();

            try
            {
                var syncer = new S3PathSyncer();

                sw.Start();

                ConsoleCancel.RunAsync(ct => syncer.SyncPathsAsync(bucket, args, ct), TimeSpan.Zero).Wait();

                sw.Stop();
            }
            catch (Exception ex)
            {
                sw.Stop();

                Console.WriteLine(ex.Message);
            }

            var process = Process.GetCurrentProcess();

            Console.WriteLine("Elapsed: {0} CPU {1} User {2}", sw.Elapsed, process.TotalProcessorTime, process.UserProcessorTime);
        }
    }
}
