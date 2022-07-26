// Copyright (c) 2014-2017 Henric Jungheim <software@henric.org>
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
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using AwsSyncer.Utility;
using Microsoft.Extensions.Configuration;

namespace AwsDedupSync
{
    static class Program
    {
        static readonly Encoding Utf8NoBom = new UTF8Encoding(false);
        static readonly string[] ExcludeFiles = { "desktop.ini", "Thumbs.db" };

        static Func<CancellationToken, Task> CreateSyncRunner(string[] paths)
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", false);

            var config = builder.Build();

            var syncer = new S3PathSyncer();

            return ct => syncer.SyncPathsAsync(config, paths,
                fi => !ExcludeFiles.Contains(fi.Name, StringComparer.OrdinalIgnoreCase), ct);
        }

        static void Main(string[] args)
        {
            if (ServicePointManager.DefaultConnectionLimit < 30)
                ServicePointManager.DefaultConnectionLimit = 30;

            Console.Out.Close();

            using var writer = new StreamWriter(Console.OpenStandardOutput(), Utf8NoBom, 4 * 4096);
            using var _ = new Timer(_ => TaskCollector.Default.Add(Console.Out.FlushAsync(), "Console Flush"), null, 1000, 1000);

            Console.SetOut(writer);

            var syncRunner = CreateSyncRunner(args);

            var sw = new Stopwatch();

            try
            {
                sw.Start();

                ConsoleCancel.RunAsync(syncRunner, TimeSpan.Zero).GetAwaiter().GetResult();

                sw.Stop();
            }
            catch (Exception ex)
            {
                sw.Stop();

                Console.WriteLine(ex.Message);
            }

            TaskCollector.Default.Wait();

            var process = Process.GetCurrentProcess();

            Console.WriteLine("Elapsed {0} CPU {1} User {2} Priv {3}",
                sw.Elapsed, process.TotalProcessorTime, process.UserProcessorTime, process.PrivilegedProcessorTime);
            Console.WriteLine("Peak Memory: Virtual {0:F1}GiB Paged {1:F1}MiB Working {2:F1}MiB",
                process.PeakVirtualMemorySize64.BytesToGiB(), process.PeakPagedMemorySize64.BytesToMiB(),
                process.PeakWorkingSet64.BytesToMiB());
        }
    }
}
