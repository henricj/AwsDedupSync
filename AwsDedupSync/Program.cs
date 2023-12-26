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
using System.Collections.Frozen;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using AwsDedupSync;
using AwsSyncer.Utility;
using Microsoft.Extensions.Configuration;

if (ServicePointManager.DefaultConnectionLimit < 30)
    ServicePointManager.DefaultConnectionLimit = 30;

Console.Out.Close();

await using var writer = new StreamWriter(Console.OpenStandardOutput(), new UTF8Encoding(false), 4 * 4096);

using var cts = new CancellationTokenSource();

var cancellationToken = cts.Token;

var flushTask = Task.Run(async () =>
{
    try
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            await Task.Delay(1000, cancellationToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);

            if (cancellationToken.IsCancellationRequested)
                break;

            await Console.Out.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
    }
    catch (OperationCanceledException ex) when (ex.CancellationToken == cancellationToken)
    { }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"Flush failed with {ex.Message}");
    }
});

Console.SetOut(writer);

var syncRunner = Sync.CreateSyncRunner(args);

var sw = new Stopwatch();

try
{
    sw.Start();

    await ConsoleCancel.RunAsync(syncRunner, TimeSpan.Zero);

    sw.Stop();
}
catch (Exception ex)
{
    sw.Stop();

    Console.WriteLine(ex.Message);
}

await cts.CancelAsync().ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);

await flushTask.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);

var process = Process.GetCurrentProcess();

Console.WriteLine("Elapsed {0} CPU {1} User {2} Priv {3}",
    sw.Elapsed, process.TotalProcessorTime, process.UserProcessorTime, process.PrivilegedProcessorTime);
Console.WriteLine("Peak Memory: Virtual {0:F1} GiB Paged {1:F1} MiB Working {2:F1} MiB",
    process.PeakVirtualMemorySize64.BytesToGiB(), process.PeakPagedMemorySize64.BytesToMiB(),
    process.PeakWorkingSet64.BytesToMiB());

static class Sync
{
    static readonly FrozenSet<string> ExcludeFiles =
        new[] { "desktop.ini", "Thumbs.db" }.ToFrozenSet(StringComparer.OrdinalIgnoreCase);

    public static Func<CancellationToken, Task> CreateSyncRunner(string[] paths)
    {
        var builder = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", false);

        var config = builder.Build();

        var syncer = new S3PathSyncer();

        return ct => syncer.SyncPathsAsync(config, paths,
            fi => !ExcludeFiles.Contains(fi.Name), ct);
    }
}
