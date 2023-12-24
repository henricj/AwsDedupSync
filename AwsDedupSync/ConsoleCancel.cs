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
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using AwsSyncer.Utility;

namespace AwsDedupSync;

public static class ConsoleCancel
{
    public static async Task RunAsync(Func<CancellationToken, Task> runAsync, TimeSpan timeout)
    {
        var sw = Stopwatch.StartNew();

        var cts = timeout > TimeSpan.Zero ? new(timeout) : new CancellationTokenSource();

        Console.WriteLine("Press enter to cancel");
        await Console.Out.FlushAsync().ConfigureAwait(false);

        try
        {
            var runTask = runAsync(cts.Token);

            // It would be nice if Console.In.ReadLineAsync() didn't block...
            var readTask = Task.Run(Console.ReadLine, cts.Token);

            var anyTask = await Task.WhenAny(readTask, runTask).ConfigureAwait(false);

            TaskCollector.Default.Add(anyTask, "ConsoleCancel");

            if (!runTask.IsCompleted)
            {
                Console.WriteLine("Cancel requested");
                await Console.Out.FlushAsync().ConfigureAwait(false);
                cts.Cancel();
            }

            // Wait for runTask to complete and/or observe any exceptions.
            await runTask.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // This is normal...
        }
        catch (Exception ex)
        {
            Console.WriteLine("Task failed: " + ex.Message);
        }
        finally
        {
            sw.Stop();
        }

        Console.WriteLine("Done ({0}) {1}", cts.Token.IsCancellationRequested ? "cancelled" : "not cancelled", sw.Elapsed);
        await Console.Out.FlushAsync().ConfigureAwait(false);
    }
}
