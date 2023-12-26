// Copyright (c) 2012-2017 Henric Jungheim <software@henric.org>
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
using System.Linq;
using System.Threading.Tasks;

namespace AwsSyncer.Utility;

public class TaskCollector
{
    public static readonly TaskCollector Default = new();
    readonly object _lock = new();
    readonly Dictionary<Task, string> _tasks = [];

    //[Conditional("DEBUG")]
    public void Add(Task task, string description)
    {
        if (task.IsCompleted)
        {
            ReportException(task, description);

            return;
        }

        lock (_lock)
        {
            Debug.Assert(!_tasks.ContainsKey(task));
            _tasks[task] = description;
        }

        task.ContinueWith(Cleanup);
    }

    public async Task WaitAsync()
    {
        KeyValuePair<Task, string>[] tasks = null;

        lock (_lock)
        {
            if (_tasks.Count > 0)
                tasks = [.. _tasks];
        }

        if (null == tasks || 0 == tasks.Length)
            return;

        try
        {
            await Task.WhenAll(tasks.Select(t => t.Key)).ConfigureAwait(false);
        }
        catch (AggregateException ex)
        {
            foreach (var e in ex.InnerExceptions)
                Debug.WriteLine("TaskCollector.WaitAsync() Task wait failed: " + e.Message);
        }
        catch (Exception ex)
        {
            Debug.WriteLine("TaskCollector.WaitAsync() Task wait failed: " + ex.Message);
        }
    }

    void Cleanup(Task task)
    {
        Debug.Assert(task.IsCompleted);

        var wasRemoved = false;
        string description;

        lock (_lock)
        {
            if (_tasks.TryGetValue(task, out description))
                wasRemoved = _tasks.Remove(task);
        }

        Debug.Assert(wasRemoved, description ?? "No description");

        ReportException(task, description);
    }

    static void ReportException(Task task, string description)
    {
        try
        {
            var ex = task.Exception;

            if (null != ex)
            {
                Debug.WriteLine("TaskCollector.Cleanup() task {0} failed: {1}{2}{3}", description, ex, Environment.NewLine,
                    ex.StackTrace);

                if (Debugger.IsAttached)
                    Debugger.Break();
            }
        }
        catch (Exception ex2)
        {
            Debug.WriteLine("TaskCollector.Cleanup() cleanup of task {0} failed: {1}", description, ex2);

            if (Debugger.IsAttached)
                Debugger.Break();
        }
    }
}
