﻿// Copyright (c) 2012-2017 Henric Jungheim <software@henric.org>
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
using System.Threading;
using System.Threading.Tasks;

namespace AwsSyncer.Utility;

public sealed class AsyncLock : IDisposable
{
    readonly object _lock = new();
    bool _isLocked;
    Queue<TaskCompletionSource<IDisposable>> _pending = new();

    #region IDisposable Members

    public void Dispose()
    {
        TaskCompletionSource<IDisposable>[] pending;

        lock (_lock)
        {
            if (null == _pending)
                return;

            CheckInvariant();

            _isLocked = true;

            if (0 == _pending.Count)
            {
                _pending = null;
                return;
            }

            pending = [.. _pending];
            _pending.Clear();

            _pending = null;
        }

        foreach (var tcs in pending)
            tcs.TrySetCanceled();
    }

    #endregion

    [Conditional("DEBUG")]
    void CheckInvariant() =>
        Debug.Assert(_isLocked || (null != _pending && 0 == _pending.Count),
            "Either we are locked or we have an empty queue");

    public IDisposable TryLock()
    {
        ThrowIfDisposed();

        lock (_lock)
        {
            CheckInvariant();

            if (_isLocked)
                return null;

            _isLocked = true;

            return new Releaser(this);
        }
    }

    public Task<IDisposable> LockAsync(CancellationToken cancellationToken)
    {
        ThrowIfDisposed();

        cancellationToken.ThrowIfCancellationRequested();

        TaskCompletionSource<IDisposable> tcs;

        lock (_lock)
        {
            CheckInvariant();

            if (!_isLocked)
            {
                _isLocked = true;

                return Task.FromResult<IDisposable>(new Releaser(this));
            }

            tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

            _pending.Enqueue(tcs);
        }

        if (!cancellationToken.CanBeCanceled)
            return tcs.Task;

        return CancellableTaskAsync(tcs, cancellationToken);
    }

    async Task<IDisposable> CancellableTaskAsync(TaskCompletionSource<IDisposable> tcs, CancellationToken cancellationToken)
    {
        var registration = cancellationToken.Register(
            () =>
            {
                bool wasPending;

                lock (_lock)
                {
                    CheckInvariant();

                    wasPending = _pending.Remove(tcs);
                }

                if (wasPending)
                    tcs.TrySetCanceled();
            });

        await using (registration.ConfigureAwait(false))
        {
            return await tcs.Task.ConfigureAwait(false);
        }
    }

    void Release()
    {
        for (;;)
        {
            TaskCompletionSource<IDisposable> tcs;

            lock (_lock)
            {
                CheckInvariant();

                Debug.Assert(_isLocked, "AsyncLock.Release() was unlocked");

                if (null == _pending)
                    return; // We have been disposed (so stay locked)

                if (0 == _pending.Count)
                {
                    _isLocked = false;
                    return;
                }

                tcs = _pending.Dequeue();
            }

            if (tcs.TrySetResult(new Releaser(this)))
                return;
        }
    }

    void ThrowIfDisposed()
    {
        if (null != _pending)
            return;

        throw new ObjectDisposedException(GetType().Name);
    }

    #region Nested type: Releaser

    sealed class Releaser : IDisposable
    {
        AsyncLock _asyncLock;

        public Releaser(AsyncLock asynclock) => _asyncLock = asynclock;

        #region IDisposable Members

        public void Dispose()
        {
            var asyncLock = Interlocked.Exchange(ref _asyncLock, null);

            asyncLock?.Release();
        }

        #endregion
    }

    #endregion
}
