using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using AwsSyncer.Types;

namespace AwsSyncer.FingerprintStore;

public interface IFileFingerprintStore : IAsyncDisposable
{
    int UpdateCount { get; }
    long UpdateSize { get; }
    Task<IReadOnlyDictionary<string, FileFingerprint>> LoadBlobsAsync(CancellationToken cancellationToken);
    Task CloseAsync(CancellationToken cancellationToken);
    Task StoreBlobsAsync(IReadOnlyCollection<FileFingerprint> fileFingerprints, CancellationToken cancellationToken);
}
