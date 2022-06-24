using AwsSyncer.Types;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace AwsSyncer.FingerprintStore
{
    public interface IFileFingerprintStore : IDisposable
    {
        int UpdateCount { get; }
        long UpdateSize { get; }
        Task<IReadOnlyDictionary<string, FileFingerprint>> LoadBlobsAsync(CancellationToken cancellationToken);
        Task CloseAsync(CancellationToken cancellationToken);
        Task StoreBlobsAsync(ICollection<FileFingerprint> fileFingerprints, CancellationToken cancellationToken);
    }
}
