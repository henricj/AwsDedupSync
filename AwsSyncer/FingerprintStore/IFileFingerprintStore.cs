using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using AwsSyncer.Types;

namespace AwsSyncer.FingerprintStore
{
    public interface IFileFingerprintStore : IDisposable
    {
        int UpdateCount { get; }
        long UpdateSize { get; }
        Task<IReadOnlyDictionary<string, FileFingerprint>> LoadBlobsAsync(CancellationToken cancellationToken);
        Task FlushAsync(CancellationToken cancellationToken);
        Task StoreBlobsAsync(ICollection<FileFingerprint> fileFingerprints, CancellationToken cancellationToken);
    }
}
