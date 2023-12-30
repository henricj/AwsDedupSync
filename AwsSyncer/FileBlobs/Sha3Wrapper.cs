using System;
using System.Security.Cryptography;
using KeccakOpt;

namespace AwsSyncer.FileBlobs;

public sealed class Sha3Wrapper : IDisposable
{
    readonly Keccak _keccak;
    readonly IncrementalHash _sha3;

    public Sha3Wrapper()
    {
        if (SHA3_512.IsSupported)
        {
            _sha3 = IncrementalHash.CreateHash(HashAlgorithmName.SHA3_512);
        }
        else
        {
            _keccak = new();
        }
    }

    public void Dispose()
    {
        _sha3?.Dispose();
        _keccak?.Dispose();
    }

    public void Initialize()
    {
        if (_sha3 is not null)
        {
            Span<byte> hash = stackalloc byte[_sha3.HashLengthInBytes];
            _sha3.TryGetHashAndReset(hash, out _);
        }
        else
            _keccak?.Initialize();
    }

    public void AppendData(ReadOnlySpan<byte> data)
    {
        if (_sha3 is not null)
        {
            _sha3.AppendData(data);
            return;
        }

        unsafe
        {
            fixed (byte* p = data)
            {
                _keccak.AppendData(p, data.Length);
            }
        }
    }

    public void GetHashAndReset(Span<byte> destination)
    {
        if (_sha3 is not null)
        {
            _sha3.TryGetHashAndReset(destination, out _);
            return;
        }

        unsafe
        {
            fixed (byte* p = destination)
            {
                _keccak.GetHashAndReset(p, destination.Length);
            }
        }
    }
}
