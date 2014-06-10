using System.IO;
using System.Security.Cryptography;
using System.Threading.Tasks;
using KeccakOpt;

namespace AwsSyncer
{
    public class StreamFingerprinter
    {
        public async Task<IBlobFingerprint> GetFingerprintAsync(Stream stream)
        {
            using (var sha3 = new Keccak())
            using (var sha256 = SHA256.Create())
            using (var md5 = MD5.Create())
            {
                var buffer = new byte[32 * 1024];

                var totalLength = 0L;

                for (; ; )
                {
                    var length = await stream.ReadAsync(buffer, 0, buffer.Length).ConfigureAwait(false);

                    if (length < 1)
                        break;

                    totalLength += length;

                    sha3.TransformBlock(buffer, 0, length, null, 0);

                    sha256.TransformBlock(buffer, 0, length, null, 0);

                    md5.TransformBlock(buffer, 0, length, null, 0);
                }

                sha3.TransformFinalBlock(buffer, 0, 0);

                sha256.TransformFinalBlock(buffer, 0, 0);

                md5.TransformFinalBlock(buffer, 0, 0);

                var fingerprint = new BlobFingerprint(totalLength, sha3.Hash, sha256.Hash, md5.Hash);

                return fingerprint;
            }
        }
    }
}
