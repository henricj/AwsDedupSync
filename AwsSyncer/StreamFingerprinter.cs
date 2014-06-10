using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
//using HashLib;

namespace AwsSyncer
{
    public class StreamFingerprinter
    {
        static readonly ICollection<byte[]> TestVectors = new[] { new byte[0], new byte[] { 0xcc }, new byte[] { 0x1F, 0x87, 0x7C }, Encoding.ASCII.GetBytes("abc") };

        //public static void Doodle()
        //{
        //    var sha3 = HashFactory.Crypto.SHA3.CreateKeccak512();

        //    foreach (var tv in TestVectors)
        //    {
        //        sha3.Initialize();

        //        sha3.TransformBytes(tv);

        //        var result = sha3.TransformFinal();

        //        Console.WriteLine(BitConverter.ToString(result.GetBytes()));
        //    }
        //}

        public async Task<IBlobFingerprint> GetFingerprintAsync(Stream stream)
        {
            using (var sha3 = new KeccakOpt.Keccak())
            using (var sha256 = SHA256.Create())
            using (var md5 = MD5.Create())
            {
                //var sha3 = HashFactory.Crypto.SHA3.CreateKeccak512();

                //sha3.Initialize();

                //var sha3 = new KeccakOpt.Keccak();

                var buffer = new byte[32 * 1024];

                var totalLength = 0L;

                for (; ; )
                {
                    var length = await stream.ReadAsync(buffer, 0, buffer.Length).ConfigureAwait(false);

                    if (length < 1)
                        break;

                    totalLength += length;

                    sha3.TransformBlock(buffer, 0, length, null, 0);
                    //sha3.TransformBytes(buffer, 0, length);

                    sha256.TransformBlock(buffer, 0, length, null, 0);

                    md5.TransformBlock(buffer, 0, length, null, 0);
                }

                sha3.TransformFinalBlock(buffer, 0, 0);

                sha256.TransformFinalBlock(buffer, 0, 0);

                md5.TransformFinalBlock(buffer, 0, 0);

                //var sha3Result = sha3.TransformFinal();

                var fingerprint = new BlobFingerprint(totalLength, sha3.Hash, sha256.Hash, md5.Hash);

                return fingerprint;
            }
        }
    }
}
