// Copyright (c) 2016 Henric Jungheim <software@henric.org>
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
using System.Security.Cryptography;
using System.Threading;

namespace AwsSyncer
{
    public static class RandomUtil
    {
        static readonly ThreadLocal<Random> LocalRandom = new ThreadLocal<Random>(CreateRandom);

        public static Random ThreadLocalRandom => LocalRandom.Value;

        public static Random CreateRandom()
        {
            using (var rng = RandomNumberGenerator.Create())
            {
                var buffer = new byte[4];

                rng.GetBytes(buffer);

                return new Random(BitConverter.ToInt32(buffer, 0));
            }
        }

        /// <summary>
        ///     Fisher-Yates shuffle
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="random"></param>
        /// <param name="list"></param>
        public static void Shuffle<T>(this Random random, IList<T> list)
        {
            for (var i = list.Count - 1; i >= 1; --i)
            {
                var j = random.Next(i + 1);

                var tmp = list[i];
                list[i] = list[j];
                list[j] = tmp;
            }
        }

        /// <summary>
        ///     Fisher-Yates shuffle
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="list"></param>
        public static void Shuffle<T>(IList<T> list)
        {
            LocalRandom.Value.Shuffle(list);
        }
    }
}
