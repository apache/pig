/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * Copyright Felix Gessert and Florian Bücklers. All rights reserved. Permission is hereby granted,
 * free of charge, to any person obtaining a copy of this software and associated documentation files
 * (the "Software"), to deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the
 * Software, and to permit persons to whom the Software is furnished to do so, subject to the following
 * conditions:
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package org.apache.pig.impl.bloom;

import java.util.function.BiFunction;

/**
 * Taken from https://github.com/Baqend/Orestes-Bloomfilter
 */
public class HashProvider {

    public static int[] hashMurmur3(byte[] value, int m, int k) {
        return rejectionSample(HashProvider::murmur3_signed, value, m, k);
    }

    public static int[] hashKirschMitzenmacher(byte[] value, int m, int k) {
        int[] result = new int[k];
        long hash1 = murmur3(0, value);
        long hash2 = murmur3((int) hash1, value);
        for (int i = 0; i < k; i++) {
            result[i] = (int) ((hash1 + i * hash2) % m);
        }
        return result;
    }

    private static long murmur3(int seed, byte[] bytes) {
        return Integer.toUnsignedLong(murmur3_signed(seed, bytes));
    }

    private static int murmur3_signed(int seed, byte[] bytes) {
        int h1 = seed;
        //Standard in Guava
        int c1 = 0xcc9e2d51;
        int c2 = 0x1b873593;
        int len = bytes.length;
        int i = 0;

        while (len >= 4) {
            //process()
            int k1  = (bytes[i++] & 0xFF);
                k1 |= (bytes[i++] & 0xFF) << 8;
                k1 |= (bytes[i++] & 0xFF) << 16;
                k1 |= (bytes[i++] & 0xFF) << 24;

            k1 *= c1;
            k1 = Integer.rotateLeft(k1, 15);
            k1 *= c2;

            h1 ^= k1;
            h1 = Integer.rotateLeft(h1, 13);
            h1 = h1 * 5 + 0xe6546b64;

            len -= 4;
        }

        //processingRemaining()
        int k1 = 0;
        switch (len) {
            case 3:
                k1 ^= (bytes[i + 2] & 0xFF) << 16;
                // fall through
            case 2:
                k1 ^= (bytes[i + 1] & 0xFF) << 8;
                // fall through
            case 1:
                k1 ^= (bytes[i] & 0xFF);

                k1 *= c1;
                k1 = Integer.rotateLeft(k1, 15);
                k1 *= c2;
                h1 ^= k1;
        }
        i += len;

        //makeHash()
        h1 ^= i;

        h1 ^= h1 >>> 16;
        h1 *= 0x85ebca6b;
        h1 ^= h1 >>> 13;
        h1 *= 0xc2b2ae35;
        h1 ^= h1 >>> 16;

        return h1;
    }

    /**
     * Performs rejection sampling on a random 32bit Java int (sampled from Integer.MIN_VALUE to Integer.MAX_VALUE).
     *
     * @param random int
     * @param m     integer output range [1,size]
     * @return the number down-sampled to interval [0, size]. Or -1 if it has to be rejected.
     */
    private static int rejectionSample(int random, int m) {
        random = Math.abs(random);
        if (random > (2147483647 - 2147483647 % m)
                || random == Integer.MIN_VALUE)
            return -1;
        else
            return random % m;
    }

    private static int[] rejectionSample(BiFunction<Integer, byte[], Integer> hashFunction, byte[] value, int m, int k) {
        int[] hashes = new int[k];
        int seed = 0;
        int pos = 0;
        while (pos < k) {
            seed = hashFunction.apply(seed, value);
            int hash = rejectionSample(seed, m);
            if (hash != -1) {
                hashes[pos++] = hash;
            }
        }
        return hashes;
    }

}
