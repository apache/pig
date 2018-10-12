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

package org.apache.pig.impl.bloom;

import org.apache.hadoop.util.bloom.Key;

public class HashFunction {
      /** The number of hashed values. */
      protected int nbHash;
      /** The maximum highest returned value. */
      protected int maxValue;
      /** Hashing algorithm to use. */
      protected Hash hashAlgorithm;
      /**
       * Constructor.
       * <p>
       * Builds a hash function that must obey to a given maximum number of returned values and a highest value.
       * @param maxValue The maximum highest returned value.
       * @param nbHash The number of resulting hashed values.
       * @param hashAlgorithm type of the hashing algorithm (see {@link Hash}).
       */
      public HashFunction(int maxValue, int nbHash, int hashAlgorithm) {
          if (maxValue <= 0) {
              throw new IllegalArgumentException("maxValue must be > 0");
          }
          if (nbHash <= 0) {
              throw new IllegalArgumentException("nbHash must be > 0");
          }

          this.maxValue = maxValue;
          this.nbHash = nbHash;
          this.hashAlgorithm = Hash.getInstance(hashAlgorithm);
      }

      /** Clears <i>this</i> hash function. A NOOP */
      public void clear() {
      }

      /**
       * Hashes a specified key into several integers.
       * @param k The specified key.
       * @return The array of hashed values.
       */
      public int[] hash(Key k){
          byte[] b = k.getBytes();
          if (b == null) {
              throw new NullPointerException("buffer reference is null");
          }
          if (b.length == 0) {
              throw new IllegalArgumentException("key length must be > 0");
          }
          return hashAlgorithm.hash(b, maxValue, nbHash);
       }
}