/**
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

package org.apache.hadoop.zebra.mapred;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.zebra.tfile.RandomDistribution.Binomial;
import org.apache.hadoop.zebra.tfile.RandomDistribution.DiscreteRNG;
import org.apache.hadoop.zebra.tfile.RandomDistribution.Zipf;

/**
 * A dictionary that generates English words, whose frequency follows Zipf
 * distributions, and length follows Binomial distribution.
 */
class Dictionary {
  private static final double BINOMIAL_P = 0.3;
  private static final double SIGMA = 1.1;
  private final int lead;
  private final Zipf zipf;
  private final String[] dict;
  private final long[] wordCnts;

  private static String makeWord(DiscreteRNG rng, Random random) {
    int len = rng.nextInt();
    StringBuilder sb = new StringBuilder(len);
    for (int i = 0; i < len; ++i) {
      sb.append((char) ('a' + random.nextInt(26)));
    }
    return sb.toString();
  }

  /**
   * Constructor
   * 
   * @param entries
   *          How many words exist in the dictionary.
   * @param minWordLen
   *          Minimum word length.
   * @param maxWordLen
   *          Maximum word length.
   * @param freqRatio
   *          Expected ratio between the most frequent words and the least
   *          frequent words. (e.g. 100)
   */
  public Dictionary(Random random, int entries, int minWordLen, int maxWordLen,
      int freqRatio) {
    Binomial binomial = new Binomial(random, minWordLen, maxWordLen, BINOMIAL_P);
    lead = Math.max(0,
        (int) (entries / (Math.exp(Math.log(freqRatio) / SIGMA) - 1)) - 1);
    zipf = new Zipf(random, lead, entries + lead, 1.1);
    dict = new String[entries];
    // Use a set to ensure no dup words in dictionary
    Set<String> dictTmp = new HashSet<String>();
    for (int i = 0; i < entries; ++i) {
      while (true) {
        String word = makeWord(binomial, random);
        if (!dictTmp.contains(word)) {
          dictTmp.add(word);
          dict[i] = word;
          break;
        }
      }
    }
    wordCnts = new long[dict.length];
  }

  /**
   * Get the next word from the dictionary.
   * 
   * @return The next word from the dictionary.
   */
  public String nextWord() {
    int index = zipf.nextInt() - lead;
    ++wordCnts[index];
    return dict[index];
  }

  public void resetWordCnts() {
    for (int i = 0; i < wordCnts.length; ++i) {
      wordCnts[i] = 0;
    }
  }

  public Map<String, Long> getWordCounts() {
    Map<String, Long> ret = new HashMap<String, Long>();
    for (int i = 0; i < dict.length; ++i) {
      if (wordCnts[i] > 0) {
        ret.put(dict[i], wordCnts[i]);
      }
    }
    return ret;
  }
}
