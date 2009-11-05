/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.zebra.io;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.file.tfile.RawComparable;
import org.apache.hadoop.io.file.tfile.ByteArray;

/**
 * Class used to convey the information of how on-disk data are distributed
 * among key-partitioned buckets. This class is used by the MapReduce layer to
 * calculate intelligent splits.
 */
public class KeyDistribution {
  private long uniqueBytes;
  private SortedMap<RawComparable, BlockDistribution> data;

  KeyDistribution(Comparator<? super RawComparable> comparator) {
    data = new TreeMap<RawComparable, BlockDistribution>(comparator);
  }

  void add(RawComparable key, BlockDistribution bucket) {
    uniqueBytes += bucket.getLength();
    data.put(key, BlockDistribution.sum(data.get(key), bucket));
  }

  void add(KeyDistribution other) {
    this.uniqueBytes += other.uniqueBytes;
    reduceKeyDistri(this.data, other.data);
  }

  static void reduceKeyDistri(SortedMap<RawComparable, BlockDistribution> lv,
      SortedMap<RawComparable, BlockDistribution> rv) {
    for (Iterator<Map.Entry<RawComparable, BlockDistribution>> it =
        rv.entrySet().iterator(); it.hasNext();) {
      Map.Entry<RawComparable, BlockDistribution> e = it.next();
      RawComparable key = e.getKey();
      BlockDistribution sum = lv.get(key);
      BlockDistribution delta = e.getValue();
      lv.put(key, BlockDistribution.sum(sum, delta));
    }
  }

  /**
   * Aggregate two key distributions.
   * 
   * @param a
   *          first key distribution (can be null)
   * @param b
   *          second key distribution (can be null)
   * @return the aggregated key distribution.
   */
  public static KeyDistribution sum(KeyDistribution a, KeyDistribution b) {
    if (a == null) return b;
    if (b == null) return a;
    a.add(b);
    return a;
  }
  
  /**
   * Get the size of the key sampling.
   * 
   * @return Number of key samples.
   */
  public int size() {
    return data.size();
  }

  /**
   * Get the total unique bytes contained in the key-partitioned buckets.
   * 
   * @return The total number of bytes contained in the key-partitioned buckets.
   */
  public long length() {
    return uniqueBytes;
  }

  /**
   * Resize the key samples
   * 
   * @param n
   *          targeted sampling size
   * @return the actual size after the resize().
   */
  public int resize(int n) {
    Iterator<Map.Entry<RawComparable, BlockDistribution>> it =
        data.entrySet().iterator();
    KeyDistribution adjusted = new KeyDistribution(data.comparator());
    for (int i = 0; i < n; ++i) {
      long targetMarker = (i + 1) * uniqueBytes / n;
      if (adjusted.uniqueBytes >= targetMarker) {
        continue;
      }
      RawComparable key = null;
      do {
        Map.Entry<RawComparable, BlockDistribution> e = it.next();
        if (key == null) {
          key = e.getKey();
        }
        adjusted.add(key, e.getValue());
      }
      while (adjusted.uniqueBytes < targetMarker);
    }

    swap(adjusted);
    return data.size();
  }
  
  void swap(KeyDistribution other) {
    long tmp = uniqueBytes;
    uniqueBytes = other.uniqueBytes;
    other.uniqueBytes = tmp;
    SortedMap<RawComparable, BlockDistribution> tmp2 = data;
    data = other.data;
    other.data = tmp2;
  }
  
  /**
   * Get the list of sampling keys.
   * 
   * @return A list of sampling keys.
   */
  public RawComparable[] getKeys() {
    RawComparable[] ret = new RawComparable[data.size()];
    return data.keySet().toArray(ret);

  }

  /**
   * Get the block distribution of all data that maps to the key bucket.
   */
  public BlockDistribution getBlockDistribution(BytesWritable key) {
    ByteArray key0 = new ByteArray(key.get(), 0, key.getSize());
    BlockDistribution bInfo = data.get(key0);
    if (bInfo == null) {
      throw new IllegalArgumentException("Invalid key");
    }
    return bInfo;
  }
}
