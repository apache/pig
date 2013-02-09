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

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.fs.BlockLocation;

/**
 * Class used to convey the information of how on-disk data that fall in a
 * specific split are distributed across hosts. This class is used by the
 * MapReduce layer to calculate intelligent splits.
 * 
 * @see BasicTable.Reader#getBlockDistribution(BasicTable.Reader.RangeSplit)
 * @see KeyDistribution#getBlockDistribution(BytesWritable)
 */
public class BlockDistribution {
  private long uniqueBytes;
  private Map<String, Long> dataDistri; // map from host names to bytes.

  public BlockDistribution() {
    dataDistri = new HashMap<String, Long>();
  }
  
  void add(long bytes, Map<String, Long> distri) {
    this.uniqueBytes += bytes;
    reduceDataDistri(dataDistri, distri);
  }

  static void reduceDataDistri(Map<String, Long> lv, Map<String, Long> rv) {
    for (Iterator<Map.Entry<String, Long>> it = rv.entrySet().iterator(); it
        .hasNext();) {
      Map.Entry<String, Long> e = it.next();
      String key = e.getKey();
      Long sum = lv.get(key);
      Long delta = e.getValue();
      lv.put(key, (sum == null) ? delta : sum + delta);
    }
  }
  
  void add(BlockLocation blkLocation) throws IOException {
    long blkLen = blkLocation.getLength();
    Map<String, Long> tmp = new HashMap<String, Long>();
    for (String host : blkLocation.getHosts()) {
      tmp.put(host, blkLen);
    }
    add(blkLen, tmp);
  }

  /**
   * Add a partial block.
   * 
   * @param blkLocation
   * @param length
   * @throws IOException
   */
  void add(BlockLocation blkLocation, long length) throws IOException {
    Map<String, Long> tmp = new HashMap<String, Long>();
    for (String host : blkLocation.getHosts()) {
      tmp.put(host, length);
    }
    add(length, tmp);
  }

  /**
   * Add another block distribution to this one.
   * 
   * @param other
   *          The other block distribution.
   */
  public void add(BlockDistribution other) {
    add(other.uniqueBytes, other.dataDistri);
  }

  /**
   * Sum up two block distributions together.
   * 
   * @param a
   *          first block distribution
   * @param b
   *          second block distribution
   * @return aggregated block distribution. The input objects may no longer be
   *         held.
   */
  public static BlockDistribution sum(BlockDistribution a, BlockDistribution b) {
    if (a == null) return b;
    if (b == null) return a;
    a.add(b);
    return a;
  }
  
  /**
   * Get the total number of bytes of all the blocks.
   * 
   * @return total number of bytes for the blocks.
   */
  public long getLength() {
    return uniqueBytes;
  }

  /**
   * Get up to n hosts that own the most bytes.
   * 
   * @param n
   *          targeted number of hosts.
   * @return A list of host names (up to n).
   */
  @SuppressWarnings("unchecked")
  public String[] getHosts(int n) {
    Set<Map.Entry<String, Long>> entrySet = dataDistri.entrySet();
    Map.Entry<String, Long>[] hostSize =
        entrySet.toArray(new Map.Entry[entrySet.size()]);
    Arrays.sort(hostSize, new Comparator<Map.Entry<String, Long>>() {

      @Override
      public int compare(Entry<String, Long> o1, Entry<String, Long> o2) {
        long diff = o1.getValue() - o2.getValue();
        if (diff < 0) return 1;
        if (diff > 0) return -1;
        return 0;
      }
    });
    int nHost = Math.min(hostSize.length, n);
    String[] ret = new String[nHost];
    for (int i = 0; i < nHost; ++i) {
      ret[i] = hostSize[i].getKey();
    }
    return ret;
  }
}
