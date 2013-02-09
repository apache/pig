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
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.zebra.tfile.RawComparable;
import org.apache.hadoop.zebra.tfile.ByteArray;

/**
 * Class used to convey the information of how on-disk data are distributed
 * among key-partitioned buckets. This class is used by the MapReduce layer to
 * calculate intelligent splits.
 */
public class KeyDistribution {
  private long uniqueBytes;
  private long minStepSize = -1;
  private SortedMap<RawComparable, BlockDistribution> data;

  KeyDistribution(Comparator<? super RawComparable> comparator) {
    data = new TreeMap<RawComparable, BlockDistribution>(comparator);
  }

  void add(RawComparable key) {
    data.put(key, null);
  }
  
  void add(RawComparable key, BlockDistribution bucket)
  {
    uniqueBytes += bucket.getLength();
    data.put(key, BlockDistribution.sum(data.get(key), bucket));
  }
  
  void setMinStepSize(long minStepSize)
  {
    this.minStepSize = minStepSize;
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
   * Get the size of the key sampling.
   * 
   * @return Number of key samples.
   */
  public int size() {
    return data.size();
  }

  /**
   * Get the minimum split step size from all tables in union
   */
  public long getMinStepSize() {
    return minStepSize;
  }
 
  /** Get the list of sampling keys
   * 
   * @return A list of sampling keys
   */
  public RawComparable[] getKeys() {
    RawComparable[] ret = new RawComparable[data.size()];
    return data.keySet().toArray(ret);
  }
  
  public BlockDistribution getBlockDistribution(RawComparable key) {
    return data.get(key);
  }
  
  /**
   * Merge the key samples
   * 
   * Algorithm: select the smallest key from all clean source ranges and ranges subsequent to
   *            respective dirty ranges. A dirty range is a range that has been partially needed
   *            by one or more of the previous final ranges.  
   *
   * @param sourceKeys
   *          key samples to be merged
   * @return the merged key samples
   */
  public static KeyDistribution merge(KeyDistribution[] sourceKeys) throws IOException {
    if (sourceKeys == null || sourceKeys.length == 0)
      return null;
    int srcSize = sourceKeys.length;
    if (srcSize == 1)
      return sourceKeys[0];
    
    Comparator<? super RawComparable> comp = sourceKeys[0].data.comparator();
    // TODO check the identical comparators used in the source keys
    /*
    for (int i = 1; i < srcSize; i++)
      if (!comp.equals(sourceKeys[i].data.comparator()))
        throw new IOException("Incompatible sort keys found:" + comp.toString() + " vs. "+ sourceKeys[i].data.comparator().toString());
     */
    
    KeyDistribution result = new KeyDistribution(comp);
    
    result.minStepSize = sourceKeys[0].minStepSize;
    for (int i = 1; i < srcSize; i++)
      if (result.minStepSize > sourceKeys[i].minStepSize)
        result.minStepSize = sourceKeys[i].minStepSize;
    
    RawComparable[][] its = new RawComparable[srcSize][];
    for (int i = 0; i < srcSize; i++)
      its[i] = sourceKeys[i].getKeys();
    RawComparable min, current;
    int minIndex = -1;
    int[] index = new int[srcSize];
    boolean[] dirty = new boolean[srcSize];
    while (true)
    {
      min = null;
      BlockDistribution bd = new BlockDistribution();
      for (int i = 0; i < srcSize; i++)
      {
        if (index[i] >= its[i].length)
          continue;
        current = its[i][index[i]];
        bd.add(sourceKeys[i].getBlockDistribution(current));
        if (min == null || comp.compare(min, current) > 0)
        {
          min = current;
          minIndex = i;
        }
      }
      if (min == null)
        break;

      result.add(min, bd);
      for (int i = 0; i < srcSize; i++)
      {
        if (index[i] >= its[i].length)
          continue;
        current = its[i][index[i]];
        if (i != minIndex)
        {
          if (comp.compare(min, current) != 0)
          {
            if (!dirty[i])
            {
              dirty[i] = true;
              index[i]++;
            } else if (comp.compare(min, its[i][index[i] - 1]) > 0 )
              index[i]++;
          } else {
            if (dirty[i])
              dirty[i] = false;
            index[i]++;
          }
        } else {
          if (dirty[i])
            dirty[i] = false;
          index[i]++;
        }
      }
    }
    return result;
  }
  
  public int resize(BlockDistribution lastBd)
  {
    Iterator<Map.Entry<RawComparable, BlockDistribution>> it =
      data.entrySet().iterator();
    KeyDistribution adjusted = new KeyDistribution(data.comparator());
    long realSize = 0, mySize = 0;
    RawComparable key = null;
    BlockDistribution bd = null, bd0 = null;
    while (it.hasNext())
    {
      Map.Entry<RawComparable, BlockDistribution> mapEntry = it.next();
      bd0 = mapEntry.getValue();
      mySize = bd0.getLength();
      if (realSize >= minStepSize/2 ||
          (realSize + mySize >= minStepSize*ColumnGroup.SPLIT_SLOP && 
              realSize >= minStepSize * (ColumnGroup.SPLIT_SLOP-1)))
      {
        adjusted.add(key, bd);
        bd = null;
        realSize = 0;
      }
      key = mapEntry.getKey();
      realSize += mySize;
      bd = BlockDistribution.sum(bd, bd0);
    }
    if (bd != null)
    {
      realSize += lastBd.getLength();
      if (realSize >= minStepSize/2 || adjusted.size() == 0)
      {
         // the last plus would contain more than liked, don't merge them.
        adjusted.add(key, bd);
      } else
        BlockDistribution.sum(lastBd, bd);
    }
    swap(adjusted);
    return data.size();
  }
  
  private void swap(KeyDistribution other) {
    long tmp = minStepSize;
    minStepSize = other.minStepSize;
    other.minStepSize = tmp;
    SortedMap<RawComparable, BlockDistribution> tmp2 = data;
    data = other.data;
    other.data = tmp2;
  }
}
