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
package org.apache.pig.data;

import org.apache.pig.PigConfiguration;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;

/**
 * Class to hold code common to self spilling bags such as InternalCachedBag
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class SelfSpillBag extends DefaultAbstractBag {
    private static final long serialVersionUID = 1L;
    protected MemoryLimits memLimit;

    public SelfSpillBag(int bagCount) {
        memLimit = new MemoryLimits(bagCount, -1);
    }

    public SelfSpillBag(int bagCount, float percent) {
        memLimit = new MemoryLimits(bagCount, percent);
    }

    /**
     * This class helps to compute the number of entries that should be held in
     * memory so that memory consumption is limited. The memory limit is
     * computed using the percentage of max memory that the user of this class
     * is allowed to use, and number of similar objects that share this limit.
     * The number of objects that will fit into this memory limit is computed
     * using the average memory size of the objects whose size is given to this
     * class.
     */
    @InterfaceAudience.Private
    @InterfaceStability.Evolving
    public static class MemoryLimits {

        private long maxMemUsage;
        private int cacheLimit = Integer.MAX_VALUE;
        private long memUsage = 0;
        private long numObjsSizeChecked = 0;
        
        private static float cachedMemUsage = 0.2F;
        private static long maxMem = 0;
        static {
            maxMem = Runtime.getRuntime().maxMemory();
            if (PigMapReduce.sJobConfInternal.get() != null) {
                String usage = PigMapReduce.sJobConfInternal.get().get(
                        PigConfiguration.PROP_CACHEDBAG_MEMUSAGE);
                if (usage != null) {
                    cachedMemUsage = Float.parseFloat(usage);
                }
            }
        }

        /**
         * @param bagCount
         * @param percent
         */
        public MemoryLimits(int bagCount, float percent) {
            init(bagCount, percent);
        }

        private void init(int bagCount, float percent) {

            if (percent < 0) {
                percent = cachedMemUsage;
            }

            maxMemUsage = (long) ((maxMem * percent) / bagCount);

            // set limit to 0, if memusage is 0 or really really small.
            // then all tuples are put into disk
            if (maxMemUsage < 1) {
                cacheLimit = 0;
            }
        }

        /**
         * Computes the number of objects that would fit into memory based on
         * the memory limit and average size of each object.
         * 
         * @return number of objects limit
         */
        public int getCacheLimit() {
            if (numObjsSizeChecked > 0) {
                long avgUsage = memUsage / numObjsSizeChecked;
                if (avgUsage > 0) {
                    cacheLimit = (int) (maxMemUsage / avgUsage);
                }
            }
            return cacheLimit;
        }

        /**
         * Submit information about size of another object
         * 
         * @param memorySize
         */
        public void addNewObjSize(long memorySize) {
            memUsage += memorySize;
            ++numObjsSizeChecked;
        }

        /**
         * @return the size of
         */
        public long getNumObjectsSizeAdded() {
            return numObjsSizeChecked;
        }
    }

}
