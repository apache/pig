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
package org.apache.pig.impl.util;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryNotificationInfo;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.PigConfiguration;

/**
 * This class Tracks the tenured pool and a list of Spillable objects. When memory gets low, this
 * class will start requesting Spillable objects to free up memory.
 * <p>
 * Low memory is defined as more than 50% of the tenured pool being allocated. Spillable objects are
 * tracked using WeakReferences so that the objects can be GCed even though this class has a reference
 * to them.
 *
 */
public class SpillableMemoryManager implements NotificationListener {

    private static final Log log = LogFactory.getLog(SpillableMemoryManager.class);

    private static final int UNUSED_MEMORY_THRESHOLD_DEFAULT = 350 * 1024 * 1024;
    private static final float MEMORY_THRESHOLD_FRACTION_DEFAULT = 0.7f;
    private static final float COLLECTION_THRESHOLD_FRACTION_DEFAULT = 0.7f;

    private LinkedList<WeakReference<Spillable>> spillables = new LinkedList<WeakReference<Spillable>>();
    // References to spillables with size
    private LinkedList<SpillablePtr> spillablesSR = null;

    private Object spillLock = new Object();

    // if we freed at least this much, invoke GC
    // (default 40 MB - this can be overridden by user supplied property)
    private static long gcActivationSize = 40000000L ;

    // spill file size should be at least this much
    // (default 5MB - this can be overridden by user supplied property)
    private static long spillFileSizeThreshold = 5000000L ;

    // this will keep track of memory freed across spills
    // and between GC invocations
    private long accumulatedFreeSize = 0L;

    private long memoryThresholdSize = 0L;

    private long collectionThresholdSize = 0L;

    // log notification on usage threshold exceeded only the first time
    private boolean firstUsageThreshExceededLogged = false;

    // log notification on collection threshold exceeded only the first time
    private boolean firstCollectionThreshExceededLogged = false;

    // fraction of the total heap used for the threshold to determine
    // if we want to perform an extra gc before the spill
    private float extraGCThresholdFraction = 0.05f;
    private long extraGCSpillSizeThreshold  = 0L;

    private volatile boolean blockRegisterOnSpill = false;

    private MemoryPoolMXBean tenuredHeap;

    private static final SpillableMemoryManager manager = new SpillableMemoryManager();

    //@StaticDataCleanup
    public static void staticDataCleanup() {
        manager.spillables.clear();
        manager.accumulatedFreeSize = 0L;
    }

    private SpillableMemoryManager() {
        ((NotificationEmitter)ManagementFactory.getMemoryMXBean()).addNotificationListener(this, null, null);
        List<MemoryPoolMXBean> mpbeans = ManagementFactory.getMemoryPoolMXBeans();
        long totalSize = 0;
        for (MemoryPoolMXBean pool : mpbeans) {
            log.debug("Found heap (" + pool.getName() + ") of type " + pool.getType());
            if (pool.getType() == MemoryType.HEAP) {
                long size = pool.getUsage().getMax();
                totalSize += size;
                // CMS Old Gen or "tenured" is the only heap that supports
                // setting usage threshold.
                if (pool.isUsageThresholdSupported()) {
                    tenuredHeap = pool;
                }
            }
        }
        extraGCSpillSizeThreshold  = (long) (totalSize * extraGCThresholdFraction);
        if (tenuredHeap == null) {
            throw new RuntimeException("Couldn't find heap");
        }

        configureMemoryThresholds(MEMORY_THRESHOLD_FRACTION_DEFAULT,
                COLLECTION_THRESHOLD_FRACTION_DEFAULT,
                UNUSED_MEMORY_THRESHOLD_DEFAULT);

    }

    /**
     * Configure thresholds for memory usage/collection threshold exceeded notifications.
     * Uses memoryThresholdFraction and collectionMemoryThresholdFraction to configure thresholds
     * for heap sizes less than 1GB and unusedMemoryThreshold for bigger heaps.
     *
     * @param memoryThresholdFraction
     *            fraction of biggest heap for which we want to get memory usage
     *            threshold exceeded notifications
     * @param collectionMemoryThresholdFraction
     *            fraction of biggest heap for which we want to get collection
     *            threshold exceeded notifications
     * @param unusedMemoryThreshold
     *            Unused memory size below which we want to get notifications
     */
    private void configureMemoryThresholds(float memoryThresholdFraction, float collectionMemoryThresholdFraction, long unusedMemoryThreshold) {
        long tenuredHeapSize = tenuredHeap.getUsage().getMax();
        memoryThresholdSize = (long)(tenuredHeapSize * memoryThresholdFraction);
        collectionThresholdSize = (long)(tenuredHeapSize * collectionMemoryThresholdFraction);
        if (unusedMemoryThreshold > 0) {
            // For a 1G heap we will be spilling around ~700MB with 300MB still unused with default 0.7 threshold
            // For bigger heaps, we still want to spill when there is 300MB unused (plus another 50MB for buffer) and not at 70%.
            // For eg: For 4G we want to start spilling at 3.65GB and not at 2.8GB(70%) for better use of memory
            long unusedThreshold = tenuredHeapSize - unusedMemoryThreshold;
            memoryThresholdSize = Math.max(memoryThresholdSize, unusedThreshold);
            collectionThresholdSize = Math.max(collectionThresholdSize, unusedThreshold);
        }

        // we want to set both collection and usage threshold alerts to be
        // safe. In some local tests after a point only collection threshold
        // notifications were being sent though usage threshold notifications
        // were sent early on. So using both would ensure that
        // 1) we get notified early (though usage threshold exceeded notifications)
        // 2) we get notified always when threshold is exceeded (either usage or
        //    collection)

        /* We set the threshold to be 50% of tenured since that is where
         * the GC starts to dominate CPU time according to Sun doc */
        tenuredHeap.setCollectionUsageThreshold((long)(collectionThresholdSize));
        // we set a higher threshold for usage threshold exceeded notification
        // since this is more likely to be effective sooner and we do not
        // want to be spilling too soon

        tenuredHeap.setUsageThreshold((long)(memoryThresholdSize));
        log.info("Selected heap (" + tenuredHeap.getName() + ")" + " of size " + tenuredHeapSize
                + " to monitor. collectionUsageThreshold = " + tenuredHeap.getCollectionUsageThreshold()
                + ", usageThreshold = " + tenuredHeap.getUsageThreshold() );
    }

    public static SpillableMemoryManager getInstance() {
        return manager;
    }

    public void configure(Configuration conf) {

        spillFileSizeThreshold = conf.getLong("pig.spill.size.threshold", spillFileSizeThreshold);
        gcActivationSize = conf.getLong("pig.spill.gc.activation.size", gcActivationSize);
        float memoryThresholdFraction = conf.getFloat(PigConfiguration.PIG_SPILL_MEMORY_USAGE_THRESHOLD_FRACTION, MEMORY_THRESHOLD_FRACTION_DEFAULT);
        float collectionThresholdFraction = conf.getFloat(PigConfiguration.PIG_SPILL_COLLECTION_THRESHOLD_FRACTION, COLLECTION_THRESHOLD_FRACTION_DEFAULT);
        long unusedMemoryThreshold = conf.getLong(PigConfiguration.PIG_SPILL_UNUSED_MEMORY_THRESHOLD_SIZE, UNUSED_MEMORY_THRESHOLD_DEFAULT);
        configureMemoryThresholds(memoryThresholdFraction, collectionThresholdFraction, unusedMemoryThreshold);
    }

    @Override
    public void handleNotification(Notification n, Object o) {
        CompositeData cd = (CompositeData) n.getUserData();
        MemoryNotificationInfo info = MemoryNotificationInfo.from(cd);
        // free the amount exceeded over the threshold and then a further half
        // so if threshold = heapmax/2, we will be trying to free
        // used - heapmax/2 + heapmax/4
        long toFree = 0L;
        if(n.getType().equals(MemoryNotificationInfo.MEMORY_THRESHOLD_EXCEEDED)) {
            toFree = info.getUsage().getUsed() - memoryThresholdSize + (long)(memoryThresholdSize * 0.5);

            //log
            String msg = "memory handler call- Usage threshold "
                + info.getUsage() + ", toFree = " + toFree;
            if(!firstUsageThreshExceededLogged){
                log.info("first " + msg);
                firstUsageThreshExceededLogged = true;
            }else{
                log.debug(msg);
            }
        } else { // MEMORY_COLLECTION_THRESHOLD_EXCEEDED CASE
            toFree = info.getUsage().getUsed() - collectionThresholdSize + (long)(collectionThresholdSize * 0.5);

            //log
            String msg = "memory handler call - Collection threshold "
                + info.getUsage() + ", toFree = " + toFree;
            if(!firstCollectionThreshExceededLogged){
                log.info("first " + msg);
                firstCollectionThreshExceededLogged = true;
            }else{
                log.debug(msg);
            }

        }
        if (toFree < 0) {
            log.debug("low memory handler returning " +
                "because there is nothing to free");
            return;
        }

        // Use a separate spillLock to block multiple handleNotification calls
        synchronized (spillLock) {
            synchronized(spillables) {
                spillablesSR = new LinkedList<SpillablePtr>();
                for (Iterator<WeakReference<Spillable>> i = spillables.iterator(); i.hasNext();) {
                    Spillable s = i.next().get();
                    if (s == null) {
                        i.remove();
                        continue;
                    }
                    // Create a list with spillable size for stable sorting. Refer PIG-4012
                    spillablesSR.add(new SpillablePtr(s, s.getMemorySize()));
                }
                log.debug("Spillables list size: " + spillablesSR.size());
                Collections.sort(spillablesSR, new Comparator<SpillablePtr>() {
                    @Override
                    public int compare(SpillablePtr o1Ref, SpillablePtr o2Ref) {
                        long o1Size = o1Ref.getMemorySize();
                        long o2Size = o2Ref.getMemorySize();

                        if (o1Size == o2Size) {
                            return 0;
                        }
                        if (o1Size < o2Size) {
                            return 1;
                        }
                        return -1;
                    }
                });
                // Block new bags from being registered
                blockRegisterOnSpill = true;
            }

            try {
                long estimatedFreed = 0;
                int numObjSpilled = 0;
                boolean invokeGC = false;
                boolean extraGCCalled = false;
                boolean isGroupingSpillable = false;
                for (Iterator<SpillablePtr> i = spillablesSR.iterator(); i.hasNext();) {
                    SpillablePtr sPtr = i.next();
                    Spillable s = sPtr.get();
                    // Still need to check for null here, even after we removed
                    // above, because the reference may have gone bad on us
                    // since the last check.
                    if (s == null) {
                        i.remove();
                        continue;
                    }
                    long toBeFreed = sPtr.getMemorySize();
                    log.debug("Memorysize = "+toBeFreed+", spillFilesizethreshold = "+spillFileSizeThreshold+", gcactivationsize = "+gcActivationSize);
                    // Don't keep trying if the rest of files are too small
                    if (toBeFreed < spillFileSizeThreshold) {
                        log.debug("spilling small files - getting out of memory handler");
                        break ;
                    }
                    isGroupingSpillable = (s instanceof GroupingSpillable);
                    // If single Spillable is bigger than the threshold,
                    // we force GC to make sure we really need to keep this
                    // object before paying for the expensive spill().
                    // Done at most once per handleNotification.
                    // Do not invoke extraGC for GroupingSpillable. Its size will always exceed
                    // extraGCSpillSizeThreshold and the data is always strong referenced.
                    if( !extraGCCalled && extraGCSpillSizeThreshold != 0
                        && toBeFreed > extraGCSpillSizeThreshold  && !isGroupingSpillable) {
                        log.debug("Single spillable has size " + toBeFreed + "bytes. Calling extra gc()");
                        // this extra assignment to null is needed so that gc can free the
                        // spillable if nothing else is pointing at it
                        s = null;
                        System.gc();
                        extraGCCalled = true;
                        // checking again to see if this reference is still valid
                        s = sPtr.get();
                        if (s == null) {
                            i.remove();
                            accumulatedFreeSize = 0;
                            invokeGC = false;
                            continue;
                        }
                    }
                    // Unblock registering of new bags temporarily as aggregation
                    // of POPartialAgg requires new record to be loaded.
                    blockRegisterOnSpill = !isGroupingSpillable;
                    long numSpilled;
                    try {
                        numSpilled = s.spill();
                    } finally {
                        blockRegisterOnSpill = true;
                    }

                    if (numSpilled > 0) {
                        numObjSpilled++;
                        estimatedFreed += toBeFreed;
                        accumulatedFreeSize += toBeFreed;
                    }
                    // This should significantly reduce the number of small files
                    // in case that we have a lot of nested bags
                    if (accumulatedFreeSize > gcActivationSize) {
                        invokeGC = true;
                    }

                    if (estimatedFreed > toFree) {
                        log.debug("Freed enough space - getting out of memory handler");
                        invokeGC = true;
                        break;
                    }
                }
                spillablesSR = null;
                /* Poke the GC again to see if we successfully freed enough memory */
                if(invokeGC) {
                    System.gc();
                    // now that we have invoked the GC, reset accumulatedFreeSize
                    accumulatedFreeSize = 0;
                }
                if(estimatedFreed > 0){
                    String msg = "Spilled an estimate of " + estimatedFreed +
                    " bytes from " + numObjSpilled + " objects. " + info.getUsage();;
                    log.info(msg);
                }
            } finally {
                blockRegisterOnSpill = false;
            }
        }

    }

    public void clearSpillables() {
        synchronized (spillables) {
            // Walk the list first and remove nulls, otherwise the sort
            // takes way too long.
            for (Iterator<WeakReference<Spillable>> i = spillables.iterator(); i
                    .hasNext();) {
                Spillable s = i.next().get();
                if (s == null) {
                    i.remove();
                }
            }
        }
    }
    /**
     * Register a spillable to be tracked. No need to unregister, the tracking will stop
     * when the spillable is GCed.
     * @param s the spillable to track.
     */
    public void registerSpillable(Spillable s) {
        synchronized (spillables) {
            // Cleaing the entire list is too expensive.  Just trim off the front while
            // we can.
            WeakReference<Spillable> first = spillables.peek();
            while (first != null && first.get() == null) {
                spillables.remove();
                first = spillables.peek();
            }

            if (blockRegisterOnSpill) {
                // When the spill is happening we do not want to register new bags
                // save for exceptions like POPartialAgg. So block here.
                // blockRegisterOnSpill is set to false in the finally block after spill.
                // But just in case adding a safeguard of 5 min timeout (assuming a large
                // spill completes within 5 mins) instead of infinitely blocking
                // in case there are missed corner cases causing deadlock.
                try {
                    int i = 6000;
                    for (; i > 0 && blockRegisterOnSpill; i--) {
                        Thread.sleep(50);
                    }
                    if (i == 0) {
                        log.warn("Spill took more than 5 mins. This needs investigation");
                    }
                } catch (InterruptedException e) {
                    log.warn("Interrupted exception in registerSpillable while blocked on spill", e);
                }
                blockRegisterOnSpill = false;
            }
            spillables.add(new WeakReference<Spillable>(s));
        }
    }

    private static class SpillablePtr {
        private WeakReference<Spillable> spillable;
        private long size;

        public SpillablePtr(Spillable p, long s) {
            spillable = new WeakReference<Spillable>(p);
            size = s;
        }

        public Spillable get() {
            return spillable.get();
        }

        public long getMemorySize() {
            return size;
        }
    }
}
