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
import java.util.Properties;

import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
    
    private final Log log = LogFactory.getLog(getClass());
    
    private LinkedList<WeakReference<Spillable>> spillables = new LinkedList<WeakReference<Spillable>>();
    // String references to spillables
    private LinkedList<SpillablePtr> spillablesSR = null;
    
    // if we freed at least this much, invoke GC 
    // (default 40 MB - this can be overridden by user supplied property)
    private static long gcActivationSize = 40000000L ;
    
    // spill file size should be at least this much
    // (default 5MB - this can be overridden by user supplied property)
    private static long spillFileSizeThreshold = 5000000L ;
    
    // fraction of biggest heap for which we want to get
    // "memory usage threshold exceeded" notifications
    private static double memoryThresholdFraction = 0.7;
    
    // fraction of biggest heap for which we want to get
    // "collection threshold exceeded" notifications
    private static double collectionMemoryThresholdFraction = 0.5;
        
    // log notification on usage threshold exceeded only the first time
    private boolean firstUsageThreshExceededLogged = false;
    
    // log notification on collection threshold exceeded only the first time
    private boolean firstCollectionThreshExceededLogged = false;
    
    private static volatile SpillableMemoryManager manager;

    private SpillableMemoryManager() {
        ((NotificationEmitter)ManagementFactory.getMemoryMXBean()).addNotificationListener(this, null, null);
        List<MemoryPoolMXBean> mpbeans = ManagementFactory.getMemoryPoolMXBeans();
        MemoryPoolMXBean biggestHeap = null;
        long biggestSize = 0;
        for (MemoryPoolMXBean b: mpbeans) {
            log.debug("Found heap (" + b.getName() +
                ") of type " + b.getType());
            if (b.getType() == MemoryType.HEAP) {
                /* Here we are making the leap of faith that the biggest
                 * heap is the tenured heap
                 */
                long size = b.getUsage().getMax();
                if (size > biggestSize) {
                    biggestSize = size;
                    biggestHeap = b;
                }
            }
        }
        if (biggestHeap == null) {
            throw new RuntimeException("Couldn't find heap");
        }
        log.debug("Selected heap to monitor (" +
            biggestHeap.getName() + ")");
        
        // we want to set both collection and usage threshold alerts to be 
        // safe. In some local tests after a point only collection threshold
        // notifications were being sent though usage threshold notifications
        // were sent early on. So using both would ensure that
        // 1) we get notified early (though usage threshold exceeded notifications)
        // 2) we get notified always when threshold is exceeded (either usage or
        //    collection)
        
        /* We set the threshold to be 50% of tenured since that is where
         * the GC starts to dominate CPU time according to Sun doc */
        biggestHeap.setCollectionUsageThreshold((long)(biggestSize * collectionMemoryThresholdFraction));
        // we set a higher threshold for usage threshold exceeded notification
        // since this is more likely to be effective sooner and we do not
        // want to be spilling too soon
        biggestHeap.setUsageThreshold((long)(biggestSize * memoryThresholdFraction));
    }
    
    public static SpillableMemoryManager getInstance() {
        if (manager == null) {
            manager = new SpillableMemoryManager();
        }
        return manager;
    }

    public static void configure(Properties properties) {
        
        try {
            
            spillFileSizeThreshold = Long.parseLong(
                    properties.getProperty("pig.spill.size.threshold") ) ;
            
            gcActivationSize = Long.parseLong(
                    properties.getProperty("pig.spill.gc.activation.size") ) ;
        } 
        catch (NumberFormatException  nfe) {
            throw new RuntimeException("Error while converting system configurations" +
            		"spill.size.threshold, spill.gc.activation.size", nfe) ;
        }
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
            long threshold = (long)(info.getUsage().getMax() * memoryThresholdFraction);
            toFree = info.getUsage().getUsed() - threshold + (long)(threshold * 0.5);

            //log
            String msg = "memory handler call- Usage threshold " 
                + info.getUsage();
            if(!firstUsageThreshExceededLogged){
                log.info("first " + msg);
                firstUsageThreshExceededLogged = true;
            }else{
                log.debug(msg);
            }
        } else { // MEMORY_COLLECTION_THRESHOLD_EXCEEDED CASE
            long threshold = (long)(info.getUsage().getMax() * collectionMemoryThresholdFraction);
            toFree = info.getUsage().getUsed() - threshold + (long)(threshold * 0.5);
            
            //log
            String msg = "memory handler call - Collection threshold "
                + info.getUsage();
            if(!firstCollectionThreshExceededLogged){
                log.info("first " + msg);
                firstCollectionThreshExceededLogged = true;
            }else{
                log.debug(msg);
            }

        }
        // Remove empty spillables to improve sort speed.
        clearSpillables();
        if (toFree < 0) {
            log.debug("low memory handler returning " + 
                "because there is nothing to free");
            return;
        }
        synchronized(spillables) {
            /**
             * Store a reference to a spillable and its size into a stable
             * list so that the sort is stable (a Java 7 contract).
             * Between the time we sort and we use these spillables, they
             * may actually change in size.
             */
            spillablesSR = new LinkedList<SpillablePtr>();
            for (Iterator<WeakReference<Spillable>> i = spillables.iterator(); i.hasNext();) {
                // Check that the object still exists before adding to the Strong Referenced list.
                Spillable s = i.next().get();
                if (s == null) {
                    i.remove();
                    continue;
                }
                // Get a ptr to the spillable and its current size.
                // we need a stable size for sorting.
                spillablesSR.add(new SpillablePtr(s, s.getMemorySize()));
            }
            log.debug("Spillables list size: " + spillablesSR.size());
            Collections.sort(spillablesSR, new Comparator<SpillablePtr>() {
                // Sort the list in descending order. We spill the biggest items first,
                // and only as many as we need to to reduce memory usage
                // below the threshold.
                @Override
                public int compare(SpillablePtr o1Ref, SpillablePtr o2Ref) {
                    Spillable o1 = o1Ref.get();
                    Spillable o2 = o2Ref.get();
                    if (o1 == null && o2 == null) {
                        return 0;
                    }
                    if (o1 == null) {
                        return 1;
                    }
                    if (o2 == null) {
                        return -1;
                    }
                    long o1Size = o1.getMemorySize();
                    long o2Size = o2.getMemorySize();
                
                    if (o1Size == o2Size) {
                        return 0;
                    }
                    if (o1Size < o2Size) {
                        return 1;
                    }
                    return -1;
                }
            });
            long estimatedFreed = 0;
            int numObjSpilled = 0;
            /*
             * Before PIG-3979, Pig invoke System.gc inside this hook,
             * but calling gc from within a gc notification seems to cause a lot of gc activity.
             * More accurately, on Java implementations in which System.gc() does force a gc,
             * this causes a lot of looping. On systems in which System.gc() is just a suggestion,
             * relying on the outcome of the System.gc() call is a mistake.
             *
             * Therefore, this version of the code does away with attempts to guess what happens
             * what we call s.spill(). For POPartionAgg spillables, the call tells the spillable
             * to reduce itself. No data is necessarily written to disk. 
             */
            for (Iterator<SpillablePtr> i = spillablesSR.iterator(); i.hasNext();) {
                SpillablePtr sPtr = i.next();
                long toBeFreed = sPtr.getMemorySize();
                log.debug("Memorysize = "+toBeFreed+", spillFilesizethreshold = "+spillFileSizeThreshold+", gcactivationsize = "+gcActivationSize);
                // Don't keep trying if the rest of files are too small
                if (toBeFreed < spillFileSizeThreshold) {
                    log.debug("spilling small files - getting out of memory handler");
                    break ;
                }
                Spillable s = sPtr.get();
                if (s != null)
                    s.spill();
                numObjSpilled++;
                estimatedFreed += toBeFreed;

                if (estimatedFreed > toFree) {
                    log.debug("Freed enough space - getting out of memory handler");
                    break;
                }
            }
            // We are done with the strongly referenced list of spillables
            spillablesSR = null;

            if (estimatedFreed > 0) {
                String msg = "Spilled an estimate of " + estimatedFreed +
                " bytes from " + numObjSpilled + " objects. " + info.getUsage();;
                log.debug(msg);
            }

        }
    }
    
    public static class SpillablePtr {
        private WeakReference<Spillable> spillable;
        private long size;
        SpillablePtr(Spillable p, long s) {
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
        synchronized(spillables) {
            // Cleaning the entire list is too expensive.  Just trim off the front while
            // we can.
            WeakReference<Spillable> first = spillables.peek();
            while (first != null && first.get() == null) {
                spillables.remove();
                first = spillables.peek();
            }
            spillables.add(new WeakReference<Spillable>(s));
        }
    }
}
