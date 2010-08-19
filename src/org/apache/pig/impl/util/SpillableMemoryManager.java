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
    
    LinkedList<WeakReference<Spillable>> spillables = new LinkedList<WeakReference<Spillable>>();
    
    // if we freed at least this much, invoke GC 
    // (default 40 MB - this can be overridden by user supplied property)
    private static long gcActivationSize = 40000000L ;
    
    // spill file size should be at least this much
    // (default 5MB - this can be overridden by user supplied property)
    private static long spillFileSizeThreshold = 5000000L ;
    
    // this will keep track of memory freed across spills
    // and between GC invocations
    private static long accumulatedFreeSize = 0L;
    
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
    
    public SpillableMemoryManager() {
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
         
        if (toFree < 0) {
            log.debug("low memory handler returning " + 
                "because there is nothing to free");
            return;
        }
        synchronized(spillables) {
            // Walk the list first and remove nulls, otherwise the sort
            // takes way too long.
            Iterator<WeakReference<Spillable>> i;
            for (i = spillables.iterator(); i.hasNext();) {
                Spillable s = i.next().get();
                if (s == null) {
                    i.remove();
                }
            }
            Collections.sort(spillables, new Comparator<WeakReference<Spillable>>() {

                /**
                 * We don't lock anything, so this sort may not be stable if a WeakReference suddenly
                 * becomes null, but it will be close enough.
                 * Also between the time we sort and we use these spillables, they
                 * may actually change in size - so this is just best effort
                 */    
                public int compare(WeakReference<Spillable> o1Ref, WeakReference<Spillable> o2Ref) {
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
            boolean invokeGC = false;
            for (i = spillables.iterator(); i.hasNext();) {
                Spillable s = i.next().get();
                // Still need to check for null here, even after we removed
                // above, because the reference may have gone bad on us
                // since the last check.
                if (s == null) {
                    i.remove();
                    continue;
                }
                long toBeFreed = s.getMemorySize();
                log.debug("Memorysize = "+toBeFreed+", spillFilesizethreshold = "+spillFileSizeThreshold+", gcactivationsize = "+gcActivationSize);
                // Don't keep trying if the rest of files are too small
                if (toBeFreed < spillFileSizeThreshold) {
                    log.debug("spilling small files - getting out of memory handler");
                    break ;
                }
                s.spill();               
                numObjSpilled++;
                estimatedFreed += toBeFreed;
                accumulatedFreeSize += toBeFreed;
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

        }
    }
    
    /**
     * Register a spillable to be tracked. No need to unregister, the tracking will stop
     * when the spillable is GCed.
     * @param s the spillable to track.
     */
    public void registerSpillable(Spillable s) {
        synchronized(spillables) {
            // Cleaing the entire list is too expensive.  Just trim off the front while
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
