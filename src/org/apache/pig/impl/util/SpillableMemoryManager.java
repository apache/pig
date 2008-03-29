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
        /* We set the threshold to be 50% of tenured since that is where
         * the GC starts to dominate CPU time according to Sun doc */
        biggestHeap.setCollectionUsageThreshold((long)(biggestSize*.5));    
    }
    
    public void handleNotification(Notification n, Object o) {
        CompositeData cd = (CompositeData) n.getUserData();
        MemoryNotificationInfo info = MemoryNotificationInfo.from(cd);
        log.info("low memory handler called " + info.getUsage());
        long toFree = info.getUsage().getUsed() - (long)(info.getUsage().getMax()*.5);
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
                s.spill();
                estimatedFreed += toBeFreed;
                if (estimatedFreed > toFree) {
                    break;
                }
            }
            /* Poke the GC again to see if we successfully freed enough memory */
            System.gc();
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
