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

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.ArrayList;

import org.apache.pig.PigCounters;
import org.apache.pig.PigException;
import org.apache.pig.PigWarning;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigHadoopLogger;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PigLogger;
import org.apache.pig.impl.util.BagFormat;
import org.apache.pig.impl.util.Spillable;
import org.apache.pig.tools.pigstats.PigStatusReporter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Default implementation of DataBag.  This is the an abstract class used as a
 * parent for all three of the types of data bags.
 */
public abstract class DefaultAbstractBag implements DataBag {

    private static final Log log = LogFactory.getLog(DataBag.class);

    private static PigLogger pigLogger = PhysicalOperator.getPigLogger();

    private static InterSedes sedes = InterSedesFactory.getInterSedesInstance();
    // Container that holds the tuples. Actual object instantiated by
    // subclasses.
    protected Collection<Tuple> mContents;

    // Spill files we've created.  These need to be removed in finalize.
    protected FileList mSpillFiles;

    // Total size, including tuples on disk.  Stored here so we don't have
    // to run through the disk when people ask.
    protected long mSize = 0;

    protected int mLastContentsSize = -1;

    protected long mMemSize = 0;

    /**
     * Get the number of elements in the bag, both in memory and on disk.
     */
    public long size() {
        return mSize;
    }

    /**
     * Add a tuple to the bag.
     * @param t tuple to add.
     */
    public void add(Tuple t) {
        synchronized (mContents) {
            mSize++;
            mContents.add(t);
        }
    }

    /**
     * Add contents of a bag to the bag.
     * @param b bag to add contents of.
     */
    public void addAll(DataBag b) {
        synchronized (mContents) {
            mSize += b.size();
            Iterator<Tuple> i = b.iterator();
            while (i.hasNext()) mContents.add(i.next());
        }
    }

    /**
     * Add contents of a container to the bag.
     * @param c Collection to add contents of.
     */
    public void addAll(Collection<Tuple> c) {
        synchronized (mContents) {
            mSize += c.size();
            Iterator<Tuple> i = c.iterator();
            while (i.hasNext()) mContents.add(i.next());
        }
    }

    /**
     * Return the size of memory usage.
     */
    public long getMemorySize() {
        int j;
        int numInMem = 0;
        long used = 0;

        synchronized (mContents) {
            if (mLastContentsSize == mContents.size()) return mMemSize;

            // I can't afford to talk through all the tuples every time the
            // memory manager wants to know if it's time to dump.  Just sample
            // the first 100 and see what we get.  This may not be 100%
            // accurate, but it's just an estimate anyway.
            numInMem = mContents.size();
            // Measure only what's in memory, not what's on disk.
            Iterator<Tuple> i = mContents.iterator();
            for (j = 0; i.hasNext() && j < 100; j++) { 
                used += i.next().getMemorySize();
            }
            mLastContentsSize = numInMem;
        }

        if (numInMem > 100) {
            // Estimate the per tuple size.  Do it in integer arithmetic
            // (even though it will be slightly less accurate) for speed.
            used /= j;
            used *= numInMem;
        }

        // add up the overhead for this object and other object variables
        int bag_fix_size = 8 /* object header */ 
        + 4 + 8 + 8 /* mLastContentsSize + mMemSize + mSize */
        + 8 + 8 /* mContents ref  + mSpillFiles ref*/
        + 4 /* +4 to round it to eight*/
        + 36 /* mContents fixed */
        ;
        long mFields_size =   roundToEight(4 + numInMem*4); /* mContents fixed + per entry */
        // in java hotspot 32bit vm, there seems to be a minimum bag size of 188 bytes
        // some of the extra bytes is probably from a minimum size of this array list
        mFields_size = Math.max(40, mFields_size); 
        
        used += bag_fix_size + mFields_size;

        // add up overhead for mSpillFiles ArrayList, Object[] inside ArrayList,
        // object variable inside ArrayList and references to spill files
        if (mSpillFiles != null) {
            used += roundToEight(36 /* mSpillFiles fixed overhead*/ + mSpillFiles.size()*4);
            
            if(mSpillFiles.size() > 0){
                //a rough estimate of memory used by each file entry
                // the auto generated files are likely to have same length
                long approx_per_entry_size =
                    roundToEight(mSpillFiles.get(0).toString().length() * 2 + 38);
                
                used += mSpillFiles.size() * approx_per_entry_size;
            }
        }
        
        mMemSize = used;
        return used;
    }

    
    /**
     * Memory size of objects are rounded to multiple of 8 bytes
     * @param i
     * @return i rounded to a equal of higher multiple of 8 
     */
    private long roundToEight(long i) {
        return 8 * ((i+7)/8); // integer division rounds the result down
    }
    
    /**
     * Clear out the contents of the bag, both on disk and in memory.
     * Any attempts to read after this is called will produce undefined
     * results.
     */
    public void clear() {
        synchronized (mContents) {
            mContents.clear();
            if (mSpillFiles != null) {
                for (int i = 0; i < mSpillFiles.size(); i++) {
                    boolean res = mSpillFiles.get(i).delete();
                    if (!res)
                        warn ("DefaultAbstractBag.clear: failed to delete " + mSpillFiles.get(i), PigWarning.DELETE_FAILED, null);  
                }
                mSpillFiles.clear();
            }
            mSize = 0;
        }
    }

    /**
     * This method is potentially very expensive since it may require a
     * sort of the bag; don't call it unless you have to.
     */
    @SuppressWarnings("unchecked")
    public int compareTo(Object other) {
        if (this == other)
            return 0;
        if (other instanceof DataBag) {
            DataBag bOther = (DataBag) other;
            if (this.size() != bOther.size()) {
                if (this.size() > bOther.size()) return 1;
                else return -1;
            }

            // Ugh, this is bogus.  But I have to know if two bags have the
            // same tuples, regardless of order.  Hopefully most of the
            // time the size check above will prevent this.
            // If either bag isn't already sorted, create a sorted bag out
            // of it so I can guarantee order.
            DataBag thisClone;
            DataBag otherClone;
            BagFactory factory = BagFactory.getInstance();
            
            if (this.isSorted() || this.isDistinct()) {
                thisClone = this;
            } else {
                thisClone = factory.newSortedBag(null);
                Iterator<Tuple> i = iterator();
                while (i.hasNext()) thisClone.add(i.next());
                
            }
            if (((DataBag) other).isSorted() || ((DataBag)other).isDistinct()) {
                otherClone = bOther;
            } else {
                otherClone = factory.newSortedBag(null);
                Iterator<Tuple> i = bOther.iterator();
                while (i.hasNext()) otherClone.add(i.next());
            }
            Iterator<Tuple> thisIt = thisClone.iterator();
            Iterator<Tuple> otherIt = otherClone.iterator();
            while (thisIt.hasNext() && otherIt.hasNext()) {
                Tuple thisT = thisIt.next();
                Tuple otherT = otherIt.next();
                
                int c = thisT.compareTo(otherT);
                if (c != 0) return c;
            }
            
            return 0;   // if we got this far, they must be equal
        } else {
            return DataType.compare(this, other);
        }
    }

    @Override
    public boolean equals(Object other) {
        return compareTo(other) == 0;
    }

    /**
     * Write a bag's contents to disk.
     * @param out DataOutput to write data to.
     * @throws IOException (passes it on from underlying calls).
     */
    public void write(DataOutput out) throws IOException {
        sedes.writeDatum(out, this);
    }
 
    /**
     * Read a bag from disk.
     * @param in DataInput to read data from.
     * @throws IOException (passes it on from underlying calls).
     */
    public void readFields(DataInput in) throws IOException {
        long size = in.readLong();
        
        for (long i = 0; i < size; i++) {
            try {
                Object o = sedes.readDatum(in);
                add((Tuple)o);
            } catch (ExecException ee) {
                throw ee;
            }
        }
    }

    /**
     * This is used by FuncEvalSpec.FakeDataBag.
     * @param stale Set stale state.
     */
    public void markStale(boolean stale)
    {
    }

    /**
     * Write the bag into a string. */
    @Override
    public String toString() {
        return BagFormat.format(this);
    }

    @Override
    public int hashCode() {
        int hash = 1;
        Iterator<Tuple> i = iterator();
        while (i.hasNext()) {
            // Use 37 because we want a prime, and tuple uses 31.
            hash = 37 * hash + i.next().hashCode();
        }
        return hash;
    }

    /**
     * Get a file to spill contents to.  The file will be registered in the
     * mSpillFiles array.
     * @return stream to write tuples to.
     */
    protected DataOutputStream getSpillFile() throws IOException {
        if (mSpillFiles == null) {
            // We want to keep the list as small as possible.
            mSpillFiles = new FileList(1);
        }

        String tmpDirName= System.getProperties().getProperty("java.io.tmpdir") ;                
        File tmpDir = new File(tmpDirName);
  
        // if the directory does not exist, create it.
        if (!tmpDir.exists()){
            log.info("Temporary directory doesn't exists. Trying to create: " + tmpDir.getAbsolutePath());
          // Create the directory and see if it was successful
          if (tmpDir.mkdir()){
            log.info("Successfully created temporary directory: " + tmpDir.getAbsolutePath());
          } else {
              // If execution reaches here, it means that we needed to create the directory but
              // were not successful in doing so.
              // 
              // If this directory is created recently then we can simply 
              // skip creation. This is to address a rare issue occuring in a cluster despite the
              // the fact that spill() makes call to getSpillFile() in a synchronized 
              // block. 
              if (tmpDir.exists()) {
                log.info("Temporary directory already exists: " + tmpDir.getAbsolutePath());
              } else {
                int errCode = 2111;
                String msg = "Unable to create temporary directory: " + tmpDir.getAbsolutePath();
                throw new ExecException(msg, errCode, PigException.BUG);                  
              }
          }
        }
        
        File f = File.createTempFile("pigbag", null);
        f.deleteOnExit();
        mSpillFiles.add(f);
        return new DataOutputStream(new BufferedOutputStream(
            new FileOutputStream(f)));
    }

    /**
     * Report progress to HDFS.
     */
    protected void reportProgress() {
        if (PhysicalOperator.reporter != null) {
            PhysicalOperator.reporter.progress();
        }
    }

    protected void warn(String msg, Enum warningEnum, Exception e) {
    	pigLogger = PhysicalOperator.getPigLogger();
    	if(pigLogger != null) {
    		pigLogger.warn(this, msg, warningEnum);
    	} else {
    		log.warn(msg, e);
    	}    	
    }
    
    protected void incSpillCount(Enum counter) {
        incSpillCount(counter, 1);
    }

    protected void incSpillCount(Enum counter, long numRecsSpilled) {
        PigStatusReporter reporter = PigStatusReporter.getInstance();
        if (reporter != null && reporter.getCounter(counter)!=null) {
            reporter.getCounter(counter).increment(numRecsSpilled);
        } else {
            PigHadoopLogger.getInstance().warn(this, "Spill counter incremented", counter);
        }
    }
    
    public static abstract class BagDelimiterTuple extends DefaultTuple{}
    public static class StartBag extends BagDelimiterTuple{
        private static final long serialVersionUID = 1L;}
    
    public static class EndBag extends BagDelimiterTuple{
        private static final long serialVersionUID = 1L;}
    
    public static final Tuple startBag = new StartBag();
    public static final Tuple endBag = new EndBag();

    protected static final int MAX_SPILL_FILES = 100;
 
}
