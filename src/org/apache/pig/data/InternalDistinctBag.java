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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.PigConfiguration;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;



/**
 * An unordered collection of Tuples with no multiples.  Data is
 * stored without duplicates as it comes in.  When it is time to spill,
 * that data is sorted and written to disk.  The data is
 * stored in a HashSet.  When it is time to sort it is placed in an
 * ArrayList and then sorted.  Dispite all these machinations, this was
 * found to be faster than storing it in a TreeSet.
 *
 * This bag spills pro-actively when the number of tuples in memory
 * reaches a limit
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class InternalDistinctBag extends SortedSpillBag {

    /**
     *
     */
    private static final long serialVersionUID = 2L;

    private static final Log log = LogFactory.getLog(InternalDistinctBag.class);

    private static TupleFactory gTupleFactory = TupleFactory.getInstance();

    private transient boolean mReadStarted = false;

    public InternalDistinctBag() {
        this(1, -1.0f);
    }

    public InternalDistinctBag(int bagCount) {
        this(bagCount, -1.0f);
    }

    public InternalDistinctBag(int bagCount, float percent) {
        super(bagCount, percent);
        if (percent < 0) {
            percent = 0.2F;
            if (PigMapReduce.sJobConfInternal.get() != null) {
                String usage = PigMapReduce.sJobConfInternal.get().get(PigConfiguration.PROP_CACHEDBAG_MEMUSAGE);
                if (usage != null) {
                    percent = Float.parseFloat(usage);
                }
            }
        }
               
        init(bagCount, percent);
    }

    private void init(int bagCount, double percent) {
        mContents = new HashSet<Tuple>();
    }

    @Override
    public boolean isSorted() {
        return false;
    }

    @Override
    public boolean isDistinct() {
        return true;
    }


    @Override
    public long size() {
        if (mSpillFiles != null && mSpillFiles.size() > 0){
            //We need to racalculate size to guarantee a count of unique
            //entries including those on disk
            Iterator<Tuple> iter = iterator();
            int newSize = 0;
            while (iter.hasNext()) {
                newSize++;
                iter.next();
            }

            mSize = newSize;
        }
        return mSize;
    }


    @Override
    public Iterator<Tuple> iterator() {
        return new DistinctDataBagIterator();
    }

    @Override
    public void add(Tuple t) {
        synchronized(mContents) {
            if(mReadStarted) {
                throw new IllegalStateException("InternalDistinctBag is closed for adding new tuples");
            }

            if (mContents.size() > memLimit.getCacheLimit()) {
                proactive_spill(null);
            }

            if (mContents.add(t)) {
                mSize ++;

                // check how many tuples memory can hold by getting average
                // size of first 100 tuples
                if(mSize < 100 && (mSpillFiles == null || mSpillFiles.isEmpty())) {
                    memLimit.addNewObjSize(t.getMemorySize());
                }
            }
            markSpillableIfNecessary();
        }
    }

    /**
     * An iterator that handles getting the next tuple from the bag.
     * Data can be stored in a combination of in memory and on disk.
     */
    private class DistinctDataBagIterator implements Iterator<Tuple> {

        private class TContainer implements Comparable<TContainer> {
            public Tuple tuple;
            public int fileNum;

            @Override
            @SuppressWarnings("unchecked")
            public int compareTo(TContainer other) {
                return tuple.compareTo(other.tuple);
            }

            @Override
            public boolean equals(Object obj) {
                if (obj instanceof TContainer) {
                    return compareTo((TContainer)obj) == 0;
                }

                return false;
            }

            @Override
            public int hashCode() {
                return tuple.hashCode();
            }
        }

        // We have to buffer a tuple because there's no easy way for next
        // to tell whether or not there's another tuple available, other
        // than to read it.
        private Tuple mBuf = null;
        private int mMemoryPtr = 0;
        private TreeSet<TContainer> mMergeTree = null;
        private ArrayList<DataInputStream> mStreams = null;
        private int mCntr = 0;

        @SuppressWarnings("unchecked")
        DistinctDataBagIterator() {
            // If this is the first read, we need to sort the data.
            synchronized(mContents) {
                if (!mReadStarted) {
                    preMerge();
                    // We're the first reader, we need to sort the data.
                    // This is in case it gets dumped under us.
                    ArrayList<Tuple> l = new ArrayList<Tuple>(mContents);
                    Collections.sort(l);
                    mContents = l;
                    mReadStarted = true;
                }
            }
        }

        @Override
        public boolean hasNext() {
            // See if we can find a tuple.  If so, buffer it.
            mBuf = next();
            return mBuf != null;
        }

        @Override
        public Tuple next() {
            // This will report progress every 1024 times through next.
            // This should be much faster than using mod.
            if ((mCntr++ & 0x3ff) == 0) reportProgress();

            // If there's one in the buffer, use that one.
            if (mBuf != null) {
                Tuple t = mBuf;
                mBuf = null;
                return t;
            }

            // Check to see if we just need to read from memory.
            if (mSpillFiles == null || mSpillFiles.size() == 0) {
                return readFromMemory();
            }

            // We have spill files, so we need to read the next tuple from
            // one of those files or from memory.
            return readFromTree();
        }

        /**
         * Not implemented.
         */
        @Override
        public void remove() {}

        private Tuple readFromTree() {
            if (mMergeTree == null) {
                // First read, we need to set up the queue and the array of
                // file streams
                mMergeTree = new TreeSet<TContainer>();

                // Add one to the size in case we spill later.
                mStreams =
                    new ArrayList<DataInputStream>(mSpillFiles.size() + 1);

                Iterator<File> i = mSpillFiles.iterator();
                while (i.hasNext()) {
                    try {
                        DataInputStream in =
                            new DataInputStream(new BufferedInputStream(
                                new FileInputStream(i.next())));
                        mStreams.add(in);
                        // Add the first tuple from this file into the
                        // merge queue.
                        addToQueue(null, mStreams.size() - 1);
                    } catch (FileNotFoundException fnfe) {
                        // We can't find our own spill file?  That should
                        // never happen.
                        String msg = "Unable to find our spill file.";
                        log.fatal(msg, fnfe);
                        throw new RuntimeException(msg, fnfe);
                    }
                }

                // Prime one from memory too
                if (mContents.size() > 0) {
                    addToQueue(null, -1);
                }
            }

            if (mMergeTree.size() == 0) return null;

            // Pop the top one off the queue
            TContainer c = mMergeTree.first();
            mMergeTree.remove(c);

            // Add the next tuple from whereever we read from into the
            // queue.  Buffer the tuple we're returning, as we'll be
            // reusing c.
            Tuple t = c.tuple;
            addToQueue(c, c.fileNum);

            return t;
        }

        private void addToQueue(TContainer c, int fileNum) {
            if (c == null) {
                c = new TContainer();
            }
            c.fileNum = fileNum;

            if (fileNum == -1) {
                // Need to read from memory.
                do {
                    c.tuple = readFromMemory();
                    if (c.tuple != null) {
                        // If we find a unique entry, then add it to the queue.
                        // Otherwise ignore it and keep reading.
                        if (mMergeTree.add(c)) {
                            return;
                        }
                    }
                } while (c.tuple != null);
                return;
            }

            // Read the next tuple from the indicated file
            DataInputStream in = mStreams.get(fileNum);
            if (in != null) {
                // There's still data in this file
                c.tuple = gTupleFactory.newTuple();
                do {
                    try {
                        c.tuple.readFields(in);
                        // If we find a unique entry, then add it to the queue.
                        // Otherwise ignore it and keep reading.  If we run out
                        // of tuples to read that's fine, we just won't add a
                        // new one from this file.
                        if (mMergeTree.add(c)) {
                            return;
                        }
                    } catch (EOFException eof) {
                        // Out of tuples in this file.  Set our slot in the
                        // array to null so we don't keep trying to read from
                        // this file.
                        try {
                            in.close();
                        }catch(IOException e) {
                            log.warn("Failed to close spill file.", e);
                        }
                        mStreams.set(fileNum, null);
                        return;
                    } catch (IOException ioe) {
                        String msg = "Unable to find our spill file.";
                        log.fatal(msg, ioe);
                        throw new RuntimeException(msg, ioe);
                    }
                } while (true);
            }
        }

        // Function assumes that the reader lock is already held before we enter
        // this function.
        private Tuple readFromMemory() {
            if (mContents.size() == 0) return null;

            if (mMemoryPtr < mContents.size()) {
                return ((ArrayList<Tuple>)mContents).get(mMemoryPtr++);
            } else {
                return null;
            }
        }

        /**
         * Pre-merge if there are too many spill files.  This avoids the issue
         * of having too large a fan out in our merge.  Experimentation by
         * the hadoop team has shown that 100 is about the optimal number
         * of spill files.  This function modifies the mSpillFiles array
         * and assumes the write lock is already held. It will not unlock it.
         *
         * Tuples are reconstituted as tuples, evaluated, and rewritten as
         * tuples.  This is expensive, but I don't know how to read tuples
         * from the file otherwise.
         *
         * This function is slightly different than the one in
         * SortedDataBag, as it uses a TreeSet instead of a PriorityQ.
         */
        private void preMerge() {
            if (mSpillFiles == null ||
                    mSpillFiles.size() <= MAX_SPILL_FILES) {
                return;
            }

            // While there are more than max spill files, gather max spill
            // files together and merge them into one file.  Then remove the others
            // from mSpillFiles.  The new spill files are attached at the
            // end of the list, so I can just keep going until I get a
            // small enough number without too much concern over uneven
            // size merges.  Convert mSpillFiles to a linked list since
            // we'll be removing pieces from the middle and we want to do
            // it efficiently.
            try {

                LinkedList<File> ll = new LinkedList<File>(mSpillFiles);
                LinkedList<File> filesToDelete = new LinkedList<File>();
                while (ll.size() > MAX_SPILL_FILES) {
                    ListIterator<File> i = ll.listIterator();
                    mStreams =
                        new ArrayList<DataInputStream>(MAX_SPILL_FILES);
                    mMergeTree = new TreeSet<TContainer>();

                    for (int j = 0; j < MAX_SPILL_FILES; j++) {
                        try {
                            File f = i.next();
                            DataInputStream in =
                                new DataInputStream(new BufferedInputStream(
                                    new FileInputStream(f)));
                            mStreams.add(in);
                            addToQueue(null, mStreams.size() - 1);
                            i.remove();
                            filesToDelete.add(f);
                        } catch (FileNotFoundException fnfe) {
                            // We can't find our own spill file?  That should
                            // neer happen.
                            String msg = "Unable to find our spill file.";
                            log.fatal(msg, fnfe);
                            throw new RuntimeException(msg, fnfe);
                        }
                    }

                    // Get a new spill file.  This adds one to the end of
                    // the spill files list.  So I need to append it to my
                    // linked list as well so that it's still there when I
                    // move my linked list back to the spill files.
                    try {
                        DataOutputStream out = getSpillFile();
                        ll.add(mSpillFiles.get(mSpillFiles.size() - 1));
                        Tuple t;
                        while ((t = readFromTree()) != null) {
                            t.write(out);
                        }
                        out.flush();
                        out.close();
                    } catch (IOException ioe) {
                        String msg = "Unable to find our spill file.";
                        log.fatal(msg, ioe);
                        throw new RuntimeException(msg, ioe);
                    }
                }

                // delete files that have been merged into new files
                for(File f : filesToDelete){
                    if( f.delete() == false){
                        log.warn("Failed to delete spill file: " + f.getPath());
                    }
                }

                // clear the list, so that finalize does not delete any files,
                // when mSpillFiles is assigned a new value
                mSpillFiles.clear();

                // Now, move our new list back to the spill files array.
                mSpillFiles = new FileList(ll);
            } finally {
                // Reset mStreams and mMerge so that they'll be allocated
                // properly for regular merging.
                mStreams = null;
                mMergeTree = null;
            }
        }
    }

    @Override
    public long spill(){
        synchronized(mContents) {
            if (this.mReadStarted) {
                return 0L;
            }
            return proactive_spill(null);
        }
    }

}
