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
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.PriorityQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.impl.eval.EvalSpec;



/**
 * An ordered collection of Tuples (possibly) with multiples.  Data is
 * stored unsorted in an ArrayList as it comes in, and only sorted when it
 * is time to dump
 * it to a file or when the first iterator is requested.  Experementation
 * found this to be the faster than storing it sorted to begin with.
 * 
 * We allow a user defined comparator, but provide a default comparator in
 * cases where the user doesn't specify one.
 */
public class SortedDataBag extends DataBag {

    private static final Log log = LogFactory.getLog(SortedDataBag.class);

    private Comparator<Tuple> mComp;
    private boolean mReadStarted = false;

    private class DefaultComparator implements Comparator<Tuple> {
        public int compare(Tuple t1, Tuple t2) {
            return t1.compareTo(t2);
        }

        public boolean equals(Object o) {
            return false;
        }

    }

    /**
     * @param spec EvalSpec to use to do the sorting. spec.getComparator()
     * will be called to populate our mComp field.  If null,
     * DefaultComparator will be used.
     */
    public SortedDataBag(EvalSpec spec) {
        if (spec == null) {
            mComp = new DefaultComparator();
        } else {
            mComp = spec.getComparator();
        }

        mContents = new ArrayList<Tuple>();
    }

    @Override
    public boolean isSorted() {
        return true;
    }
    
    @Override
    public boolean isDistinct() {
        return false;
    }
    
    @Override
    public Iterator<Tuple> iterator() {
        return new SortedDataBagIterator();
    }

    public long spill() {
        // Make sure we have something to spill.  Don't create empty
        // files, as that will make a mess.
        if (mContents.size() == 0) return 0;

        // Lock the container before I spill, so that iterators aren't
        // trying to read while I'm mucking with the container.
        long spilled = 0;
        synchronized (mContents) {
            DataOutputStream out = null;
            try {
                out = getSpillFile();
            } catch (IOException ioe) {
                // Do not remove last file from spilled array. It was not
                // added as File.createTmpFile threw an IOException
                log.error(
                    "Unable to create tmp file to spill to disk", ioe);
                return 0;
            }
            try {
                // Have to sort the data before we can dump it.  It's bogus
                // that we have to do this under the lock, but there's no way
                // around it.  If the reads alread started, then we've
                // already sorted it.  No reason to do it again.  Don't
                // set mReadStarted, because we could still be in the add
                // phase, in which case more (unsorted) will be added
                // later.
                if (!mReadStarted) {
                    Collections.sort((ArrayList<Tuple>)mContents, mComp);
                }
                Iterator<Tuple> i = mContents.iterator();
                while (i.hasNext()) {
                    i.next().write(out);
                    spilled++;
                    // This will spill every 16383 records.
                    if ((spilled & 0x3fff) == 0) reportProgress();
                }
                out.flush();
            } catch (IOException ioe) {
                // Remove the last file from the spilled array, since we failed to
                // write to it.
                mSpillFiles.remove(mSpillFiles.size() - 1);
                log.error(
                    "Unable to spill contents to disk", ioe);
                return 0;
            } finally {
                if (out != null) {
                    try {
                        out.close();
                    } catch (IOException e) {
                        log.error("Error closing spill", e);
                    }
                }
            }
            mContents.clear();
        }
        return spilled;
    }

    /**
     * An iterator that handles getting the next tuple from the bag.  This
     * iterator has a couple of issues to deal with.  First, data can be
     * stored in a combination of in memory and on disk.  Second, the bag
     * may be asked to spill while the iterator is reading it.  This means
     * that it will be pointing to someplace in memory and suddenly it
     * will need to switch to a disk file.
     */
    private class SortedDataBagIterator implements Iterator<Tuple> {

        /**
         * A container to hold tuples in a priority queue.  Stores the
         * file number the tuple came from, so that when the tuple is read
         * out of the queue, we know which file to read its replacement
         * tuple from.
         */
        private class PQContainer implements Comparable<PQContainer> {
            public Tuple tuple;
            public int fileNum;

            public int compareTo(PQContainer other) {
                return mComp.compare(tuple, other.tuple);
            }
        }

        // We have to buffer a tuple because there's no easy way for next
        // to tell whether or not there's another tuple available, other
        // than to read it.
        private Tuple mBuf = null;
        private int mMemoryPtr = 0;
        private PriorityQueue<PQContainer> mMergeQ = null;
        private ArrayList<DataInputStream> mStreams = null;
        private int mCntr = 0;

        SortedDataBagIterator() {
            // If this is the first read, we need to sort the data.
            synchronized (mContents) {
                if (!mReadStarted) {
                    preMerge();
                    Collections.sort((ArrayList<Tuple>)mContents, mComp);
                    mReadStarted = true;
                }
            }
        }

        public boolean hasNext() { 
            // See if we can find a tuple.  If so, buffer it.
            mBuf = next();
            return mBuf != null;
        }

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
            boolean spilled = false;
            synchronized (mContents) {
                if (mSpillFiles == null || mSpillFiles.size() == 0) {
                    return readFromMemory();
                }

                // Check to see if we were reading from memory but we spilled
                if (mMemoryPtr > 0 && mContents.size() == 0) {
                    spilled = true;
                }
            }

            if (spilled) {
                DataInputStream in;
                // We need to open the new file
                // and then fast forward past all of the tuples we've
                // already read.  Then we need to place the first tuple
                // from that file in the priority queue.  Whatever tuples
                // from memory that were already in the queue will be fine,
                // as they're guaranteed to be ahead of the point we fast
                // foward to.
                // We're guaranteed that the file we want to read from for
                // the fast forward is the last element in mSpillFiles,
                // because we don't support calls to add() after calls to
                // iterator(), and spill() won't create empty files.
                try {
                    in = new DataInputStream(new BufferedInputStream(
                        new FileInputStream(mSpillFiles.get(
                                mSpillFiles.size() - 1))));
                    if (mStreams == null) {
                        // We didn't have any files before this spill.
                        mMergeQ = new PriorityQueue<PQContainer>(1);
                        mStreams = new ArrayList<DataInputStream>(1);
                    }
                    mStreams.add(in);
                } catch (FileNotFoundException fnfe) {
                    // We can't find our own spill file?  That should never
                    // happen.
                    log.fatal(
                        "Unable to find our spill file", fnfe);
                    throw new RuntimeException(fnfe);
                }

                // Fast foward past the tuples we've already put in the
                // queue.
                Tuple t = new Tuple();
                for (int i = 0; i < mMemoryPtr; i++) {
                    try {
                        t.readFields(in);
                    } catch (EOFException eof) {
                        // This should never happen, it means we
                        // didn't dump all of our tuples to disk.
                        log.fatal(
                            "Ran out of tuples too soon.", eof);
                        throw new RuntimeException("Ran out of tuples to read prematurely.", eof);
                    } catch (IOException ioe) {
                        log.fatal(
                            "Unable to read our spill file", ioe);
                        throw new RuntimeException(ioe);
                    }
                }
                mMemoryPtr = 0;
                // Add the next tuple from this file to the queue.
                addToQueue(null, mSpillFiles.size() - 1);
                // Fall through to read the next entry from the priority
                // queue.
            }

            // We have spill files, so we need to read the next tuple from
            // one of those files or from memory.
            return readFromPriorityQ();
        }

        /**
         * Not implemented.
         */
        public void remove() {}

        private Tuple readFromPriorityQ() {
            if (mMergeQ == null) {
                // First read, we need to set up the queue and the array of
                // file streams
                // Add one to the size for the list in memory.
                mMergeQ =
                    new PriorityQueue<PQContainer>(mSpillFiles.size() + 1);

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
                        log.fatal(
                            "Unable to find our spill file", fnfe);
                        throw new RuntimeException(fnfe);
                    }
                }

                // Prime one from memory too
                if (mContents.size() > 0) {
                    addToQueue(null, -1);
                }
            }

            // Pop the top one off the queue
            PQContainer c = mMergeQ.poll();
            if (c == null) return null;

            // Add the next tuple from whereever we read from into the
            // queue.  Buffer the tuple we're returning, as we'll be
            // reusing c.
            Tuple t = c.tuple;
            addToQueue(c, c.fileNum);

            return t;
        }

        private void addToQueue(PQContainer c, int fileNum) {
            if (c == null) {
                c = new PQContainer();
            }
            c.fileNum = fileNum;

            if (fileNum == -1) {
                // Need to read from memory.  We may have spilled since
                // this tuple was put in the queue, and hence memory might
                // be empty.  But I don't care, as then I just won't add
                // any more from memory.
                synchronized (mContents) {
                    c.tuple = readFromMemory();
                }
                if (c.tuple != null) {
                    mMergeQ.add(c);
                }
                return;
            }

            // Read the next tuple from the indicated file
            DataInputStream in = mStreams.get(fileNum);
            if (in != null) {
                // There's still data in this file
                c.tuple = new Tuple();
                try {
                    c.tuple.readFields(in);
                    mMergeQ.add(c);
                } catch (EOFException eof) {
                    // Out of tuples in this file.  Set our slot in the
                    // array to null so we don't keep trying to read from
                    // this file.
                    mStreams.set(fileNum, null);
                } catch (IOException ioe) {
                    log.fatal(
                        "Unable to read our spill file", ioe);
                    throw new RuntimeException(ioe);
                }

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
         * tuples.  This is expensive, but I need to do this in order to
         * use the sort spec that was provided to me.
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
                while (ll.size() > MAX_SPILL_FILES) {
                    ListIterator<File> i = ll.listIterator();
                    mStreams =
                        new ArrayList<DataInputStream>(MAX_SPILL_FILES);
                    mMergeQ = new PriorityQueue<PQContainer>(MAX_SPILL_FILES);

                    for (int j = 0; j < MAX_SPILL_FILES; j++) {
                        try {
                            DataInputStream in =
                                new DataInputStream(new BufferedInputStream(
                                    new FileInputStream(i.next())));
                            mStreams.add(in);
                            addToQueue(null, mStreams.size() - 1);
                            i.remove();
                        } catch (FileNotFoundException fnfe) {
                            // We can't find our own spill file?  That should
                            // neer happen.
                            log.fatal(
                                "Unable to find our spill file", fnfe);
                            throw new RuntimeException(fnfe);
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
                        while ((t = readFromPriorityQ()) != null) {
                            t.write(out);
                        }
                        out.flush();
                    } catch (IOException ioe) {
                        log.fatal(
                            "Unable to read our spill file", ioe);
                        throw new RuntimeException(ioe);
                    }
                }

                // Now, move our new list back to the spill files array.
                mSpillFiles = new ArrayList<File>(ll);
            } finally {
                // Reset mStreams and mMerge so that they'll be allocated
                // properly for regular merging.
                mStreams = null;
                mMergeQ = null;
            }
        }
    }
}

