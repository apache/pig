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
import org.apache.pig.PigCounters;
import org.apache.pig.PigWarning;


/**
 * An ordered collection of Tuples (possibly) with multiples.  Data is
 * stored unsorted as it comes in, and only sorted when it is time to dump
 * it to a file or when the first iterator is requested.  Experementation
 * found this to be the faster than storing it sorted to begin with.
 * 
 * We allow a user defined comparator, but provide a default comparator in
 * cases where the user doesn't specify one.
 */
public class SortedDataBag extends DefaultAbstractBag{

    /**
     * 
     */
    private static final long serialVersionUID = 2L;

    private static final InterSedes SEDES = InterSedesFactory.getInterSedesInstance();

    private static final Log log = LogFactory.getLog(SortedDataBag.class);

    transient private Comparator<Tuple> mComp;
    private boolean mReadStarted = false;

    private static class DefaultComparator implements Comparator<Tuple> {
        @Override
        @SuppressWarnings("unchecked")
        public int compare(Tuple t1, Tuple t2) {
            return t1.compareTo(t2);
        }

        @Override
        public boolean equals(Object o) {
            return false;
        }

        @Override
        public int hashCode() {
            return 42; 
        }

    }

    /**
     * @param comp Comparator to use to do the sorting.  If null,
     * DefaultComparator will be used.
     */
    public SortedDataBag(Comparator<Tuple> comp) {
        mComp = (comp == null) ? new DefaultComparator() : comp;

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

    @Override
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
                warn(
                    "Unable to create tmp file to spill to disk", PigWarning.UNABLE_TO_CREATE_FILE_TO_SPILL, ioe);
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
                    SEDES.writeDatum(out, i.next(), DataType.TUPLE);
                    spilled++;
                    // This will spill every 16383 records.
                    if ((spilled & 0x3fff) == 0) reportProgress();
                }
                out.flush();
            } catch (IOException ioe) {
                // Remove the last file from the spilled array, since we failed to
                // write to it.
                mSpillFiles.remove(mSpillFiles.size() - 1);
                warn(
                    "Unable to spill contents to disk", PigWarning.UNABLE_TO_SPILL, ioe);
                return 0;
            } finally {
                if (out != null) {
                    try {
                        out.close();
                    } catch (IOException e) {
                        warn("Error closing spill", PigWarning.UNABLE_TO_CLOSE_SPILL_FILE, e);
                    }
                }
            }
            mContents.clear();
        }
        // Increment the spill count
        incSpillCount(PigCounters.SPILLABLE_MEMORY_MANAGER_SPILL_COUNT);
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

            @Override
            public int compareTo(PQContainer other) {
                return mComp.compare(tuple, other.tuple);
            }

            @Override
            public boolean equals(Object other) {
                if (other instanceof PQContainer)
                    return tuple.equals(((PQContainer)other).tuple);
                else
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
                    String msg = "Unable to find our spill file.";
                    log.fatal(msg, fnfe);
                    throw new RuntimeException(msg, fnfe);
                }

                // Fast foward past the tuples we've already put in the
                // queue.
                for (int i = 0; i < mMemoryPtr; i++) {
                    try {
                        SEDES.readDatum(in);
                    } catch (EOFException eof) {
                        // This should never happen, it means we
                        // didn't dump all of our tuples to disk.
                        String msg = "Ran out of tuples to read prematurely.";
                        log.fatal(msg, eof);
                        throw new RuntimeException(msg, eof);
                    } catch (IOException ioe) {
                        String msg = "Unable to find our spill file.";
                        log.fatal(msg, ioe);
                        throw new RuntimeException(msg, ioe);
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
        @Override
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
                try {
                    c.tuple = (Tuple) SEDES.readDatum(in);
                    mMergeQ.add(c);
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
                } catch (IOException ioe) {
                    String msg = "Unable to find our spill file.";
                    log.fatal(msg, ioe);
                    throw new RuntimeException(msg, ioe);
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
                LinkedList<File> filesToDelete = new LinkedList<File>();
                while (ll.size() > MAX_SPILL_FILES) {
                    ListIterator<File> i = ll.listIterator();
                    mStreams =
                        new ArrayList<DataInputStream>(MAX_SPILL_FILES);
                    mMergeQ = new PriorityQueue<PQContainer>(MAX_SPILL_FILES);

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
                        while ((t = readFromPriorityQ()) != null) {
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
                mMergeQ = null;
            }
        }
    }
}

