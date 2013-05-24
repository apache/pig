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
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.io.FileNotFoundException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.PigCounters;
import org.apache.pig.PigWarning;


/**
 * An unordered collection of Tuples (possibly) with multiples.  The tuples
 * are stored in a List, since there is no concern for order or
 * distinctness.
 */
public class DefaultDataBag extends DefaultAbstractBag {

    /**
     * 
     */
    private static final long serialVersionUID = 2L;

    private static final Log log = LogFactory.getLog(DefaultDataBag.class);
    
    private static final InterSedes SEDES = InterSedesFactory.getInterSedesInstance();

    public DefaultDataBag() {
        mContents = new ArrayList<Tuple>();
    }

    /**
     * This constructor creates a bag out of an existing list
     * of tuples by taking ownership of the list and NOT
     * copying the contents of the list.
     * @param listOfTuples List<Tuple> containing the tuples
     */
    public DefaultDataBag(List<Tuple> listOfTuples) {
        mContents = listOfTuples;
        mSize = listOfTuples.size();
        markSpillableIfNecessary();
    }

    @Override
    public boolean isSorted() {
        return false;
    }
    
    @Override
    public boolean isDistinct() {
        return false;
    }
    
    @Override
    public Iterator<Tuple> iterator() {
        return new DefaultDataBagIterator();
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
            }  catch (IOException ioe) {
                // Do not remove last file from spilled array. It was not
                // added as File.createTmpFile threw an IOException
                warn(
                    "Unable to create tmp file to spill to disk", PigWarning.UNABLE_TO_CREATE_FILE_TO_SPILL, ioe);
                return 0;
            }
            try {
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
    private class DefaultDataBagIterator implements Iterator<Tuple> {
        // We have to buffer a tuple because there's no easy way for next
        // to tell whether or not there's another tuple available, other
        // than to read it.
        private Tuple mBuf = null;
        private int mMemoryPtr = 0;
        private int mFilePtr = 0;
        private DataInputStream mIn = null;
        private int mCntr = 0;
        private boolean hasCachedTuple = false;

        DefaultDataBagIterator() {
        }

        @Override
        public boolean hasNext() { 
            // Once we call hasNext(), set the flag, so we can call hasNext() repeated without fetching next tuple
            if (hasCachedTuple)
                return (mBuf != null);
            mBuf = next();
            hasCachedTuple = true;
            return (mBuf != null);
        }

        @Override
        public Tuple next() {
            // This will report progress every 1024 times through next.
            // This should be much faster than using mod.
            if ((mCntr++ & 0x3ff) == 0) reportProgress();

            // If there's one in the buffer, use that one.
            if (hasCachedTuple) {
                Tuple t = mBuf;
                hasCachedTuple = false;
                return t;
            }

            // See if we've been reading from memory or not.
            if (mMemoryPtr > 0) {
                // If there's still data in memory, keep reading from
                // there.
                // Lock before we check the size, obtain a reader lock,
                // from this point forward we can't have them spilling on
                // us.
                synchronized (mContents) {
                    if (mContents.size() > 0) {
                        return readFromMemory();
                    }
                }

                // The container spilled since our last read.  Don't
                // need to the hold the lock now, as it's already
                // spilled on us.

                // Our file pointer will already point to the new
                // spill file (because it was either already 0 or had
                // been incremented past the end of the old
                // mSpillFiles.size()).  We need to open the new file
                // and then fast forward past all of the tuples we've
                // already read.  Then we need to reset mMemoryPtr so
                // we know to read from the file next time we come
                // through.
                try {
                    mIn = new DataInputStream(new BufferedInputStream(
                        new FileInputStream(mSpillFiles.get(mFilePtr++))));
                } catch (FileNotFoundException fnfe) {
                    // We can't find our own spill file?  That should never
                    // happen.
                    String msg = "Unable to find our spill file."; 
                    log.fatal(msg, fnfe);
                    throw new RuntimeException(msg, fnfe);
                }
                for (int i = 0; i < mMemoryPtr; i++) {
                    try {
                        SEDES.readDatum(mIn);
                    } catch (EOFException eof) {
                        // This should never happen, it means we
                        // didn't dump all of our tuples to disk.
                        String msg = "Ran out of tuples to read prematurely.";
                        log.fatal(msg, eof);
                        throw new RuntimeException(msg, eof);
                    } catch (IOException ioe) {
                        String msg = "Unable to read our spill file."; 
                        log.fatal(msg, ioe);
                        throw new RuntimeException(msg, ioe);
                    }
                }
                mMemoryPtr = 0;
                return readFromFile();
            }

            // We haven't read from memory yet, so keep trying to read
            // from the file
            return readFromFile();
        }

        /**
         * Not implemented.
         */
        @Override
        public void remove() {}

        private Tuple readFromFile() {
            if (mIn != null) {
                // We already have a file open
                Tuple t;
                try {
                    t = (Tuple) SEDES.readDatum(mIn);
                    return t;
                } catch (EOFException eof) {
                    // Fall through to the next case where we find the
                    // next file, or go to memory
                    try {
                        mIn.close();
                    }catch(IOException e) {
                        log.warn("Failed to close spill file.", e);
                    }
                } catch (IOException ioe) {
                    String msg = "Unable to read our spill file."; 
                    log.fatal(msg, ioe);
                    throw new RuntimeException(msg, ioe);
                }
            }

            // Need to open the next file, if there is one.  Have to lock
            // here, because otherwise we could decide there's no more
            // files and between the time we decide that and start trying
            // to read from memory the container could spill, and then
            // we're stuck.  If there's another file to read, we can
            // unlock immediately.  If there isn't, we need to hold the
            // lock and go into readFromMemory().
            synchronized (mContents) {
                if (mSpillFiles == null || mFilePtr >= mSpillFiles.size()) {
                    // We've read everything there is to read from the files, go
                    // look in memory.
                    return readFromMemory();
                }
            }

            // Open the next file, then call ourselves again as it
            // will enter the if above.
            try {
                mIn = new DataInputStream(new BufferedInputStream(
                    new FileInputStream(mSpillFiles.get(mFilePtr++))));
            } catch (FileNotFoundException fnfe) {
                // We can't find our own spill file?  That should never
                // happen.
                String msg = "Unable to find our spill file.";
                log.fatal(msg, fnfe);
                throw new RuntimeException(msg, fnfe);
            }
            return readFromFile();
        }

        // This should only be called once we know we haven't spilled.  It
        // assumes that the mContents lock is already held before we enter
        // this function.
        private Tuple readFromMemory() {
            if (mContents.size() == 0) return null;

            if (mMemoryPtr < mContents.size()) {
                return ((List<Tuple>)mContents).get(mMemoryPtr++);
            } else {
                return null;
            }
        }
    }
}

