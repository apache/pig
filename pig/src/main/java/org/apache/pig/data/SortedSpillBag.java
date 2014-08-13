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

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.pig.PigCounters;
import org.apache.pig.PigWarning;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;

/**
 * Common functionality for proactively spilling bags that need to keep the data
 * sorted. 
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class SortedSpillBag extends SelfSpillBag {

    private static final long serialVersionUID = 1L;

    SortedSpillBag(int bagCount, float percent){
        super(bagCount, percent);
    }

    /**
     * Sort contents of mContents and write them to disk
     * @param comp Comparator to sort contents of mContents
     * @return number of tuples spilled
     */
    public long proactive_spill(Comparator<Tuple> comp) {
        // Make sure we have something to spill.  Don't create empty
        // files, as that will make a mess.
        if (mContents.size() == 0) return 0;

        //count for number of objects that have spilled
        if(mSpillFiles == null)
            incSpillCount(PigCounters.PROACTIVE_SPILL_COUNT_BAGS);
        
        long spilled = 0;
        
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
            //sort the tuples
            // as per documentation of collection.sort(), it copies to an array,
            // sorts and copies back to collection
            // Avoiding that extra copy back to collection (mContents) by 
            // copying to an array and using Arrays.sort
            Tuple[] array = new Tuple[mContents.size()];
            mContents.toArray(array);
            if(comp == null)
                Arrays.sort(array);
            else 
                Arrays.sort(array,comp);

            //dump the array
            for (Tuple t : array) {
                t.write(out);
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
        
        incSpillCount(PigCounters.PROACTIVE_SPILL_COUNT_RECS, spilled);
        
        return spilled;
    }
    
}
