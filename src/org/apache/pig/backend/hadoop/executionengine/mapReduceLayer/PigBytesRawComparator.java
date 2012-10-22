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
package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.pig.data.BinInterSedes;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.io.NullableBytesWritable;
import org.apache.pig.impl.util.ObjectSerializer;

public class PigBytesRawComparator extends WritableComparator implements Configurable {

    private final Log mLog = LogFactory.getLog(getClass());
    private boolean[] mAsc;
    private WritableComparator mWrappedComp;

    public PigBytesRawComparator() {
        super(NullableBytesWritable.class);
        mWrappedComp = new BinInterSedes.BinInterSedesTupleRawComparator();
    }

    public void setConf(Configuration conf) {
        if (!(conf instanceof JobConf)) {
            mLog.warn("Expected jobconf in setConf, got " +
                conf.getClass().getName());
            return;
        }
        JobConf jconf = (JobConf)conf;
        try {
            mAsc = (boolean[])ObjectSerializer.deserialize(jconf.get(
                "pig.sortOrder"));
        } catch (IOException ioe) {
            mLog.error("Unable to deserialize pig.sortOrder " +
                ioe.getMessage());
            throw new RuntimeException(ioe);
        }
        if (mAsc == null) {
            mAsc = new boolean[1];
            mAsc[0] = true;
        }
        ((BinInterSedes.BinInterSedesTupleRawComparator)mWrappedComp).setConf(conf);
    }

    public Configuration getConf() {
        return null;
    }

    /**
     * Compare two NullableBytesWritables as raw bytes.
     * If both are null, then the indices are compared.
     * If neither are null
     *    and both are bytearrays, then direct Writable.compareBytes is used.
     *    For non-bytearrays, we use BinInterSedesTupleRawComparator.
     * If either is null, null one is defined to be less.
     */
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        int rc = 0;

        // If either are null, handle differently.
        if (b1[s1] == 0 && b2[s2] == 0) {
            // Checking if both of them are ByteArrays.
            // b1[s1+1] is always TUPLE_1 so skipping.
            // b1[s1+2] contains the datatype followed by a datasize.
            // No need to read the actual size since it is given from l1&l2.
            // We just need to skip those bytes here.
            // This shortcut is placed here for performances purposes.(PIG-2975)
            boolean dataByteArraysCompare=true;
            int offset1,offset2,length1,length2;
            switch(b1[s1 + 2]) {
              case BinInterSedes.TINYBYTEARRAY:
                // skipping mNull, TUPLE_1, TINYBYTEARRAY(1byte), size(1byte)
                offset1 = s1 + 4;
                length1 = l1 - 4;
                break;
              case BinInterSedes.SMALLBYTEARRAY:
                // skipping mNull, TUPLE_1, SMALLBYTEARRAY(1byte), size(2byte)
                offset1 = s1 + 5;
                length1 = l1 - 5;
                break;
              case BinInterSedes.BYTEARRAY:
                // skipping mNull, TUPLE_1, BYTEARRAY(1byte), size(4byte)
                offset1 = s1 + 7;
                length1 = l1 - 7;
                break;
              default:
                offset1 = length1 = 0;
                dataByteArraysCompare = false;
            }
            switch(b2[s2 + 2]) {
              case BinInterSedes.TINYBYTEARRAY:
                offset2 = s2 + 4;
                length2 = l2 - 4;
                break;
              case BinInterSedes.SMALLBYTEARRAY:
                offset2 = s2 + 5;
                length2 = l2 - 5;
                break;
              case BinInterSedes.BYTEARRAY:
                offset2 = s2 + 7;
                length2 = l2 - 7;
                break;
              default:
                offset2 = length2 = 0;
                dataByteArraysCompare=false;
            }
            if( dataByteArraysCompare ) {
              rc = WritableComparator.compareBytes(b1, offset1, length1, b2, offset2, length2);
            } else {
              // Subtract 2, one for null byte and one for index byte
              rc = mWrappedComp.compare(b1, s1 + 1, l1 - 2, b2, s2 + 1, l2 - 2);
            }
        } else {
            // For sorting purposes two nulls are equal.
            if (b1[s1] != 0 && b2[s2] != 0) rc = 0;
            else if (b1[s1] != 0) rc = -1;
            else rc = 1;
        }
        if (!mAsc[0]) rc *= -1;
        return rc;
    }

    public int compare(Object o1, Object o2) {
        NullableBytesWritable nbw1 = (NullableBytesWritable)o1;
        NullableBytesWritable nbw2 = (NullableBytesWritable)o2;
        int rc = 0;

        // If either are null, handle differently.
        if (!nbw1.isNull() && !nbw2.isNull()) {
            rc = DataType.compare(nbw1.getValueAsPigType(), nbw2.getValueAsPigType());
        } else {
            // For sorting purposes two nulls are equal.
            if (nbw1.isNull() && nbw2.isNull()) rc = 0;
            else if (nbw1.isNull()) rc = -1;
            else rc = 1;
        }
        if (!mAsc[0]) rc *= -1;
        return rc;
    }

}
