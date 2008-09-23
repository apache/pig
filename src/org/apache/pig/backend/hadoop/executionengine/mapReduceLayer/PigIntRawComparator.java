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

import org.apache.pig.impl.io.NullableIntWritable;
import org.apache.pig.impl.util.ObjectSerializer;

public class PigIntRawComparator extends WritableComparator implements Configurable {

    private final Log mLog = LogFactory.getLog(getClass());
    private boolean[] mAsc;

    public PigIntRawComparator() {
        super(NullableIntWritable.class);
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
    }

    public Configuration getConf() {
        return null;
    }

    /**
     * Compare two NullableIntWritables as raw bytes.  If neither are null,
     * then IntWritable.compare() is used.  If both are null then the indices
     * are compared.  Otherwise the null one is defined to be less.
     */
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        int rc = 0;

        // If either are null, handle differently.
        if (b1[s1] == 0 && b2[s2] == 0) {
            int int1 = readInt(b1, s1 + 1);
            int int2 = readInt(b2, s2 + 1);
            rc = (int1 < int2) ? -1 : ((int1 > int2) ? 1 : 0); 
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
        NullableIntWritable niw1 = (NullableIntWritable)o1;
        NullableIntWritable niw2 = (NullableIntWritable)o2;
        int rc = 0;

        // If either are null, handle differently.
        if (!niw1.isNull() && !niw2.isNull()) {
            rc = ((Integer)niw1.getValueAsPigType()).compareTo((Integer)niw2.getValueAsPigType());
        } else {
            // For sorting purposes two nulls are equal.
            if (niw1.isNull() && niw2.isNull()) rc = 0;
            else if (niw1.isNull()) rc = -1;
            else rc = 1;
        }
        if (!mAsc[0]) rc *= -1;
        return rc;
    }


}
