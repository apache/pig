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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BinInterSedes;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.TupleRawComparator;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.util.ObjectSerializer;

public class PigTupleDefaultRawComparator extends WritableComparator implements TupleRawComparator {

    private final Log mLog = LogFactory.getLog(getClass());
    private boolean[] mAsc;
    private boolean mWholeTuple;
    private boolean mHasNullField;

    public PigTupleDefaultRawComparator() {
        super(TupleFactory.getInstance().tupleClass());
    }

    public void setConf(Configuration conf) {
        if (!(conf instanceof JobConf)) {
            mLog.warn("Expected jobconf in setConf, got " + conf.getClass().getName());
            return;
        }
        JobConf jconf = (JobConf) conf;
        try {
            mAsc = (boolean[]) ObjectSerializer.deserialize(jconf.get("pig.sortOrder"));
        } catch (IOException ioe) {
            mLog.error("Unable to deserialize pig.sortOrder " + ioe.getMessage());
            throw new RuntimeException(ioe);
        }
        if (mAsc == null) {
            mAsc = new boolean[1];
            mAsc[0] = true;
        }
        // If there's only one entry in mAsc, it means it's for the whole
        // tuple. So we can't be looking for each column.
        mWholeTuple = (mAsc.length == 1);
    }

    public Configuration getConf() {
        return null;
    }

    @Override
    public boolean hasComparedTupleNull() {
        return mHasNullField;
    }
    
    private static final BinInterSedes bis = new BinInterSedes();

    /**
     * Compare two NullableTuples as raw bytes. If neither are null, then
     * IntWritable.compare() is used. If both are null then the indices are
     * compared. Otherwise the null one is defined to be less.
     */
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        int rc = 0;
        mHasNullField = false;

        Tuple t1;
        Tuple t2;

        // This can't be done on the raw data. Users are allowed to
        // implement their own versions of tuples, which means we have no
        // idea what the underlying representation is. So step one is to
        // instantiate each object as a tuple.
        try {
            t1 = bis.readTuple(new DataInputStream(new ByteArrayInputStream(b1, s1, l1)));
            t2 = bis.readTuple(new DataInputStream(new ByteArrayInputStream(b2, s2, l2)));
        } catch (IOException ioe) {
            mLog.error("Unable to instantiate tuples for comparison: " + ioe.getMessage());
            throw new RuntimeException(ioe.getMessage(), ioe);
        }

        rc = compareTuple(t1, t2); //TODO think about how SchemaTuple could speed this up

        return rc;
    }

    public int compare(Object o1, Object o2) {
        NullableTuple nt1 = (NullableTuple) o1;
        NullableTuple nt2 = (NullableTuple) o2;
        int rc = 0;

        // If either are null, handle differently.
        if (!nt1.isNull() && !nt2.isNull()) {
            rc = compareTuple((Tuple) nt1.getValueAsPigType(), (Tuple) nt2.getValueAsPigType());
        } else {
            // For sorting purposes two nulls are equal.
            if (nt1.isNull() && nt2.isNull())
                rc = 0;
            else if (nt1.isNull())
                rc = -1;
            else
                rc = 1;
            if (mWholeTuple && !mAsc[0])
                rc *= -1;
        }
        return rc;
    }

    private int compareTuple(Tuple t1, Tuple t2) {
        int sz1 = t1.size();
        int sz2 = t2.size();
        if (sz2 < sz1) {
            return 1;
        } else if (sz2 > sz1) {
            return -1;
        } else {
            for (int i = 0; i < sz1; i++) {
                try {
                    Object o1 = t1.get(i);
                    Object o2 = t2.get(i);
                    if (o1==null || o2==null)
                        mHasNullField = true;
                    int c = DataType.compare(o1, o2);
                    if (c != 0) {
                        if (!mWholeTuple && !mAsc[i])
                            c *= -1;
                        else if (mWholeTuple && !mAsc[0])
                            c *= -1;
                        return c;
                    }
                } catch (ExecException e) {
                    throw new RuntimeException("Unable to compare tuples", e);
                }
            }
            return 0;
        }
    }

}
