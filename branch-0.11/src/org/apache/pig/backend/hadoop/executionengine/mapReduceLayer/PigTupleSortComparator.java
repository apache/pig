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
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.TupleRawComparator;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.util.ObjectSerializer;

public class PigTupleSortComparator extends WritableComparator implements Configurable {

    private final Log mLog = LogFactory.getLog(getClass());
    private boolean[] mAsc;
    private boolean mWholeTuple;
    private TupleRawComparator mComparator=null;

    public PigTupleSortComparator() {
        super(TupleFactory.getInstance().tupleClass());
    }

    @Override
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
        try {
            Class<? extends TupleRawComparator> mComparatorClass = TupleFactory.getInstance().tupleRawComparatorClass();
            if (mComparatorClass!=null)
                mComparator = mComparatorClass.newInstance();
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (AbstractMethodError e) {
            // Tuple does not implement method getRawComparatorClass, or backward compatibility, we assign mComparator to null.
            // We will use conventional comparator in this case
        }
        if (mComparator==null) {
            try {
                mComparator = PigTupleDefaultRawComparator.class.newInstance();
            } catch (InstantiationException e) {
                throw new RuntimeException(e);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
        ((Configurable)mComparator).setConf(jconf);
    }

    @Override
    public Configuration getConf() {
        return null;
    }

    /**
     * Compare two NullableTuples as raw bytes. Tuples are compared field-wise. If both are null they are defined equal.
     * Otherwise the null one is defined to be less.
     */
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        int rc = 0;
        if (b1[s1] == 0 && b2[s2] == 0) {
            // skip mNull and mIndex
            rc = mComparator.compare(b1, s1 + 1, l1 - 2, b2, s2 + 1, l2 - 2);
        } else {
            // for sorting purposes two nulls are equal, null sorts first
            if (b1[s1] != 0 && b2[s2] != 0)
                rc = 0;
            else if (b1[s1] != 0)
                rc = -1;
            else
                rc = 1;
            if (mWholeTuple && !mAsc[0])
                rc *= -1;
        }
        return rc;
    }

    @SuppressWarnings("unchecked")
    public int compare(Object o1, Object o2) {
        NullableTuple nt1 = (NullableTuple) o1;
        NullableTuple nt2 = (NullableTuple) o2;
        int rc = 0;

        // If either are null, handle differently.
        if (!nt1.isNull() && !nt2.isNull()) {
            rc = mComparator.compare((Tuple) nt1.getValueAsPigType(), (Tuple) nt2.getValueAsPigType());
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
                    int c = DataType.compare(t1.get(i), t2.get(i));
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