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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.TupleRawComparator;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.io.PigNullableWritable;

public class PigSecondaryKeyComparator extends WritableComparator implements Configurable {
    private final Log mLog = LogFactory.getLog(getClass());
    private TupleRawComparator mComparator=null;

    @Override
    public void setConf(Configuration conf) {
        if (!(conf instanceof JobConf)) {
            mLog.warn("Expected jobconf in setConf, got " + conf.getClass().getName());
            return;
        }
        JobConf jconf = (JobConf) conf;
        try {
            Class<? extends TupleRawComparator> mComparatorClass = TupleFactory.getInstance().tupleRawComparatorClass();
            mComparator = mComparatorClass.newInstance();
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        mComparator.setConf(jconf);
    }

    protected PigSecondaryKeyComparator() {
        super(TupleFactory.getInstance().tupleClass());
    }

    @Override
    public Configuration getConf() {
        return null;
    }

    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

        // the last byte of a NullableTuple is its Index
        byte mIndex1 = b1[s1 + l1 - 1];
        byte mIndex2 = b2[s2 + l2 - 1];

        if ((mIndex1 & NullableTuple.mqFlag) != 0) { // multi-query index
            if ((mIndex1 & NullableTuple.idxSpace) < (mIndex2 & NullableTuple.idxSpace))
                return -1;
            else if ((mIndex1 & NullableTuple.idxSpace) > (mIndex2 & NullableTuple.idxSpace))
                return 1;
            // if equal, we fall through
        }
        int rc = 0;
        if (b1[s1] == 0 && b2[s2] == 0) {
            // skip mNull and mIndex
            rc = mComparator.compare(b1, s1 + 1, l1 - 2, b2, s2 + 1, l2 - 2);
            // handle PIG-927
            // if tuples are equal but any field inside tuple is null, then we do not merge keys
            if (rc == 0 && mComparator.hasComparedTupleNull())
                rc = (mIndex1 & PigNullableWritable.idxSpace) - (mIndex2 & PigNullableWritable.idxSpace);
        } else {
            // if they're both null, compare the indexes
            if (b1[s1] != 0 && b2[s2] != 0)
                rc = (mIndex1 & PigNullableWritable.idxSpace) - (mIndex2 & PigNullableWritable.idxSpace);
            // null sorts first
            else if (b1[s1] != 0)
                rc = -1;
            else
                rc = 1;
        }
        return rc;
    }
}
