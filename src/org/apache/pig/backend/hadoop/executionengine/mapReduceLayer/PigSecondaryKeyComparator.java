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
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.impl.util.ObjectSerializer;

public class PigSecondaryKeyComparator extends WritableComparator implements Configurable {
    private final Log mLog = LogFactory.getLog(getClass());
    private boolean[] mAsc;
    private boolean[] mSecondaryAsc;
    private boolean mWholeTuple;
    private boolean mSecondaryWholeTuple;
    private final DataInputBuffer buffer;
    private final NullableTuple key1;
    private final NullableTuple key2;
    @Override
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
            mSecondaryAsc = (boolean[])ObjectSerializer.deserialize(jconf.get(
                "pig.secondarySortOrder"));
        } catch (IOException ioe) {
            mLog.error("Unable to deserialize sort order object" +
                ioe.getMessage());
            throw new RuntimeException(ioe);
        }
        if (mAsc == null) {
            mAsc = new boolean[1];
            mAsc[0] = true;
        }
        if (mSecondaryAsc == null) {
            mSecondaryAsc = new boolean[1];
            mSecondaryAsc[0] = true;
        }
        mWholeTuple = (mAsc.length == 1);
        mSecondaryWholeTuple = (mSecondaryAsc.length == 1);
    }
    public Configuration getConf() {
        return null;
    }
    @SuppressWarnings("unchecked")
    public PigSecondaryKeyComparator() {
        super(TupleFactory.getInstance().tupleClass());
        buffer = new DataInputBuffer();
        key1 = new NullableTuple();
        key2 = new NullableTuple();
    }
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2){
        try {
            buffer.reset(b1, s1, l1);                   // parse key1
            key1.readFields(buffer);
            
            buffer.reset(b2, s2, l2);                   // parse key2
            key2.readFields(buffer);
            
            if ((key1.getIndex() & NullableTuple.mqFlag) != 0) { // this is a multi-query index
                if ((key1.getIndex() & NullableTuple.idxSpace) < (key2.getIndex() & NullableTuple.idxSpace)) return -1;
                else if ((key1.getIndex() & NullableTuple.idxSpace) > (key2.getIndex() & NullableTuple.idxSpace)) return 1;
                // If equal, we fall through
            }
            
            Tuple t1 = (Tuple)key1.getValueAsPigType();  // t1 and t2 is guaranteed to be not null
            Tuple t2 = (Tuple)key2.getValueAsPigType();
            
            Object k1 = t1.get(0);
            Object k2 = t2.get(0);
            
            int r = compare(k1, k2, key1.getIndex(), key2.getIndex(), mAsc, mWholeTuple);
            
            if (r!=0)
                return r;
            
            // main key is equal, compare the secondary key
            k1 = t1.get(1);
            k2 = t2.get(1);
            
            r = compare(k1, k2, key1.getIndex(), key2.getIndex(), mSecondaryAsc, mSecondaryWholeTuple);
            
            return r;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public int compare(Object k1, Object k2, int index1, int index2, boolean[] ascs, boolean wholeTuple) {
        try {
            if (k1!=null && k2!=null) {
                if (k1 instanceof Tuple && k2 instanceof Tuple) {
                    Tuple t1 = (Tuple)k1;
                    Tuple t2 = (Tuple)k2;
                    int sz1 = t1.size();
                    int sz2 = t2.size();
                    if (sz2 < sz1) {
                        return 1;
                    } else if (sz2 > sz1) {
                        return -1;
                    } else {
                        for (int i = 0; i < sz1; i++) {
                            int c = DataType.compare(t1.get(i), t2.get(i));
                            if (c != 0) {
                                if (!wholeTuple && !ascs[i]) c *= -1;
                                else if (wholeTuple && !ascs[0]) c *= -1;
                                return c;
                            }
                        }
                        // If any of the field inside tuple is null, then we do not merge keys
                        // See PIG-927
                        for (int i=0;i<t1.size();i++)
                            if (t1.get(i)==null)
                                return (index1& PigNullableWritable.idxSpace) - (index2& PigNullableWritable.idxSpace);
                        return 0;
                    }
                }
                int c = DataType.compare(k1, k2);
                if (!ascs[0]) c *= -1;
                return c;
            }
            else if (k1==null && k2==null) {
                // If they're both null, compare the index
                return (index1& PigNullableWritable.idxSpace) - (index2& PigNullableWritable.idxSpace);
            }
            else if (k1==null) return -1; 
            else return 1;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
