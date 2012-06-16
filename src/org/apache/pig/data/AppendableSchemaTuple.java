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

import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;
import org.apache.pig.data.utils.HierarchyHelper.MustOverride;
import org.apache.pig.data.utils.SedesHelper;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public abstract class AppendableSchemaTuple<T extends AppendableSchemaTuple<T>> extends SchemaTuple<T> {
    private static final long serialVersionUID = 1L;

    private Tuple appendedFields;

    private static final TupleFactory mTupleFactory = TupleFactory.getInstance();

    @Override
    public void append(Object val) {
        if (appendedFields == null) {
            appendedFields = mTupleFactory.newTuple();
        }

        appendedFields.append(val);
    }

    protected int appendedFieldsSize() {
        return appendedFields == null ? 0 : appendedFields.size();
    }

    protected boolean isAppendedFieldsNull() {
        return appendedFieldsSize() == 0;
    }

    protected Object getAppendedField(int i) throws ExecException {
        return isAppendedFieldNull(i) ? null : appendedFields.get(i);
    }

    private boolean isAppendedFieldNull(int i) throws ExecException {
        return isAppendedFieldsNull() || appendedFields.isNull(i);
    }

    //protected Tuple getAppend() {
    public Tuple getAppendedFields() {
        return appendedFields;
    }

    protected void setAppendedFields(Tuple t) {
        appendedFields = t;
    }

    private void resetAppendedFields() {
        appendedFields = null;
    }

    private void setAppendedField(int fieldNum, Object val) throws ExecException {
        appendedFields.set(fieldNum, val);
    }

    /**
     * This adds the additional overhead of the append Tuple
     */
    @Override
    public long getMemorySize() {
        return SizeUtil.roundToEight(appendedFields.getMemorySize()) + super.getMemorySize();
    }


    private byte getAppendedFieldType(int i) throws ExecException {
        return appendedFields == null ? DataType.UNKNOWN : appendedFields.getType(i);
    }

    protected SchemaTuple<T> set(SchemaTuple<?> t, boolean checkType) throws ExecException {
        resetAppendedFields();
        for (int j = schemaSize(); j < t.size(); j++) {
            append(t.get(j));
        }
        return super.set(t, checkType);
    }

    protected SchemaTuple<T> setSpecific(T t) {
        resetAppendedFields();
        setAppendedFields(t.getAppendedFields());
        return super.setSpecific(t);
    }

    public SchemaTuple<T> set(List<Object> l) throws ExecException {
        if (l.size() < schemaSize())
            throw new ExecException("Given list of objects has too few fields ("+l.size()+" vs "+schemaSize()+")");

        for (int i = 0; i < schemaSize(); i++)
            set(i, l.get(i));

        resetAppendedFields();

        for (int i = schemaSize(); i < l.size(); i++) {
            append(l.get(i++));
        }

        return this;
    }

    protected int compareTo(SchemaTuple<?> t, boolean checkType) {
        int i = super.compareTo(t, checkType);
        if (i != 0) {
            return i;
        }
        if (appendedFieldsSize() > 0) {
            int m = schemaSize();
            for (int k = 0; k < size() - schemaSize(); k++) {
                try {
                    i = DataType.compare(getAppendedField(k), t.get(m++));
                } catch (ExecException e) {
                    throw new RuntimeException("Unable to get append value", e);
                }
                if (i != 0) {
                    return i;
                }
            }
        }
        return 0;
    }

    @MustOverride
    protected int compareToSpecific(T t) {
        int i = super.compareToSpecific(t);
        if (i != 0) {
            return i;
        }
        for (int z = 0; z < appendedFieldsSize(); z++) {
            try {
                i = DataType.compare(getAppendedField(z), t.getAppendedField(z));
            } catch (ExecException e) {
                throw new RuntimeException("Unable to get append", e);
            }
            if (i != 0) {
                return i;
            }
        }
        return 0;
    }

    public int hashCode() {
        return super.hashCode() + appendedFields.hashCode();
    }

    public void set(int fieldNum, Object val) throws ExecException {
        int diff = fieldNum - schemaSize();
        if (diff >= 0 && diff < appendedFieldsSize()) {
            setAppendedField(diff, val);
        } else {
            super.set(fieldNum, val);
        }
    }

    @Override
    public Object get(int fieldNum) throws ExecException {
        int diff = fieldNum - schemaSize();
        if (diff >= 0 && diff < appendedFieldsSize()) {
            return getAppendedField(diff);
        } else {
            return super.get(fieldNum);
        }
    }

    @Override
    public boolean isNull(int fieldNum) throws ExecException {
        int diff = fieldNum - schemaSize();
        if (diff >= 0 && diff < appendedFieldsSize()) {
            return isAppendedFieldNull(diff);
        } else {
            return super.isNull(fieldNum);
        }
    }

    @Override
    public byte getType(int fieldNum) throws ExecException {
        int diff = fieldNum - schemaSize();
        if (diff >= 0 && diff < appendedFieldsSize()) {
            return getAppendedFieldType(diff);
        } else {
            return super.getType(fieldNum);
        }
    }

    @Override
    protected void setTypeAwareBase(int fieldNum, Object val, String type) throws ExecException {
        int diff = fieldNum - schemaSize();
        if (diff >= 0 && diff < appendedFieldsSize()) {
            setAppendedField(diff, val);
        } else {
            super.setTypeAwareBase(fieldNum, val, type);
        }
    }

    @Override
    protected Object getTypeAwareBase(int fieldNum, String type) throws ExecException {
        int diff = fieldNum - schemaSize();
        if (diff >= 0 && diff < appendedFieldsSize()) {
            return getAppendedField(diff);
        } else {
            return super.getTypeAwareBase(fieldNum, type);
        }
    }

    protected void writeElements(DataOutput out) throws IOException {
        super.writeElements(out);
        if (!isAppendedFieldsNull()) {
            SedesHelper.writeGenericTuple(out, getAppendedFields());
        }
    }

    @MustOverride
    protected int compareSizeSpecific(T t) {
        int mySz = appendedFieldsSize();
        int tSz = t.appendedFieldsSize();
        if (mySz != tSz) {
            return mySz > tSz ? 1 : -1;
        }
        return 0;
    }
}
