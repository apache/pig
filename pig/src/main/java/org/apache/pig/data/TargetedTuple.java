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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.plan.OperatorKey;

/**
 * A tuple composed with the operators to which
 * it needs be attached
 *
 */
public class TargetedTuple extends AbstractTuple {
    /**
     * 
     */
    private static final long serialVersionUID = 2L;
    
    private Tuple t;
    // The list of operators to which this tuple
    // has to be attached as input.
    public List<OperatorKey> targetOps = null;

    protected boolean isNull = false;

    public TargetedTuple() {
    }

    public TargetedTuple(Tuple t, List<OperatorKey> targetOps) {
        this.t = t;
        this.targetOps = targetOps;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(super.toString());
        sb.append("[");
        for (OperatorKey target : targetOps) {
            sb.append(target.toString());
            sb.append(",");
        }
        sb.replace(sb.length() - 1, sb.length(), "]");
        return sb.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        t.write(out);
        out.writeInt(targetOps.size());
        for (OperatorKey target : targetOps) {
            // Ideally I should be able to call target.write(out).
            // Since it doesn't support it yet handling it here
            out.writeInt(target.scope.length());
            out.writeBytes(target.scope);
            out.writeLong(target.id);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        t.readFields(in);
        targetOps = new ArrayList<OperatorKey>();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            OperatorKey target = new OperatorKey();
            // Ideally I should be able to call target.read(in).
            // Since it doesn't support it yet handling it here
            int scopeSz = in.readInt();
            byte[] buf = new byte[scopeSz];
            in.readFully(buf);
            target.scope = new String(buf);
            target.id = in.readLong();
            targetOps.add(target);
        }
    }

    public Tuple toTuple() {
        return t;
    }

    public List<OperatorKey> getTargetOps() {
        return targetOps;
    }

    public void setTargetOps(List<OperatorKey> targetOps) {
        this.targetOps = targetOps;
    }

    @Override
    public void append(Object val) {
        t.append(val);
    }

    @Override
    public Object get(int fieldNum) throws ExecException {
        return t.get(fieldNum);
    }

    @Override
    public List<Object> getAll() {
        return t.getAll();
    }

    @Override
    public long getMemorySize() {
        return t.getMemorySize();
    }

    @Override
    public byte getType(int fieldNum) throws ExecException {
        return t.getType(fieldNum);
    }

    @Override
    public void reference(Tuple t) {
        this.t = t;
    }

    @Override
    public void set(int fieldNum, Object val) throws ExecException {
        t.set(fieldNum, val);
    }

    @Override
    public int size() {
        return t.size();
    }

    @Override
    @SuppressWarnings("unchecked")
    public int compareTo(Object o) {
        return t.compareTo(o);
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public boolean equals(Object o) {
        return t.equals(o);
    }

    @Override
    public int hashCode() {
        return t.hashCode();
    }
}
