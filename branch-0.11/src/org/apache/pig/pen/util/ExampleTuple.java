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

package org.apache.pig.pen.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.AbstractTuple;

//Example tuple adds 2 booleans to Tuple
//synthetic say whether the tuple was generated synthetically
//omittable is for future use in case we want to attach weights to tuples that have been displayed earlier
public class ExampleTuple extends AbstractTuple {
    private static final long serialVersionUID = 2L;

    public boolean synthetic = false;
    public boolean omittable = true;
    Object expr = null;
    Tuple t = null;

    public ExampleTuple() {

    }
    
    public ExampleTuple(Object expr) {
      this.expr = expr;
    }

    public ExampleTuple(Tuple t) {
        // Have to do it like this because Tuple is an interface, we don't
        // have access to its internal structures.
        this.t = t;
    }

    @Override
    public String toString() {
        return t.toString();
    }

    // Writable methods:
    @Override
    public void write(DataOutput out) throws IOException {
        t.write(out);
        out.writeBoolean(synthetic);
        out.writeBoolean(omittable);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        t.readFields(in);
        this.synthetic = in.readBoolean();
        this.omittable = in.readBoolean();
    }

    public Tuple toTuple() {
        return t;
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
    public boolean isNull(int fieldNum) throws ExecException {
        return t.isNull(fieldNum);
    }

    @Override
    public void reference(Tuple t) {
        t.reference(t);
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
}
