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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.joda.time.DateTime;

public interface TypeAwareTuple extends Tuple {
    public void setInt(int idx, int val) throws ExecException;
    public void setFloat(int idx, float val) throws ExecException;
    public void setDouble(int idx, double val) throws ExecException;
    public void setLong(int idx, long val) throws ExecException;
    public void setString(int idx, String val) throws ExecException;
    public void setBoolean(int idx, boolean val) throws ExecException;
    public void setBigInteger(int idx, BigInteger val) throws ExecException;
    public void setBigDecimal(int idx, BigDecimal val) throws ExecException;
    public void setBytes(int idx, byte[] val) throws ExecException;
    public void setTuple(int idx, Tuple val) throws ExecException;
    public void setDataBag(int idx, DataBag val) throws ExecException;
    public void setMap(int idx, Map<String,Object> val) throws ExecException;
    public void setDateTime(int idx, DateTime val) throws ExecException;

    public int getInt(int idx) throws ExecException, FieldIsNullException;
    public float getFloat(int idx) throws ExecException, FieldIsNullException;
    public double getDouble(int idx) throws ExecException, FieldIsNullException;
    public long getLong(int idx) throws ExecException, FieldIsNullException;
    public String getString(int idx) throws ExecException, FieldIsNullException;
    public boolean getBoolean(int idx) throws ExecException, FieldIsNullException;
    public BigInteger getBigInteger(int idx) throws ExecException;
    public BigDecimal getBigDecimal(int idx) throws ExecException;
    public byte[] getBytes(int idx) throws ExecException, FieldIsNullException;
    public Tuple getTuple(int idx) throws ExecException;
    public DataBag getDataBag(int idx) throws ExecException, FieldIsNullException;
    public Map<String,Object> getMap(int idx) throws ExecException, FieldIsNullException;
    public DateTime getDateTime(int idx) throws ExecException, FieldIsNullException;

    public Schema getSchema();
}
