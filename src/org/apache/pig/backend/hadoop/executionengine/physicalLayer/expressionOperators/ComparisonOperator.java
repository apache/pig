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
package org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators;

import java.util.Map;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.impl.plan.OperatorKey;

/**
 * This is an interface for all comparison operators. Supports the
 * use of operand type instead of result type as the result type is
 * always boolean.
 * 
 */
public interface ComparisonOperator {
    
    /**
     * Determine the type of the operand(s) of this comparator.
     * @return type, as a byte (using DataType types).
     */
    byte getOperandType();

    /**
     * Set the type of the operand(s) of this comparator.
     * @param operandType Type of the operand(s), as a byte (using DataType
     * types).
     */
    void setOperandType(byte operandType);

    // Stupid java doesn't allow multiple inheritence, so I have to duplicate
    // all the getNext functions here so that comparitors can have them.
    public Result getNext(Integer i) throws ExecException;

    public Result getNext(Long l) throws ExecException;

    public Result getNext(Double d) throws ExecException;

    public Result getNext(Float f) throws ExecException;

    public Result getNext(String s) throws ExecException;

    public Result getNext(DataByteArray ba) throws ExecException;

    public Result getNext(Map m) throws ExecException;

    public Result getNext(Boolean b) throws ExecException;

    public Result getNext(Tuple t) throws ExecException;

    public Result getNext(DataBag db) throws ExecException;

}
