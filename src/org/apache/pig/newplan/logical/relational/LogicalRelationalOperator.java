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

package org.apache.pig.newplan.logical.relational;

import java.util.HashSet;
import java.util.Map;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;

/**
 * Logical representation of relational operators.  Relational operators have
 * a schema.
 */
abstract public class LogicalRelationalOperator extends Operator {
    
    protected LogicalSchema schema;
    protected int requestedParallelism;
    protected String alias;
    protected int lineNum;
    
    /**
     * Name of the customPartitioner if one is used, this is set to null otherwise.
     */
    protected String mCustomPartitioner = null;
    
    /**
     * A HashSet to indicate whether an option (such a Join Type) was pinned
     * by the user or can be chosen at runtime by the optimizer.
     */
    protected HashSet<Integer> mPinnedOptions = new HashSet<Integer>();

    /**
     * 
     * @param name of this operator
     * @param plan this operator is in
     */
    public LogicalRelationalOperator(String name, OperatorPlan plan) {
        this(name, plan, -1);
    }
    
    /**
     * 
     * @param name of this operator
     * @param plan this operator is in
     * @param rp requested parallelism
     */
    public LogicalRelationalOperator(String name,
                                     OperatorPlan plan,
                                     int rp) {
        super(name, plan);
        requestedParallelism = rp;
    }

    /**
     * Get the schema for the output of this relational operator.  This does
     * not merely return the schema variable.  If schema is not yet set, this
     * will attempt to construct it.  Therefore it is abstract since each
     * operator will need to construct its schema differently.
     * @return the schema
     * @throws FrontendException
     */
    abstract public LogicalSchema getSchema() throws FrontendException;
    
    public void setSchema(LogicalSchema schema) {
        this.schema = schema;
    }
    
    /**
     * Reset the schema to null so that the next time getSchema is called
     * the schema will be regenerated from scratch.
     */
    public void resetSchema() {
        schema = null;
    }
    
    /**
     * Erase all cached uid, regenerate uid when we regenerating schema.
     * This process currently only used in ImplicitSplitInsert, which will
     * insert split and invalidate some uids in plan
     */
    public void resetUid() {
    }

    /**
     * Get the requestedParallelism for this operator.
     * @return requestedParallelsim
     */
    public int getRequestedParallelism() {
        return requestedParallelism;
    } 

    /**
     * Get the alias of this operator.  That is, if the Pig Latin for this operator
     * was 'X = sort W by $0' then the alias will be X.  For store and split it will
     * be the alias being stored or split.  Note that because of this this alias
     * is not guaranteed to be unique to a single operator.
     * @return alias
     */

    public String getAlias() {
        return alias;
    }
    
    public void setAlias(String alias) {
        this.alias = alias;
    }
    
    public void setRequestedParallelism(int parallel) {
        this.requestedParallelism = parallel;
    }

    /**
     * Get the line number in the submitted Pig Latin script where this operator
     * occurred.
     * @return line number
     */
    public int getLineNumber() {
        return lineNum;
    }
    
    /**
     * Only to be used by unit tests.  This is a back door cheat to set the schema
     * without having to calculate it.  This should never be called by production
     * code, only by tests.
     * @param schema to set
     */
    public void neverUseForRealSetSchema(LogicalSchema schema) {
        this.schema = schema;
    }
    
    /**
     * Do some basic equality checks on two relational operators.  Equality
     * is defined here as having equal schemas and  predecessors that are equal.
     * This is intended to be used by operators' equals methods.
     * @param other LogicalRelationalOperator to compare predecessors against
     * @return true if the isEquals() methods of this node's predecessor(s) returns
     * true when invoked with other's predecessor(s).
     * @throws FrontendException
     */
    protected boolean checkEquality(LogicalRelationalOperator other) throws FrontendException {
        if (other == null) return false;
        LogicalSchema s = getSchema();
        LogicalSchema os = other.getSchema();
        if (s == null && os == null) {
            // intentionally blank
        } else if (s == null || os == null) {
            // one of them is null and one isn't
            return false;
        } else {
            if (!s.isEqual(os)) return false;
        }
        return true;
    }
    
    public String toString() {
        StringBuilder msg = new StringBuilder();

        if (alias!=null) {
            msg.append(alias + ": ");
        }
        msg.append("(Name: " + getName() + " Schema: ");
        if (schema!=null)
            msg.append(schema);
        else
            msg.append("null");
        msg.append(")");
        if (annotations!=null) {
            for (Map.Entry<String, Object> entry : annotations.entrySet()) {
                msg.append(entry);
            }
        }
        return msg.toString();
    }
    
    public String getCustomPartitioner() {
        return mCustomPartitioner;
    }
    
    public void setCustomPartitioner(String customPartitioner) {
        mCustomPartitioner = customPartitioner;
    }
    
    public void pinOption(Integer opt) {
        mPinnedOptions.add(opt);
    }
    
    public boolean isPinnedOption(Integer opt) {
        return mPinnedOptions.contains(opt);
    }
}
