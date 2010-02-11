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

package org.apache.pig.experimental.logical.relational;

import org.apache.pig.experimental.plan.Operator;
import org.apache.pig.experimental.plan.OperatorPlan;

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
     */
    abstract public LogicalSchema getSchema();
    
    /**
     * Reset the schema to null so that the next time getSchema is called
     * the schema will be regenerated from scratch.
     */
    public void resetSchema() {
        schema = null;
    }
 

    /**
     * Get the requestedParallelism for this operator.
     * @return requestedParallelsim
     */
    public int getRequestedParallelisam() {
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
     * @return true if the equals() methods of this node's predecessor(s) returns
     * true when invoked with other's predecessor(s).
     */
    protected boolean checkEquality(LogicalRelationalOperator other) {
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
}
