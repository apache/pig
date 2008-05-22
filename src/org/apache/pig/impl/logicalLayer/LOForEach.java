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
package org.apache.pig.impl.logicalLayer;

import java.util.ArrayList;
import java.util.List;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.PlanVisitor;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.data.DataType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class LOForEach extends LogicalOperator {

    private static final long serialVersionUID = 2L;

    /**
     * The foreach operator supports nested query plans. At this point its one
     * level of nesting. Foreach can have a list of operators that need to be
     * applied over the input.
     */

    private LogicalPlan mForEachPlan;
    private static Log log = LogFactory.getLog(LOForEach.class);

    /**
     * @param plan
     *            Logical plan this operator is a part of.
     * @param k
     *            Operator key to assign to this node.
     * @param operators
     *            the list of operators that are applied for each input
     */

    public LOForEach(LogicalPlan plan, OperatorKey k,
            LogicalPlan foreachPlan) {

        super(plan, k);
        mForEachPlan = foreachPlan;
    }

    public LogicalPlan getForEachPlan() {
        return mForEachPlan;
    }

    @Override
    public Schema getSchema() throws FrontendException {
        if (!mIsSchemaComputed && (null == mSchema)) {
            // Assuming that the last operator is the GENERATE
            // foreach has to terminate with a GENERATE
            LogicalOperator last = null;
            for(LogicalOperator op: mForEachPlan.getLeaves()) {
                if(op instanceof LOGenerate) {
                    last = op;
                    break;
                }
            }
            if(null == last) throw new FrontendException("Did not find generate in the foreach logicalplan");

            log.debug("Last Operator: " + last.getClass().getName());
            try {
                mSchema = last.getSchema();
                mIsSchemaComputed = true;
            } catch (FrontendException ioe) {
                mSchema = null;
                mIsSchemaComputed = false;
                throw ioe;
            }
        }
        return mSchema;
    }

    @Override
    public String name() {
        return "ForEach " + mKey.scope + "-" + mKey.id;
    }

    @Override
    public boolean supportsMultipleInputs() {
        return false;
    }

    @Override
    public boolean supportsMultipleOutputs() {
        return false;
    }

    @Override
    public void visit(LOVisitor v) throws VisitorException {
        v.visit(this);
    }

    public byte getType() {
        return DataType.BAG ;
    }
}
