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
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.PlanVisitor;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LOUnion extends LogicalOperator {

    private static final long serialVersionUID = 2L;
    private ArrayList<LogicalOperator> mInputs;
	private static Log log = LogFactory.getLog(LOUnion.class);

    /**
     * @param plan
     *            Logical plan this operator is a part of.
     * @param k
     *            Operator key to assign to this node.
     * @param inputs
     *            List of operators that are input to the union
     */
    public LOUnion(LogicalPlan plan, OperatorKey k,
            ArrayList<LogicalOperator> inputs) {
        super(plan, k);
        mInputs = inputs;
    }

    public List<LogicalOperator> getInputs() {
        return mInputs;
    }
    
    public void addInput(LogicalOperator input) {
        mInputs.add(input);
    }

    @Override
    public Schema getSchema() {
        if (null == mSchema) {
            // TODO FIX
            // The schema merge operation needs to be implemented in
            // order to compute the schema of the union
            //If schemas are the same then return one of the schemas else 
            //return null
        }
        return mSchema;
    }

    @Override
    public String name() {
        return "Union " + mKey.scope + "-" + mKey.id;
    }

    @Override
    public boolean supportsMultipleInputs() {
        return true;
    }

    @Override
    public boolean supportsMultipleOutputs() {
        return false;
    }

    @Override
    public void visit(LOVisitor v) throws VisitorException {
        v.visit(this);
    }

}
