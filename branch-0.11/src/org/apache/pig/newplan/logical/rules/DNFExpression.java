/**
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
package org.apache.pig.newplan.logical.rules;
import java.util.List;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.logical.expression.*;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.data.DataType;

/**
 * 
 * A boolean expression with multiple children vs. the usual binary
 * boolean expression.
 *
 */
class DNFExpression extends LogicalExpression {
    enum DNFExpressionType {
        AND, OR
    }
    final DNFExpressionType type;

    DNFExpression(String name, OperatorPlan plan, DNFExpressionType type) {
        super(name, plan);
        this.type = type;
    }

    @Override
    public void accept(PlanVisitor v) throws FrontendException {
        throw new FrontendException(
                        "DNF expression does not accept any visitor");
    }

    @Override
    public boolean isEqual(Operator other) {
        if (other != null && other instanceof DNFExpression) {
            DNFExpression otherexp = (DNFExpression) other;
            if (type != otherexp.type) return false;

            try {
                // this equality check is too strong: it does not allow for permutations;
                // should be improved in the future
                List<Operator> thisChildren = plan.getSuccessors(this), thatChildren = plan.getSuccessors(otherexp);
                if (thisChildren == null && thatChildren == null) return true;
                else if (thisChildren == null || thatChildren == null) return false;
                else {
                    if (thisChildren.size() != thatChildren.size())
                        return false;
                    for (int i = 0; i < thisChildren.size(); i++) {
                        if (!thisChildren.get(i).isEqual(
                                        thatChildren.get(i)))
                            return false;
                    }
                }
            }
            catch (FrontendException e) {
                throw new RuntimeException(e);
            }
            return true;
        }
        else {
            return false;
        }
    }

    @Override
    public LogicalSchema.LogicalFieldSchema getFieldSchema()
                    throws FrontendException {
        if (fieldSchema != null) return fieldSchema;
        fieldSchema = new LogicalSchema.LogicalFieldSchema(null, null,
                        DataType.BOOLEAN);
        fieldSchema.stampFieldSchema();
        return fieldSchema;
    }
    
    @Override
    public LogicalExpression deepCopy(LogicalExpressionPlan lgExpPlan) throws FrontendException {
        throw new FrontendException("Deepcopy not expected");
    }
}
