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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.PlanVisitor;

public class LOProject extends ExpressionOperator {
    private static final long serialVersionUID = 2L;

    /**
     * The expression and the column to be projected.
     */
    private ExpressionOperator mExp;
    private List<String> mProjection;

    /**
     * 
     * @param plan
     *            Logical plan this operator is a part of.
     * @param k
     *            Operator key to assign to this node.
     * @param rp
     *            degree of requested parallelism with which to execute this
     *            node.
     * @param exp
     *            the expression which might contain the column to project
     * @param projection
     *            the list of columns to project
     */
    public LOProject(LogicalPlan plan, OperatorKey key, int rp,
            ExpressionOperator exp, List<String> projection) {
        super(plan, key, rp);
        mExp = exp;
        mProjection = projection;
    }

    /**
     * 
     * @param plan
     *            Logical plan this operator is a part of.
     * @param k
     *            Operator key to assign to this node.
     * @param rp
     *            degree of requested parallelism with which to execute this
     *            node.
     * @param exp
     *            the expression which might contain the column to project
     * @param projection
     *            the column to project
     */
    public LOProject(LogicalPlan plan, OperatorKey key, int rp,
            ExpressionOperator exp, String projection) {
        super(plan, key, rp);
        mExp = exp;
        mProjection = new ArrayList<String>(1);
        mProjection.add(projection);
    }

    public ExpressionOperator getExpression() {
        return mExp;
    }

    public List<String> getProjection() {
        return mProjection;
    }

    @Override
    public String name() {
        return "Project " + mKey.scope + "-" + mKey.id;
    }

    @Override
    public boolean supportsMultipleInputs() {
        return false;
    }

    @Override
    public Schema getSchema() {
        return mSchema;
    }

    @Override
    public void visit(LOVisitor v) throws ParseException {
        v.visit(this);
    }

}
