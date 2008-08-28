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
import java.util.Iterator;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.Collection;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanVisitor;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LOGenerate extends LogicalOperator {
    private static final long serialVersionUID = 2L;

    /**
     * The projection list of this generate.
     */
    //private ArrayList<ExpressionOperator> mProjections;
    private ArrayList<LogicalPlan> mGeneratePlans;
    private ArrayList<Boolean> mFlatten;
    private ArrayList<Schema> mUserDefinedSchema = null;
    private static Log log = LogFactory.getLog(LOGenerate.class);

    /**
     * 
     * @param plan
     *            Logical plan this operator is a part of.
     * @param key
     *            Operator key to assign to this node.
     * @param generatePlans
     *            Plans for each projection element
     * @param flatten
     *            Whether to flatten each projection element
     */

    public LOGenerate(LogicalPlan plan, OperatorKey key,
            ArrayList<LogicalPlan> generatePlans, ArrayList<Boolean> flatten) {
        super(plan, key);
        mGeneratePlans = generatePlans;
        mFlatten = flatten;
    }

    public LOGenerate(LogicalPlan plan, OperatorKey key,
            ArrayList<LogicalPlan> generatePlans, ArrayList<Boolean> flatten,
            ArrayList<Schema> userDefinedSchemaList) {
        super(plan, key);
        mGeneratePlans = generatePlans;
        mFlatten = flatten;
        mUserDefinedSchema = userDefinedSchemaList;
    }


    /**
     * 
     * @param plan
     *            Logical plan this operator is a part of.
     * @param key
     *            Operator key to assign to this node.
     * @param generatePlan
     *            the projection of the generate
     * @param flatten
     *            whether the result needs to be flattened
     */

    public LOGenerate(LogicalPlan plan, OperatorKey key,
            LogicalPlan generatePlan, boolean flatten) {
        super(plan, key);
        mGeneratePlans = new ArrayList<LogicalPlan>();
        mGeneratePlans.add(generatePlan);
        mFlatten = new ArrayList<Boolean>();
        mFlatten.add(flatten);
    }


    public List<LogicalPlan> getGeneratePlans() {
        return mGeneratePlans;
    }

    public List<Boolean> getFlatten() {
        return mFlatten;
    }

    public List<Schema> getUserDefinedSchema() {
        return mUserDefinedSchema;
    }

    @Override
    public String name() {
        return "Generate " + mKey.scope + "-" + mKey.id;
    }

    @Override
    public boolean supportsMultipleInputs() {
        return true;
    }

    @Override
    public Schema getSchema() throws FrontendException {
        if (!mIsSchemaComputed) {
            // Get the schema of the parent
            Collection<LogicalOperator> s = mPlan.getPredecessors(this);
            ArrayList<Schema.FieldSchema> fss = new ArrayList<Schema.FieldSchema>();
            try {
                LogicalOperator op = s.iterator().next();
                if (null == op) {
                    throw new FrontendException("Could not find operator in plan");
                }
                if(op instanceof ExpressionOperator) {
                    fss.add(((ExpressionOperator)op).getFieldSchema());
                    mSchema = new Schema(fss);
                } else {
                    mSchema = op.getSchema();
                }
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
    public void visit(LOVisitor v) throws VisitorException {
        v.visit(this);
    }

}
