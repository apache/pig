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
import java.util.HashSet;
import java.util.List;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import org.apache.pig.PigException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanVisitor;
import org.apache.pig.impl.plan.ProjectionMap;
import org.apache.pig.impl.plan.RequiredFields;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.data.DataType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LOUnion extends RelationalOperator {

    private static final long serialVersionUID = 2L;
    private static Log log = LogFactory.getLog(LOUnion.class);
    
    List<Pair<Integer, Integer>> stagingPrunedColumns = new ArrayList<Pair<Integer, Integer>>(); 

    /**
     * @param plan
     *            Logical plan this operator is a part of.
     * @param k
     *            Operator key to assign to this node.
     */
    public LOUnion(LogicalPlan plan, OperatorKey k) {
        super(plan, k);
    }

    public List<LogicalOperator> getInputs() {
        return mPlan.getPredecessors(this);
    }
    
    @Override
    public Schema getSchema() throws FrontendException {
        if (!mIsSchemaComputed) {
            Collection<LogicalOperator> s = mPlan.getPredecessors(this);
            log.debug("Number of predecessors in the graph: " + s.size());
            try {
                Iterator<LogicalOperator> iter = s.iterator();
                LogicalOperator op = iter.next();
                if (null == op) {
                    int errCode = 1006;
                    String msg = "Could not find operator in plan";
                    throw new FrontendException(msg, errCode, PigException.INPUT, false, null);
                }
                mSchema = op.getSchema();
                while(iter.hasNext()) {
                    op = iter.next();
                    if(null != mSchema) {
                        mSchema = mSchema.merge(op.getSchema(), false);
                    } else {
                        mSchema = null;
                        break;
                    }
                }
                if(null != mSchema) {
                    for(Schema.FieldSchema fs: mSchema.getFields()) {
                        iter = s.iterator();
                        while(iter.hasNext()) {
                            op = iter.next();
                            Schema opSchema = op.getSchema();
                            if(null != opSchema) {
                                for(Schema.FieldSchema opFs: opSchema.getFields()) {
                                    fs.setParent(opFs.canonicalName, op);
                                }
                            } else {
                                fs.setParent(null, op);
                            }
                        }
                    }
                }
                mIsSchemaComputed = true;
            } catch (FrontendException fe) {
                mSchema = null;
                mIsSchemaComputed = false;
                throw fe;
            }
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
    public void visit(LOVisitor v) throws VisitorException {
        v.visit(this);
    }

    public byte getType() {
        return DataType.BAG;
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.LogicalOperator#clone()
     * Do not use the clone method directly. Operators are cloned when logical plans
     * are cloned using {@link LogicalPlanCloner}
     */
    @Override
    protected Object clone() throws CloneNotSupportedException {
        LOUnion unionClone = (LOUnion)super.clone();
        return unionClone;
    }
    
    @Override
    public ProjectionMap getProjectionMap() {
        
        if(mIsProjectionMapComputed) return mProjectionMap;
        mIsProjectionMapComputed = true;
        
        Schema outputSchema;
        
        try {
            outputSchema = getSchema();
        } catch (FrontendException fee) {
            mProjectionMap = null;
            return mProjectionMap;
        }
        
        if(outputSchema == null) {
            mProjectionMap = null;
            return mProjectionMap;
        }
        
        List<LogicalOperator> predecessors = (ArrayList<LogicalOperator>)mPlan.getPredecessors(this);
        if(predecessors == null) {
            mProjectionMap = null;
            return mProjectionMap;
        }
        
        MultiMap<Integer, ProjectionMap.Column> mapFields = new MultiMap<Integer, ProjectionMap.Column>();
        
        for(int inputNum = 0; inputNum < predecessors.size(); ++inputNum) {
            LogicalOperator predecessor = predecessors.get(inputNum);
            Schema inputSchema = null;        
            
            try {
                inputSchema = predecessor.getSchema();
            } catch (FrontendException fee) {
                mProjectionMap = null;
                return mProjectionMap;
            }
            
            if(inputSchema == null) {
                mProjectionMap = null;
                return mProjectionMap;
            } else {
                for(int inputColumn = 0; inputColumn < inputSchema.size(); ++inputColumn) {
                    mapFields.put(inputColumn, new ProjectionMap.Column(new Pair<Integer, Integer>(inputNum, inputColumn)));
                }
            }
        }
        
        mProjectionMap = new ProjectionMap(mapFields, null, null);
        return mProjectionMap;
    }

    @Override
    public List<RequiredFields> getRequiredFields() {
        List<LogicalOperator> predecessors = mPlan.getPredecessors(this);
        
        if(predecessors == null) {
            return null;
        }

        List<RequiredFields> requiredFields = new ArrayList<RequiredFields>();
        
        for(int inputNum = 0; inputNum < predecessors.size(); ++inputNum) {
            requiredFields.add(new RequiredFields(true));
        }
        
        return (requiredFields.size() == 0? null: requiredFields);
    }

    @Override
    public List<RequiredFields> getRelevantInputs(int output, int column) throws FrontendException {
        if (!mIsSchemaComputed)
            getSchema();
        
        if (output!=0)
            return null;

        if (column<0)
            return null;
        
        // if we have schema information, check if output column is valid
        if (mSchema!=null)
        {
            if (column >= mSchema.size())
                return null;
        }
                
        List<LogicalOperator> predecessors = mPlan.getPredecessors(this);
        if (predecessors == null)
            return null;
        
        List<RequiredFields> result = new ArrayList<RequiredFields>();
        for (int i=0;i<predecessors.size();i++)
        {
            ArrayList<Pair<Integer, Integer>> inputList = new ArrayList<Pair<Integer, Integer>>(); 
            inputList.add(new Pair<Integer, Integer>(i, column));
            result.add(new RequiredFields(inputList));
        }
        
        
        return result;
    }

    public boolean pruneColumns(List<Pair<Integer, Integer>> columns)
        throws FrontendException {
        stagingPrunedColumns.addAll(columns);
        boolean allPruned = true;
        for (Pair<Integer, Integer>pair : columns)
        {
            for (int i=0;i<mPlan.getPredecessors(this).size();i++)
            {
                if (!stagingPrunedColumns.contains(new Pair<Integer, Integer>(i, pair.second)))
                    allPruned = false;
            }
        }
        if (allPruned)
        {
            super.pruneColumns(columns);
            return true;
        }
        return false;
    }
}
