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
package org.apache.pig.newplan.logical.relational;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;



public class LOJoin extends LogicalRelationalOperator {
    private static final long serialVersionUID = 2L;

    /**
     * Enum for the type of join
     */
    public static enum JOINTYPE {
        HASH,    // Hash Join
        REPLICATED, // Fragment Replicated join
        SKEWED, // Skewed Join
        MERGE,   // Sort Merge Join
        MERGESPARSE   // Sort Merge Index Join
    };


    /**
     * LOJoin contains a list of logical operators corresponding to the
     * relational operators and a list of generates for each relational
     * operator. Each generate operator in turn contains a list of expressions
     * for the columns that are projected
     */
    //private static Log log = LogFactory.getLog(LOJoin.class);
    // expression plans for each input.
    private MultiMap<Integer, LogicalExpressionPlan> mJoinPlans;
    // indicator for each input whether it is inner
    private boolean[] mInnerFlags;
    private JOINTYPE mJoinType; // Retains the type of the join

    /**
     * static constant to refer to the option of selecting a join type
     */
    public final static Integer OPTION_JOIN = 1;

    public LOJoin(LogicalPlan plan) {
        super("LOJoin", plan);
    }

    public LOJoin(LogicalPlan plan,
                MultiMap<Integer, LogicalExpressionPlan> joinPlans,
                JOINTYPE jt,
                boolean[] isInner) {
        super("LOJoin", plan);
        mJoinPlans = joinPlans;
        mJoinType = jt;
        mInnerFlags = isInner;
    }

    public void setJoinPlans(MultiMap<Integer, LogicalExpressionPlan> joinPlans) {
        this.mJoinPlans = joinPlans;
    }

    public void setInnerFlags(boolean[] isInner) {
        this.mInnerFlags = isInner;
    }

    public void setJoinType(JOINTYPE jt) {
        this.mJoinType = jt;
    }

    public boolean isInner(int inputIndex) {
        return mInnerFlags[inputIndex];
    }

    public boolean[] getInnerFlags() {
        return mInnerFlags;
    }

    public JOINTYPE getJoinType() {
        return mJoinType;
    }

    public void resetJoinType() {
        mJoinType = JOINTYPE.HASH;
    }

    public Collection<LogicalExpressionPlan> getJoinPlan(int inputIndex) {
        return mJoinPlans.get(inputIndex);
    }

    /**
     * Get all of the expressions plans that are in this join.
     * @return collection of all expression plans.
     */
    public MultiMap<Integer,LogicalExpressionPlan> getExpressionPlans() {
        return mJoinPlans;
    }

    public Collection<LogicalExpressionPlan> getExpressionPlanValues() {
        return mJoinPlans.values();
    }

    @Override
    public LogicalSchema getSchema() throws FrontendException {
        // if schema is calculated before, just return
        if (schema != null) {
            return schema;
        }

        List<Operator> inputs = null;
        inputs = plan.getPredecessors(this);
        if (inputs == null) {
            return null;
        }

        List<LogicalSchema.LogicalFieldSchema> fss = new ArrayList<LogicalSchema.LogicalFieldSchema>();

        for (Operator op : inputs) {
            LogicalSchema inputSchema = ((LogicalRelationalOperator)op).getSchema();
            // the schema of one input is unknown, so the join schema is unknown, just return
            if (inputSchema == null) {
                schema = null;
                return schema;
            }

            for (int i=0; i<inputSchema.size(); i++) {
                 LogicalSchema.LogicalFieldSchema fs = inputSchema.getField(i);
                 LogicalSchema.LogicalFieldSchema newFS = null;
                 if(fs.alias != null) {
                     newFS = new LogicalSchema.LogicalFieldSchema(((LogicalRelationalOperator)op).getAlias()+"::"+fs.alias ,fs.schema, fs.type, fs.uid);
                 } else {
                     newFS = new LogicalSchema.LogicalFieldSchema(fs.alias, fs.schema, fs.type, fs.uid);
                 }
                 fss.add(newFS);
            }
        }

        schema = new LogicalSchema();
        for(LogicalSchema.LogicalFieldSchema fieldSchema: fss) {
            schema.addField(fieldSchema);
        }

        return schema;
    }

    @Override
    public void accept(PlanVisitor v) throws FrontendException {
        if (!(v instanceof LogicalRelationalNodesVisitor)) {
            throw new FrontendException("Expected LogicalPlanVisitor", 2223);
        }
        ((LogicalRelationalNodesVisitor)v).visit(this);

    }

    @Override
    public boolean isEqual(Operator other) throws FrontendException {
        if (other != null && other instanceof LOJoin) {
            LOJoin oj = (LOJoin)other;
            if (mJoinType != oj.mJoinType) return false;
            if (mInnerFlags.length != oj.mInnerFlags.length) return false;
            for (int i = 0; i < mInnerFlags.length; i++) {
                if (mInnerFlags[i] != oj.mInnerFlags[i]) return false;
            }
            if (!checkEquality(oj)) return false;

            if (mJoinPlans.size() != oj.mJoinPlans.size())  return false;

            // Now, we need to make sure that for each input we are projecting
            // the same columns.  This is slightly complicated since MultiMap
            // doesn't return any particular order, so we have to find the
            // matching input in each case.
            for (Integer p : mJoinPlans.keySet()) {
                Iterator<Integer> iter = oj.mJoinPlans.keySet().iterator();
                int op = -1;
                while (iter.hasNext()) {
                    op = iter.next();
                    if (p.equals(op)) break;
                }
                if (op != -1) {
                    Collection<LogicalExpressionPlan> c = mJoinPlans.get(p);
                    Collection<LogicalExpressionPlan> oc = oj.mJoinPlans.get(op);
                    if (c.size() != oc.size()) return false;

                    if (!(c instanceof List) || !(oc instanceof List)) {
                        throw new FrontendException(
                            "Expected list of expression plans", 2238);
                    }
                    List<LogicalExpressionPlan> elist = (List<LogicalExpressionPlan>)c;
                    List<LogicalExpressionPlan> oelist = (List<LogicalExpressionPlan>)oc;
                    for (int i = 0; i < elist.size(); i++) {
                        if (!elist.get(i).isEqual(oelist.get(i))) return false;
                    }
                } else {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }

    @Override
    public String getName() {
        return name + "(" + mJoinType.toString() + ")";
    }

    public List<Operator> getInputs(LogicalPlan plan) {
        return plan.getPredecessors(this);
    }
}
