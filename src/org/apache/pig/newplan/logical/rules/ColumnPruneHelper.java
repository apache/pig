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

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.OperatorSubPlan;
import org.apache.pig.newplan.ReverseDependencyOrderWalker;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LOCross;
import org.apache.pig.newplan.logical.relational.LODistinct;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LOInnerLoad;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LOLimit;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LORank;
import org.apache.pig.newplan.logical.relational.LOSort;
import org.apache.pig.newplan.logical.relational.LOSplit;
import org.apache.pig.newplan.logical.relational.LOSplitOutput;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LOStream;
import org.apache.pig.newplan.logical.relational.LOUnion;
import org.apache.pig.newplan.logical.relational.LogicalRelationalNodesVisitor;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;
import org.apache.pig.newplan.logical.relational.SchemaNotDefinedException;

/**
 * Helper class used by ColumnMapKeyPrune to figure out what columns can be pruned.
 * It doesn't make any changes to the operator plan
 *
 */
public class ColumnPruneHelper {
    protected static final String INPUTUIDS = "ColumnPrune:InputUids";
    public static final String OUTPUTUIDS = "ColumnPrune:OutputUids";
    protected static final String REQUIREDCOLS = "ColumnPrune:RequiredColumns";

    private OperatorPlan currentPlan;
    private OperatorSubPlan subPlan;

    public ColumnPruneHelper(OperatorPlan currentPlan) {
        this.currentPlan = currentPlan;
    }

    private OperatorSubPlan getSubPlan() throws FrontendException {
        OperatorSubPlan p = null;
        if (currentPlan instanceof OperatorSubPlan) {
            p = new OperatorSubPlan(((OperatorSubPlan)currentPlan).getBasePlan());
        } else {
            p = new OperatorSubPlan(currentPlan);
        }
        Iterator<Operator> iter = currentPlan.getOperators();

        while(iter.hasNext()) {
            Operator op = iter.next();
            if (op instanceof LOForEach) {
                addOperator(op, p);
            }
        }

        return p;
    }

    private void addOperator(Operator op, OperatorSubPlan subplan) throws FrontendException {
        if (op == null) {
            return;
        }

        subplan.add(op);

        List<Operator> ll = currentPlan.getPredecessors(op);
        if (ll == null) {
            return;
        }

        for(Operator pred: ll) {
            addOperator(pred, subplan);
        }
    }


    @SuppressWarnings("unchecked")
    public boolean check() throws FrontendException {
        List<Operator> sources = currentPlan.getSources();
        // if this rule has run before, just return false
        if (sources.size() > 1 && sources.get(0).getAnnotation(INPUTUIDS) != null) {
            clearAnnotation();
            return false;
        }

        // create sub-plan that ends with foreach
        subPlan = getSubPlan();
        if (subPlan.size() == 0) {
            clearAnnotation();
            return false;
        }

        ColumnDependencyVisitor v = new ColumnDependencyVisitor(currentPlan);
        try {
            v.visit();
        }catch(SchemaNotDefinedException e) {
            // if any operator has an unknown schema, just return false
            clearAnnotation();
            return false;
        }

        List<Operator> ll = subPlan.getSources();
        boolean found = false;
        for(Operator op: ll) {
            if (op instanceof LOLoad) {
                Set<Long> uids = (Set<Long>)op.getAnnotation(INPUTUIDS);
                LogicalSchema s = ((LOLoad) op).getSchema();
                Set<Integer> required = getColumns(s, uids);

                if (required.size() < s.size()) {
                    op.annotate(REQUIREDCOLS, required);
                    found = true;
                }
            }
        }

        if (!found)
            clearAnnotation();

        return found;
    }

    private void clearAnnotation() {
        Iterator<Operator> iter = currentPlan.getOperators();
        while (iter.hasNext()) {
            Operator op = iter.next();
            op.removeAnnotation(INPUTUIDS);
            op.removeAnnotation(OUTPUTUIDS);
            op.removeAnnotation(REQUIREDCOLS);
        }
    }

    // get a set of column indexes from a set of uids
    protected Set<Integer> getColumns(LogicalSchema schema, Set<Long> uids) throws FrontendException {
        if (schema == null) {
            throw new SchemaNotDefinedException("Schema is not defined.");
        }

        Set<Integer> cols = new HashSet<Integer>();
        Iterator<Long> iter = uids.iterator();
        while(iter.hasNext()) {
            long uid = iter.next();
            int index = schema.findField(uid);
            if (index == -1) {
                throw new FrontendException("UID " + uid + " is not found in the schema " + schema, 2241);
            }

            cols.add(index);
        }

        return cols;
    }

    public OperatorPlan reportChanges() {
        return subPlan;
    }

    // Visitor to calculate the input and output uids for each operator
    // It doesn't change the plan, only put calculated info as annotations
    // The input and output uids are not necessarily the top level uids of
    // a schema. They may be the uids of lower level fields of complex fields
    // that have their own schema.
    static private class ColumnDependencyVisitor extends LogicalRelationalNodesVisitor {

        public ColumnDependencyVisitor(OperatorPlan plan) throws FrontendException {
            super(plan, new ReverseDependencyOrderWalker(plan));
        }

        @Override
        public void visit(LOLoad load) throws FrontendException {
            Set<Long> output = setOutputUids(load);

            // for load, input uids are same as output uids
            load.annotate(INPUTUIDS, output);
        }

        @Override
        public void visit(LOFilter filter) throws FrontendException {
            Set<Long> output = setOutputUids(filter);

            // the input uids contains all the output uids and
            // projections in filter conditions
            Set<Long> input = new HashSet<Long>(output);

            LogicalExpressionPlan exp = filter.getFilterPlan();
            collectUids(filter, exp, input);

            filter.annotate(INPUTUIDS, input);
        }

        @Override
        public void visit(LOStore store) throws FrontendException {
            Set<Long> output = setOutputUids(store);

            if (output.isEmpty()) {
                // to deal with load-store-load-store case
                LogicalSchema s = store.getSchema();
                if (s == null) {
                    throw new SchemaNotDefinedException("Schema for " + store.getName() + " is not defined.");
                }

                for(int i=0; i<s.size(); i++) {
                    output.add(s.getField(i).uid);
                }
            }

            // for store, input uids are same as output uids
            store.annotate(INPUTUIDS, output);
        }

        @Override
        public void visit(LOJoin join) throws FrontendException {
            Set<Long> output = setOutputUids(join);

            // the input uids contains all the output uids and
            // projections in join expressions
            Set<Long> input = new HashSet<Long>(output);

            Collection<LogicalExpressionPlan> exps = join.getExpressionPlanValues();
            Iterator<LogicalExpressionPlan> iter = exps.iterator();
            while(iter.hasNext()) {
                LogicalExpressionPlan exp = iter.next();
                collectUids(join, exp, input);
            }

            join.annotate(INPUTUIDS, input);
        }

        @Override
        public void visit(LOCogroup cg) throws FrontendException {
            Set<Long> output = setOutputUids(cg);

            // the input uids contains all the output uids and
            // projections in join expressions
            Set<Long> input = new HashSet<Long>();

            // Add all the uids required for doing cogroup. As in all the
            // keys on which the cogroup is done.
            for( LogicalExpressionPlan plan : cg.getExpressionPlans().values() ) {
                collectUids(cg, plan, input);
            }

            // Now check for the case where the output uid is a generated one
            // If that is the case we need to add the uids which generated it in
            // the input
            long firstUid=-1;
            Map<Integer,Long> generatedInputUids = cg.getGeneratedInputUids();
            for( Map.Entry<Integer, Long> entry : generatedInputUids.entrySet() ) {
                Long uid = entry.getValue();
                LogicalRelationalOperator pred =
                    (LogicalRelationalOperator) cg.getPlan().getPredecessors(cg).get(entry.getKey());
                if( output.contains(uid) ) {
                    // Hence we need to all the full schema of the bag
                    input.addAll( getAllUids( pred.getSchema() ) );
                }
                if (pred.getSchema()!=null)
                    firstUid = pred.getSchema().getField(0).uid;
            }

            if (input.isEmpty() && firstUid!=-1) {
                input.add(firstUid);
            }

            cg.annotate(INPUTUIDS, input);
        }

        @Override
        public void visit(LOLimit limit) throws FrontendException {
            Set<Long> output = setOutputUids(limit);

            // the input uids contains all the output uids and
            // projections in limit expression
            Set<Long> input = new HashSet<Long>(output);

            LogicalExpressionPlan exp = limit.getLimitPlan();
            if (exp != null)
                collectUids(limit, exp, input);

            limit.annotate(INPUTUIDS, input);
        }

        @Override
        public void visit(LOStream stream) throws FrontendException {
            // output is not used, setOutputUids is used to check if it has output schema
            Set<Long> output = setOutputUids(stream);

            // Every field is required
            LogicalRelationalOperator pred = (LogicalRelationalOperator)plan.getPredecessors(stream).get(0);

            Set<Long> input = getAllUids(pred.getSchema());

            stream.annotate(INPUTUIDS, input);
        }

        @Override
        public void visit(LODistinct distinct) throws FrontendException {
            setOutputUids(distinct);
            
            Set<Long> input = new HashSet<Long>();

            // Every field is required
            LogicalSchema s = distinct.getSchema();
            if (s == null) {
                throw new SchemaNotDefinedException("Schema for " + distinct.getName() + " is not defined.");
            }

            for(int i=0; i<s.size(); i++) {
                input.add(s.getField(i).uid);
            }
            distinct.annotate(INPUTUIDS, input);
        }

        @Override
        public void visit(LOCross cross) throws FrontendException {
            Set<Long> output = setOutputUids(cross);
            // Since we do not change the topology of the plan, we keep
            // at least one input for each predecessor.
            List<Operator> preds = plan.getPredecessors(cross);
            for (Operator pred : preds) {
                LogicalSchema schema = ((LogicalRelationalOperator)pred).getSchema();
                Set<Long> uids = getAllUids(schema);
                boolean allPruned = true;
                for (Long uid : uids) {
                    if (output.contains(uid))
                        allPruned = false;
                }
                if (allPruned)
                    output.add(schema.getField(0).uid);
            }
            cross.annotate(INPUTUIDS, output);
        }

        @Override
        public void visit(LOUnion union) throws FrontendException {
            Set<Long> output = setOutputUids(union);
            Set<Long> input = new HashSet<Long>();
            for (long uid : output) {
                input.addAll(union.getInputUids(uid));
            }
            union.annotate(INPUTUIDS, input);
        }

        @Override
        public void visit(LOSplit split) throws FrontendException {
            Set<Long> output = setOutputUids(split);
            split.annotate(INPUTUIDS, output);
        }

        @Override
        public void visit(LOSplitOutput splitOutput) throws FrontendException {
            Set<Long> output = setOutputUids(splitOutput);

            // the input uids contains all the output uids and
            // projections in splitOutput conditions
            Set<Long> input = new HashSet<Long>();

            for (long uid : output) {
                input.add(splitOutput.getInputUids(uid));
            }

            LogicalExpressionPlan exp = splitOutput.getFilterPlan();
            collectUids(splitOutput, exp, input);

            splitOutput.annotate(INPUTUIDS, input);
        }

        @Override
        public void visit(LOSort sort) throws FrontendException {
            Set<Long> output = setOutputUids(sort);

            Set<Long> input = new HashSet<Long>(output);

            for (LogicalExpressionPlan exp : sort.getSortColPlans()) {
                collectUids(sort, exp, input);
            }

            sort.annotate(INPUTUIDS, input);
        }

        @Override
        public void visit(LORank rank) throws FrontendException {
            Set<Long> output = setOutputUids(rank);

            Set<Long> input = new HashSet<Long>(output);

            for (LogicalExpressionPlan exp : rank.getRankColPlans()) {
                collectUids(rank, exp, input);
            }

            rank.annotate(INPUTUIDS, input);
        }

        /*
         * This function returns all uids present in the given schema
         */
        private Set<Long> getAllUids( LogicalSchema schema ) {
            Set<Long> uids = new HashSet<Long>();

            if( schema == null ) {
                return uids;
            }

            for( LogicalFieldSchema field : schema.getFields() ) {
                if( ( field.type == DataType.TUPLE || field.type == DataType.BAG )
                        && field.schema != null ) {
                   uids.addAll( getAllUids( field.schema ) );
                }
                uids.add( field.uid );
            }
            return uids;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void visit(LOForEach foreach) throws FrontendException {
            Set<Long> output = setOutputUids(foreach);

            LOGenerate gen = OptimizerUtils.findGenerate(foreach);
            gen.annotate(OUTPUTUIDS, output);

            visit(gen);

            Set<Long> input = (Set<Long>)gen.getAnnotation(INPUTUIDS);

            // Make sure at least one column will retain
            if (input.isEmpty()) {
                LogicalRelationalOperator pred = (LogicalRelationalOperator)plan.getPredecessors(foreach).get(0);
                if (pred.getSchema()!=null)
                    input.add(pred.getSchema().getField(0).uid);
            }
            foreach.annotate(INPUTUIDS, input);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void visit(LOGenerate gen) throws FrontendException {
             Set<Long> output = (Set<Long>)gen.getAnnotation(OUTPUTUIDS);

             Set<Long> input = new HashSet<Long>();

             List<LogicalExpressionPlan> ll = gen.getOutputPlans();

             Iterator<Long> iter = output.iterator();
             while(iter.hasNext()) {
                 long uid = iter.next();
                 for(int i=0; i<ll.size(); i++) {
                     LogicalExpressionPlan exp = ll.get(i);
                     boolean found = false;
                     LogicalSchema planSchema = gen.getOutputPlanSchemas().get(i);
                     for (LogicalFieldSchema fs : planSchema.getFields()) {
                         if (fs.uid == uid) {
                             found = true;
                             break;
                         }
                     }

                     if (found) {
                         List<Operator> srcs = exp.getSinks();
                         for (Operator src : srcs) {
                             if (src instanceof ProjectExpression) {
                                 List<LOInnerLoad> innerLoads = LOForEach.findReacheableInnerLoadFromBoundaryProject((ProjectExpression)src).first;
                                 for (LOInnerLoad innerLoad : innerLoads) {
                                     ProjectExpression prj = innerLoad.getProjection();
                                     if (prj.isProjectStar()) {
                                         if (prj.findReferent().getSchema()!=null) {
                                             for (LogicalSchema.LogicalFieldSchema fs : prj.findReferent().getSchema().getFields()) {
                                                 input.add(fs.uid);
                                             }
                                         }
                                     }
                                     else {
                                         if (prj.findReferent().getSchema()!=null) {
                                             LogicalSchema.LogicalFieldSchema fs = prj.findReferent().getSchema().getField(prj.getColNum()); 
                                             input.add(fs.uid);
                                         }
                                     }
                                 }
                             }
                         }
                     }
                 }
             }

             // for the flatten bag, we need to make sure at least one field is in the input
             for(int i=0; i<ll.size(); i++) {
                 if (!gen.getFlattenFlags()[i]) {
                     continue;
                 }
                 LogicalExpressionPlan exp = ll.get(i);
                 LogicalExpression sink = (LogicalExpression)exp.getSources().get(0);
                 if (sink.getFieldSchema().type!=DataType.TUPLE && sink.getFieldSchema().type!=DataType.BAG)
                     continue;
                 List<Operator> srcs = exp.getSinks();
                 for (Operator src : srcs) {
                     if (!(src instanceof ProjectExpression))
                         continue;
                     List<LOInnerLoad> innerLoads = LOForEach.findReacheableInnerLoadFromBoundaryProject((ProjectExpression)src).first;
                     for (LOInnerLoad innerLoad : innerLoads) {
                         ProjectExpression prj = innerLoad.getProjection();
                         if (prj.isProjectStar()) {
                             if (prj.findReferent().getSchema()!=null) {
                                 for (LogicalSchema.LogicalFieldSchema fs : prj.findReferent().getSchema().getFields()) {
                                     input.add(fs.uid);
                                 }
                             }
                         }
                         else {
                             if (prj.findReferent().getSchema()!=null) {
                                 LogicalSchema.LogicalFieldSchema fs = prj.findReferent().getSchema().getField(prj.getColNum());
                                 input.add(fs.uid);
                             }
                         }
                     }
                 }
             }
             gen.annotate(INPUTUIDS, input);
        }

        @Override
        public void visit(LOInnerLoad load) throws FrontendException {
            Set<Long> output = setOutputUids(load);
            load.annotate(INPUTUIDS, output);
        }

        private void collectUids(LogicalRelationalOperator currentOp, LogicalExpressionPlan exp, Set<Long> uids) throws FrontendException {
            List<Operator> ll = exp.getSinks();
            for(Operator op: ll) {
                if (op instanceof ProjectExpression) {
                    if (!((ProjectExpression)op).isRangeOrStarProject()) {
                        long uid = ((ProjectExpression)op).getFieldSchema().uid;
                        uids.add(uid);
                    } else {
                        LogicalRelationalOperator ref = ((ProjectExpression)op).findReferent();
                        LogicalSchema s = ref.getSchema();
                        if (s == null) {
                            throw new SchemaNotDefinedException("Schema not defined for " + ref.getAlias());
                        }
                        for(LogicalFieldSchema f: s.getFields()) {
                            uids.add(f.uid);
                        }
                    }
                }
            }
        }

        @SuppressWarnings("unchecked")
        // Get output uid from output schema. If output schema does not exist,
        // throw exception
        private Set<Long> setOutputUids(LogicalRelationalOperator op) throws FrontendException {

            List<Operator> ll = plan.getSuccessors(op);
            Set<Long> uids = new HashSet<Long>();

            LogicalSchema s = op.getSchema();
            if (s == null) {
                throw new SchemaNotDefinedException("Schema for " + op.getName() + " is not defined.");
            }

            if (ll != null) {
                // if this is not sink, the output uids are union of input uids of its successors
                for(Operator succ: ll) {
                    Set<Long> inputUids = (Set<Long>)succ.getAnnotation(INPUTUIDS);
                    if (inputUids != null) {
                        Iterator<Long> iter = inputUids.iterator();
                        while(iter.hasNext()) {
                            long uid = iter.next();

                            if (s.findField(uid) != -1) {
                                uids.add(uid);
                            }
                        }
                    }
                }
            } else {
                // if  it's leaf, set to its schema
                for(int i=0; i<s.size(); i++) {
                    uids.add(s.getField(i).uid);
                }
            }

            op.annotate(OUTPUTUIDS, uids);
            return uids;
        }
    }
}
