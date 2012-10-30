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
package org.apache.pig.newplan.logical.rules;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadPushDown;
import org.apache.pig.LoadPushDown.RequiredField;
import org.apache.pig.LoadPushDown.RequiredFieldList;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.ReverseDependencyOrderWalker;
import org.apache.pig.newplan.logical.Util;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LOCross;
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
import org.apache.pig.newplan.logical.relational.LOUnion;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalNodesVisitor;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;

public class ColumnPruneVisitor extends LogicalRelationalNodesVisitor {
    protected static final Log log = LogFactory.getLog(ColumnPruneVisitor.class);
    private Map<LOLoad,Pair<Map<Integer,Set<String>>,Set<Integer>>> requiredItems = 
        new HashMap<LOLoad,Pair<Map<Integer,Set<String>>,Set<Integer>>>();
    private boolean columnPrune;

    public ColumnPruneVisitor(OperatorPlan plan, Map<LOLoad,Pair<Map<Integer,Set<String>>,Set<Integer>>> requiredItems,
            boolean columnPrune) throws FrontendException {
        super(plan, new ReverseDependencyOrderWalker(plan));
        this.columnPrune = columnPrune;
        this.requiredItems = requiredItems;
    }

    public void addRequiredItems(LOLoad load, Pair<Map<Integer,Set<String>>,Set<Integer>> requiredItem) {
        requiredItems.put(load, requiredItem);
    }

    @Override
    public void visit(LOLoad load) throws FrontendException {
        if(! requiredItems.containsKey( load ) ) {
            return;
        }

        Pair<Map<Integer,Set<String>>,Set<Integer>> required =
            requiredItems.get(load);

        RequiredFieldList requiredFields = new RequiredFieldList();

        LogicalSchema s = load.getSchema();
        for (int i=0;i<s.size();i++) {
            RequiredField requiredField = null;
            // As we have done processing ahead, we assume that 
            // a column is not present in both ColumnPruner and 
            // MapPruner
            if( required.first != null && required.first.containsKey(i) ) {
                requiredField = new RequiredField();
                requiredField.setIndex(i);
                requiredField.setAlias(s.getField(i).alias);
                requiredField.setType(s.getField(i).type);
                List<RequiredField> subFields = new ArrayList<RequiredField>();
                for( String key : required.first.get(i) ) {
                    RequiredField subField = new RequiredField(key,-1,null,DataType.BYTEARRAY);
                    subFields.add(subField);
                }
                requiredField.setSubFields(subFields);
                requiredFields.add(requiredField);
            }
            if( required.second != null && required.second.contains(i) ) {
                requiredField = new RequiredField();
                requiredField.setIndex(i);
                requiredField.setAlias(s.getField(i).alias);
                requiredField.setType(s.getField(i).type);
                requiredFields.add(requiredField);
            }
        }

        boolean[] columnRequired = new boolean[s.size()];
        for (RequiredField rf : requiredFields.getFields())
            columnRequired[rf.getIndex()] = true;

        List<Pair<Integer, Integer>> pruneList = new ArrayList<Pair<Integer, Integer>>();
        for (int i=0;i<columnRequired.length;i++)
        {
            if (!columnRequired[i])
                pruneList.add(new Pair<Integer, Integer>(0, i));
        }
        StringBuffer message = new StringBuffer();
        if (pruneList.size()!=0)
        {
            message.append("Columns pruned for " + load.getAlias() + ": ");
            for (int i=0;i<pruneList.size();i++)
            {
                message.append("$"+pruneList.get(i).second);
                if (i!=pruneList.size()-1)
                    message.append(", ");
            }
            log.info(message);
        }

        message = new StringBuffer();
        for(RequiredField rf: requiredFields.getFields()) {
            List<RequiredField> sub = rf.getSubFields();
            if (sub != null) {
                message.append("Map key required for " + load.getAlias() + ": $" + rf.getIndex() + "->" + sub + "\n");
            }
        }
        if (message.length()!=0)
            log.info(message);

        LoadPushDown.RequiredFieldResponse response = null;
        try {
            LoadFunc loadFunc = load.getLoadFunc();
            if (loadFunc instanceof LoadPushDown) {
                response = ((LoadPushDown)loadFunc).pushProjection(requiredFields);
            }

        } catch (FrontendException e) {
            log.warn("pushProjection on "+load+" throw an exception, skip it");
        }

        // Loader does not support column pruning, insert foreach
        if (columnPrune) {
            if (response==null || !response.getRequiredFieldResponse()) {
                LogicalPlan p = (LogicalPlan)load.getPlan();
                Operator next = p.getSuccessors(load).get(0);
                // if there is already a LOForEach after load, we don't need to
                // add another LOForEach
                if (next instanceof LOForEach) {
                    return;
                }

                LOForEach foreach = new LOForEach(load.getPlan());

                // add foreach to the base plan
                p.add(foreach);

                p.insertBetween(load, foreach, next);

                LogicalPlan innerPlan = new LogicalPlan();
                foreach.setInnerPlan(innerPlan);

                // build foreach inner plan
                List<LogicalExpressionPlan> exps = new ArrayList<LogicalExpressionPlan>();
                LOGenerate gen = new LOGenerate(innerPlan, exps, new boolean[requiredFields.getFields().size()]);
                innerPlan.add(gen);

                for (int i=0; i<requiredFields.getFields().size(); i++) {
                    LoadPushDown.RequiredField rf = requiredFields.getFields().get(i);
                    LOInnerLoad innerLoad = new LOInnerLoad(innerPlan, foreach, rf.getIndex());
                    innerPlan.add(innerLoad);
                    innerPlan.connect(innerLoad, gen);

                    LogicalExpressionPlan exp = new LogicalExpressionPlan();
                    ProjectExpression prj = new ProjectExpression(exp, i, -1, gen);
                    exp.add(prj);
                    exps.add(exp);
                }

            } else {
                // columns are pruned, reset schema for LOLoader
                List<Integer> requiredIndexes = new ArrayList<Integer>();
                List<LoadPushDown.RequiredField> fieldList = requiredFields.getFields();
                for (int i=0; i<fieldList.size(); i++) {
                    requiredIndexes.add(fieldList.get(i).getIndex());
                }

                load.setRequiredFields(requiredIndexes);

                LogicalSchema newSchema = new LogicalSchema();
                for (int i=0; i<fieldList.size(); i++) {
                    newSchema.addField(s.getField(fieldList.get(i).getIndex()));
                }

                load.setSchema(newSchema);
            }
        }
    }

    @Override
    public void visit(LOFilter filter) throws FrontendException {
    }

    @Override
    public void visit(LOLimit limit) throws FrontendException {
    }

    @Override
    public void visit(LOSplitOutput splitOutput) throws FrontendException {
    }

    @SuppressWarnings("unchecked")
    @Override
    public void visit(LOSplit split) throws FrontendException {
        List<Operator> branchOutputs = split.getPlan().getSuccessors(split);
        for (int i=0;i<branchOutputs.size();i++) {
            Operator branchOutput = branchOutputs.get(i);
            Set<Long> branchOutputUids = (Set<Long>)branchOutput.getAnnotation(ColumnPruneHelper.INPUTUIDS);

            if (branchOutputUids!=null) {
                Set<Integer> columnsToDrop = new HashSet<Integer>();

                for (int j=0;j<split.getSchema().size();j++) {
                    if (!branchOutputUids.contains(split.getSchema().getField(j).uid))
                        columnsToDrop.add(j);
                }

                if (!columnsToDrop.isEmpty()) {
                    LOForEach foreach = Util.addForEachAfter((LogicalPlan)split.getPlan(), split, i, columnsToDrop);
                    foreach.getSchema();
                }
            }
        }
    }

    @Override
    public void visit(LOSort sort) throws FrontendException {
    }

    @Override
    public void visit(LORank rank) throws FrontendException {
    }

    @Override
    public void visit(LOStore store) throws FrontendException {
    }

    @Override
    public void visit( LOCogroup cg ) throws FrontendException {
        addForEachIfNecessary(cg);
    }

    @Override
    public void visit(LOJoin join) throws FrontendException {
    }

    @Override
    public void visit(LOCross cross) throws FrontendException {
    }

    @Override
    @SuppressWarnings("unchecked")
    public void visit(LOForEach foreach) throws FrontendException {
        if (!columnPrune) {
            return;
        }

        // get column numbers from input uids
        Set<Long> inputUids = (Set<Long>)foreach.getAnnotation(ColumnPruneHelper.INPUTUIDS);

        // Get all top level projects
        LogicalPlan innerPlan = foreach.getInnerPlan();
        List<LOInnerLoad> innerLoads= new ArrayList<LOInnerLoad>();
        List<Operator> sources = innerPlan.getSources();
        for (Operator s : sources) {
            if (s instanceof LOInnerLoad)
                innerLoads.add((LOInnerLoad)s);
        }

        // If project of the innerLoad is not in INPUTUIDS, remove this innerLoad
        Set<LOInnerLoad> innerLoadsToRemove = new HashSet<LOInnerLoad>();
        for (LOInnerLoad innerLoad: innerLoads) {
            ProjectExpression project = innerLoad.getProjection();
            if (project.isProjectStar()) {
                LogicalSchema.LogicalFieldSchema tupleFS = project.getFieldSchema();
                // Check the first component of the star projection
                long uid = tupleFS.schema.getField(0).uid;
                if (!inputUids.contains(uid))
                    innerLoadsToRemove.add(innerLoad);
            }
            else {
                if (!inputUids.contains(project.getFieldSchema().uid))
                    innerLoadsToRemove.add(innerLoad);
            }
        }

        // Find the logical operator immediate precede LOGenerate which should be removed (the whole branch)
        Set<LogicalRelationalOperator> branchHeadToRemove = new HashSet<LogicalRelationalOperator>();
        for (LOInnerLoad innerLoad : innerLoadsToRemove) {
            Operator op = innerLoad;
            while (!(innerPlan.getSuccessors(op).get(0) instanceof LOGenerate)) {
                op = innerPlan.getSuccessors(op).get(0);
            }
            branchHeadToRemove.add((LogicalRelationalOperator)op);
        }

        // Find the expression plan to remove
        LOGenerate gen = (LOGenerate)innerPlan.getSinks().get(0);
        List<LogicalExpressionPlan> genPlansToRemove = new ArrayList<LogicalExpressionPlan>();

        List<LogicalExpressionPlan> genPlans = gen.getOutputPlans();
        for (int i=0;i<genPlans.size();i++) {
            LogicalExpressionPlan expPlan = genPlans.get(i);
            List<Operator> expSources = expPlan.getSinks();

            for (Operator expSrc : expSources) {
                if (expSrc instanceof ProjectExpression) {
                    LogicalRelationalOperator reference = ((ProjectExpression)expSrc).findReferent();
                    if (branchHeadToRemove.contains(reference)) {
                        genPlansToRemove.add(expPlan);
                    }
                }
            }
        }

        // Build the temporary structure based on genPlansToRemove, which include:
        // * flattenList
        // * outputPlanSchemas
        // * uidOnlySchemas
        // * inputsRemoved
        //     We first construct inputsNeeded, and inputsRemoved = (all inputs) - inputsNeeded.
        //     We cannot figure out inputsRemoved directly since the inputs may be used by other output plan.
        //     We can only get inputsRemoved after visiting all output plans.
        List<Boolean> flattenList = new ArrayList<Boolean>();
        Set<Integer> inputsNeeded = new HashSet<Integer>();
        Set<Integer> inputsRemoved = new HashSet<Integer>();
        List<LogicalSchema> outputPlanSchemas = new ArrayList<LogicalSchema>();
        List<LogicalSchema> uidOnlySchemas = new ArrayList<LogicalSchema>();
        List<LogicalSchema> userDefinedSchemas = null;

        if (gen.getUserDefinedSchema()!=null)
            userDefinedSchemas = new ArrayList<LogicalSchema>();

        for (int i=0;i<genPlans.size();i++) {
            LogicalExpressionPlan genPlan = genPlans.get(i);
            if (!genPlansToRemove.contains(genPlan)) {
                flattenList.add(gen.getFlattenFlags()[i]);
                outputPlanSchemas.add(gen.getOutputPlanSchemas().get(i));
                uidOnlySchemas.add(gen.getUidOnlySchemas().get(i));
                if (gen.getUserDefinedSchema()!=null) {
                    userDefinedSchemas.add(gen.getUserDefinedSchema().get(i));
                }
                List<Operator> sinks = genPlan.getSinks();
                for(Operator s: sinks) {
                    if (s instanceof ProjectExpression) {
                        inputsNeeded.add(((ProjectExpression)s).getInputNum());
                    }
                }
            }
        }

        List<Operator> preds = innerPlan.getPredecessors(gen);

        if (preds!=null) {  // otherwise, all gen plan are based on constant, no need to adjust
            for (int i=0;i<preds.size();i++) {
                if (!inputsNeeded.contains(i))
                    inputsRemoved.add(i);
            }
        }


        // Change LOGenerate: remove unneeded output expression plan
        // change flatten flag, outputPlanSchema, uidOnlySchemas
        boolean[] flatten = new boolean[flattenList.size()];
        for (int i=0;i<flattenList.size();i++)
            flatten[i] = flattenList.get(i);

        gen.setFlattenFlags(flatten);
        gen.setOutputPlanSchemas(outputPlanSchemas);
        gen.setUidOnlySchemas(uidOnlySchemas);
        gen.setUserDefinedSchema(userDefinedSchemas);

        for (LogicalExpressionPlan genPlanToRemove : genPlansToRemove) {
            genPlans.remove(genPlanToRemove);
        }

        // shift project input
        if (!inputsRemoved.isEmpty()) {
            for (LogicalExpressionPlan genPlan : genPlans) {
                List<Operator> sinks = genPlan.getSinks();
                for(Operator s: sinks) {
                    if (s instanceof ProjectExpression) {
                        int input = ((ProjectExpression)s).getInputNum();
                        int numToShift = 0;
                        for (int i :inputsRemoved) {
                            if (i<input)
                                numToShift++;
                        }
                        ((ProjectExpression)s).setInputNum(input-numToShift);
                    }
                }
            }
        }

        // Prune unneeded LOInnerLoad
        List<LogicalRelationalOperator> predToRemove = new ArrayList<LogicalRelationalOperator>();
        for (int i : inputsRemoved) {
            predToRemove.add((LogicalRelationalOperator)preds.get(i));
        }
        for (LogicalRelationalOperator pred : predToRemove) {
            removeSubTree(pred);
        }
    }

    @Override
    public void visit(LOUnion union) throws FrontendException {
        // AddForEach before union if necessary.
        List<Operator> preds = new ArrayList<Operator>();
        preds.addAll(plan.getPredecessors(union));

        for (Operator pred : preds) {
            addForEachIfNecessary((LogicalRelationalOperator)pred);
        }
    }

    // remove all the operators starting from an operator
    private void removeSubTree(LogicalRelationalOperator op) throws FrontendException {
        LogicalPlan p = (LogicalPlan)op.getPlan();
        List<Operator> ll = p.getPredecessors(op);
        if (ll != null) {
            for(Operator pred: ll.toArray(new Operator[ll.size()])) {
                removeSubTree((LogicalRelationalOperator)pred);
            }
        }

        if (p.getSuccessors(op) != null) {
            Operator[] succs = p.getSuccessors(op).toArray(new Operator[0]);
            for(Operator s: succs) {
                p.disconnect(op, s);
            }
        }

        p.remove(op);
    }

    // Add ForEach after op to prune unnecessary columns
    @SuppressWarnings("unchecked")
    private void addForEachIfNecessary(LogicalRelationalOperator op) throws FrontendException {
        Set<Long> outputUids = (Set<Long>)op.getAnnotation(ColumnPruneHelper.OUTPUTUIDS);
        if (outputUids!=null) {
            LogicalSchema schema = op.getSchema();
            Set<Integer> columnsToDrop = new HashSet<Integer>();

            for (int i=0;i<schema.size();i++) {
                if (!outputUids.contains(schema.getField(i).uid))
                    columnsToDrop.add(i);
            }

            if (!columnsToDrop.isEmpty()) {
                LOForEach foreach = Util.addForEachAfter((LogicalPlan)op.getPlan(), op, 0, columnsToDrop);
                foreach.getSchema();
            }
        }
    }
}
