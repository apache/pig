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

package org.apache.pig.pen;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLimit;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.expression.AddExpression;
import org.apache.pig.newplan.logical.expression.AndExpression;
import org.apache.pig.newplan.logical.expression.BinaryExpression;
import org.apache.pig.newplan.logical.expression.CastExpression;
import org.apache.pig.newplan.logical.expression.ConstantExpression;
import org.apache.pig.newplan.logical.expression.DivideExpression;
import org.apache.pig.newplan.logical.expression.EqualExpression;
import org.apache.pig.newplan.logical.expression.GreaterThanEqualExpression;
import org.apache.pig.newplan.logical.expression.GreaterThanExpression;
import org.apache.pig.newplan.logical.expression.IsNullExpression;
import org.apache.pig.newplan.logical.expression.LessThanEqualExpression;
import org.apache.pig.newplan.logical.expression.LessThanExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ModExpression;
import org.apache.pig.newplan.logical.expression.MultiplyExpression;
import org.apache.pig.newplan.logical.expression.NotEqualExpression;
import org.apache.pig.newplan.logical.expression.NotExpression;
import org.apache.pig.newplan.logical.expression.OrExpression;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.expression.RegexExpression;
import org.apache.pig.newplan.logical.expression.SubtractExpression;
import org.apache.pig.newplan.logical.expression.UserFuncExpression;
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LOCross;
import org.apache.pig.newplan.logical.relational.LODistinct;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LOLimit;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LOSort;
import org.apache.pig.newplan.logical.relational.LOSplit;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LOUnion;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalNodesVisitor;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.pen.util.ExampleTuple;
import org.apache.pig.pen.util.PreOrderDepthFirstWalker;

//This is used to generate synthetic data
//Synthetic data generation is done by making constraint tuples for each operator as we traverse the plan
//and try to replace the constraints with values as far as possible. We only deal with simple conditions right now

public class AugmentBaseDataVisitor extends LogicalRelationalNodesVisitor {

    Map<LOLoad, DataBag> baseData = null;
    Map<LOLoad, DataBag> newBaseData = new HashMap<LOLoad, DataBag>();
    Map<Operator, DataBag> derivedData = null;
    private boolean limit = false;
    private final Map<Operator, PhysicalOperator> logToPhysMap;
    private Map<LOLimit, Long> oriLimitMap;

    Map<Operator, DataBag> outputConstraintsMap = new HashMap<Operator, DataBag>();

    Log log = LogFactory.getLog(getClass());

    // Augmentation moves from the leaves to root and hence needs a
    // depthfirstwalker
    public AugmentBaseDataVisitor(OperatorPlan plan,
            Map<Operator, PhysicalOperator> logToPhysMap,
            Map<LOLoad, DataBag> baseData,
            Map<Operator, DataBag> derivedData) throws FrontendException {
        super(plan, new PreOrderDepthFirstWalker(
                plan));
        this.baseData = baseData;
        this.derivedData = derivedData;
        this.logToPhysMap = logToPhysMap;
    }

    public void setLimit() {
        limit = true;
    }

    public Map<LOLoad, DataBag> getNewBaseData() throws ExecException {
        // consolidate base data from different LOADs on the same inputs
        MultiMap<FileSpec, DataBag> inputDataMap = new MultiMap<FileSpec, DataBag>();
        for (Map.Entry<LOLoad, DataBag> e : newBaseData.entrySet()) {
            inputDataMap.put(e.getKey().getFileSpec(), e.getValue());
        }

        int index = 0;
        for (FileSpec fs : inputDataMap.keySet()) {
            int maxSchemaSize = 0;
            Tuple tupleOfMaxSchemaSize = null;
            for (DataBag bag : inputDataMap.get(fs)) {
                if (bag.size() > 0) {
                    int size = 0;
                    Tuple t = null;
                    t = bag.iterator().next();
                    size = t.size();
                    if (size > maxSchemaSize) {
                        maxSchemaSize = size;
                        tupleOfMaxSchemaSize = t;
                    }
                }
            }
            for (DataBag bag : inputDataMap.get(fs)) {
                if (bag.size() > 0) {
                    for (Iterator<Tuple> it = bag.iterator(); it.hasNext();) {
                        Tuple t = it.next();
                        for (int i = t.size(); i < maxSchemaSize; ++i) {
                            t.append(tupleOfMaxSchemaSize.get(i));
                        }
                    }
                }
            }
            index++;
        }


        for (Map.Entry<LOLoad, DataBag> e : baseData.entrySet()) {
            DataBag bag = newBaseData.get(e.getKey());
            if (bag == null) {
                bag = BagFactory.getInstance().newDefaultBag();
                newBaseData.put(e.getKey(), bag);
            }
            bag.addAll(e.getValue());
        }
        return newBaseData;
    }

    public Map<LOLimit, Long> getOriLimitMap() {
        return oriLimitMap;
    }

    @Override
    public void visit(LOCogroup cg) throws FrontendException {
        if (limit && !((PreOrderDepthFirstWalker) currentWalker).getBranchFlag())
            return;
        // we first get the outputconstraints for the current cogroup
        DataBag outputConstraints = outputConstraintsMap.get(cg);
        outputConstraintsMap.remove(cg);
        boolean ableToHandle = true;
        // we then check if we can handle this cogroup and try to collect some
        // information about grouping
        List<List<Integer>> groupSpecs = new LinkedList<List<Integer>>();
        int numCols = -1;

        for (int index = 0; index < cg.getInputs((LogicalPlan)plan).size(); ++index) {
            Collection<LogicalExpressionPlan> groupByPlans =
                cg.getExpressionPlans().get(index);
            List<Integer> groupCols = new ArrayList<Integer>();
            for (LogicalExpressionPlan plan : groupByPlans) {
                Operator leaf = plan.getSinks().get(0);
                if (leaf instanceof ProjectExpression) {
                    groupCols.add(Integer.valueOf(((ProjectExpression) leaf).getColNum()));
                } else {
                    ableToHandle = false;
                    break;
                }
            }
            if (numCols == -1) {
                numCols = groupCols.size();
            }
            if (groupCols.size() != groupByPlans.size()
                    || groupCols.size() != numCols) {
                // we came across an unworkable cogroup plan
                break;
            } else {
                groupSpecs.add(groupCols);
            }
        }

        // we should now have some workable data at this point to synthesize
        // tuples
        try {
            if (ableToHandle) {
                // we need to go through the output constraints first
                int numInputs = cg.getInputs((LogicalPlan) plan).size();
                if (outputConstraints != null) {
                    for (Iterator<Tuple> it = outputConstraints.iterator(); it
                            .hasNext();) {
                        Tuple outputConstraint = it.next();
                        Object groupLabel = outputConstraint.get(0);

                        for (int input = 0; input < numInputs; input++) {

                            int numInputFields = ((LogicalRelationalOperator) cg.getInputs((LogicalPlan) plan).get(input))
                                    .getSchema().size();
                            List<Integer> groupCols = groupSpecs.get(input);

                            DataBag output = outputConstraintsMap.get(cg
                                    .getInputs((LogicalPlan) plan).get(input));
                            if (output == null) {
                                output = BagFactory.getInstance()
                                        .newDefaultBag();
                                outputConstraintsMap.put(cg.getInputs((LogicalPlan) plan).get(
                                        input), output);
                            }
                            for (int i = 0; i < 2; i++) {
                                Tuple inputConstraint = GetGroupByInput(
                                        groupLabel, groupCols, numInputFields);
                                if (inputConstraint != null)
                                    output.add(inputConstraint);
                            }
                        }
                    }
                }
                // then, go through all organic data groups and add input
                // constraints to make each group big enough
                DataBag outputData = derivedData.get(cg);

                for (Iterator<Tuple> it = outputData.iterator(); it.hasNext();) {
                    Tuple groupTup = it.next();
                    Object groupLabel = groupTup.get(0);

                    for (int input = 0; input < numInputs; input++) {
                        int numInputFields = ((LogicalRelationalOperator)cg.getInputs((LogicalPlan) plan).get(input))
                                .getSchema().size();
                        List<Integer> groupCols = groupSpecs.get(input);

                        DataBag output = outputConstraintsMap.get(cg
                                .getInputs((LogicalPlan) plan).get(input));
                        if (output == null) {
                            output = BagFactory.getInstance().newDefaultBag();
                            outputConstraintsMap.put(cg.getInputs((LogicalPlan) plan).get(input),
                                    output);
                        }
                        int numTupsToAdd = 2
                                - (int) ((DataBag) groupTup.get(input + 1))
                                        .size();
                        for (int i = 0; i < numTupsToAdd; i++) {
                            Tuple inputConstraint = GetGroupByInput(groupLabel,
                                    groupCols, numInputFields);
                            if (inputConstraint != null)
                                output.add(inputConstraint);
                        }
                    }
                }
            }
        } catch (Exception e) {
            log
                    .error("Error visiting Cogroup during Augmentation phase of Example Generator! "
                            + e.getMessage());
            throw new FrontendException(
                    "Error visiting Cogroup during Augmentation phase of Example Generator! "
                            + e.getMessage());
        }
    }

    @Override
    public void visit(LOJoin join) throws FrontendException {
        if (limit && !((PreOrderDepthFirstWalker) currentWalker).getBranchFlag())
            return;
        // we first get the outputconstraints for the current cogroup
        DataBag outputConstraints = outputConstraintsMap.get(join);
        outputConstraintsMap.remove(join);
        boolean ableToHandle = true;
        // we then check if we can handle this cogroup and try to collect some
        // information about grouping
        List<List<Integer>> groupSpecs = new LinkedList<List<Integer>>();
        int numCols = -1;

        for (int index = 0; index < join.getInputs((LogicalPlan)plan).size(); ++index) {
            Collection<LogicalExpressionPlan> groupByPlans =
                join.getExpressionPlans().get(index);
            List<Integer> groupCols = new ArrayList<Integer>();
            for (LogicalExpressionPlan plan : groupByPlans) {
                Operator leaf = plan.getSinks().get(0);
                if (leaf instanceof ProjectExpression) {
                    groupCols.add(Integer.valueOf(((ProjectExpression) leaf).getColNum()));
                } else {
                    ableToHandle = false;
                    break;
                }
            }
            if (numCols == -1) {
                numCols = groupCols.size();
            }
            if (groupCols.size() != groupByPlans.size()
                    || groupCols.size() != numCols) {
                // we came across an unworkable cogroup plan
                break;
            } else {
                groupSpecs.add(groupCols);
            }
        }

        // we should now have some workable data at this point to synthesize
        // tuples
        try {
            if (ableToHandle) {
                // we need to go through the output constraints first
                int numInputs = join.getInputs((LogicalPlan) plan).size();
                if (outputConstraints != null) {
                    for (Iterator<Tuple> it = outputConstraints.iterator(); it
                            .hasNext();) {
                        Tuple outputConstraint = it.next();

                        for (int input = 0; input < numInputs; input++) {

                            int numInputFields = ((LogicalRelationalOperator) join.getInputs((LogicalPlan) plan).get(input))
                                    .getSchema().size();
                            List<Integer> groupCols = groupSpecs.get(input);

                            DataBag output = outputConstraintsMap.get(join
                                    .getInputs((LogicalPlan) plan).get(input));
                            if (output == null) {
                                output = BagFactory.getInstance()
                                        .newDefaultBag();
                                outputConstraintsMap.put(join.getInputs((LogicalPlan) plan).get(
                                        input), output);
                            }

                            Tuple inputConstraint = GetJoinInput(
                                    outputConstraint, groupCols, numInputFields);
                            if (inputConstraint != null)
                                output.add(inputConstraint);
                        }
                    }
                }
                // then, go through all organic data groups and add input
                // constraints to make each group big enough
                DataBag outputData = derivedData.get(join);

                if (outputData.size() == 0) {
                    DataBag output0 = outputConstraintsMap.get(join.getInputs((LogicalPlan) plan).get(0));
                    if (output0 == null || output0.size() == 0) {
                        output0 = derivedData.get(join.getInputs((LogicalPlan) plan).get(0));
                    }
                    Tuple inputConstraint0 = output0.iterator().next();
                    for (int input = 1; input < numInputs; input++) {
                        DataBag output = outputConstraintsMap.get(join.getInputs((LogicalPlan) plan).get(input));
                        if (output == null)
                        {
                            output = BagFactory.getInstance().newDefaultBag();
                            outputConstraintsMap.put(join.getInputs((LogicalPlan) plan).get(input),
                                    output);
                        }
                        int numInputFields = ((LogicalRelationalOperator)join.getInputs((LogicalPlan) plan).get(input)).getSchema().size();
                        Tuple inputConstraint = GetJoinInput(inputConstraint0, groupSpecs.get(0), groupSpecs.get(input), numInputFields);
                        if (inputConstraint != null)
                            output.add(inputConstraint);
                    }
                }
            }
        } catch (Exception e) {
            log
                    .error("Error visiting Cogroup during Augmentation phase of Example Generator! "
                            + e.getMessage());
            throw new FrontendException(
                    "Error visiting Cogroup during Augmentation phase of Example Generator! "
                            + e.getMessage());
        }
    }

    @Override
    public void visit(LOCross cs) throws FrontendException {

    }

    @Override
    public void visit(LODistinct dt) throws FrontendException {
        if (limit && !((PreOrderDepthFirstWalker) currentWalker).getBranchFlag())
            return;

        DataBag outputConstraints = outputConstraintsMap.get(dt);
        outputConstraintsMap.remove(dt);

        DataBag inputConstraints = outputConstraintsMap.get(dt.getInput((LogicalPlan) plan));
        if (inputConstraints == null) {
            inputConstraints = BagFactory.getInstance().newDefaultBag();
            outputConstraintsMap.put(dt.getInput((LogicalPlan) plan), inputConstraints);
        }

        if (outputConstraints != null && outputConstraints.size() > 0) {
            for (Iterator<Tuple> it = outputConstraints.iterator(); it.hasNext();)
            {
                inputConstraints.add(it.next());
            }
        }

        boolean emptyInputConstraints = inputConstraints.size() == 0;
        if (emptyInputConstraints) {
            DataBag inputData = derivedData.get(dt.getInput((LogicalPlan) plan));
            for (Iterator<Tuple> it = inputData.iterator(); it.hasNext();)
            {
                inputConstraints.add(it.next());
            }
        }
        Set<Tuple> distinctSet = new HashSet<Tuple>();
        Iterator<Tuple> it;
        for (it = inputConstraints.iterator(); it.hasNext();) {
            if (!distinctSet.add(it.next()))
                break;
        }
        if (!it.hasNext())
        {
            // no duplicates found: generate one
            if (inputConstraints.size()> 0) {
                Tuple src = ((ExampleTuple)inputConstraints.iterator().next()).toTuple(),
                      tgt = TupleFactory.getInstance().newTuple(src.getAll());
                ExampleTuple inputConstraint = new ExampleTuple(tgt);
                inputConstraint.synthetic = true;
                inputConstraints.add(inputConstraint);
            } else if (emptyInputConstraints)
                inputConstraints.clear();
        }
    }

    @Override
    public void visit(LOFilter filter) throws FrontendException {
        if (limit && !((PreOrderDepthFirstWalker) currentWalker).getBranchFlag())
            return;

        DataBag outputConstraints = outputConstraintsMap.get(filter);
        outputConstraintsMap.remove(filter);

        LogicalExpressionPlan filterCond = filter.getFilterPlan();
        DataBag inputConstraints = outputConstraintsMap.get(filter.getInput((LogicalPlan) plan));
        if (inputConstraints == null) {
            inputConstraints = BagFactory.getInstance().newDefaultBag();
            outputConstraintsMap.put(filter.getInput((LogicalPlan) plan), inputConstraints);
        }

        DataBag outputData = derivedData.get(filter);
        DataBag inputData = derivedData.get(filter.getInput((LogicalPlan) plan));
        try {
            if (outputConstraints != null && outputConstraints.size() > 0) { // there
                // 's
                // one
                // or
                // more
                // output
                // constraints
                // ;
                // generate
                // corresponding
                // input
                // constraints
                for (Iterator<Tuple> it = outputConstraints.iterator(); it
                        .hasNext();) {
                    Tuple outputConstraint = it.next();
                    ExampleTuple inputConstraint = GenerateMatchingTuple(
                            outputConstraint, filterCond, false);
                    if (inputConstraint != null)
                        inputConstraints.add(inputConstraint);
                }
            } else if (outputData.size() == 0) { // no output constraints, but
                // output is empty; generate
                // one input that will pass the
                // filter
                ExampleTuple inputConstraint = GenerateMatchingTuple(filter
                        .getSchema(), filterCond, false);

                if (inputConstraint != null)
                    inputConstraints.add(inputConstraint);
            }

            // if necessary, insert a negative example (i.e. a tuple that does
            // not pass the filter)
            if (outputData.size() == inputData.size()) { // all tuples pass the
                // filter; generate one
                // input that will not
                // pass the filter

                ExampleTuple inputConstraint = GenerateMatchingTuple(filter
                        .getSchema(), filterCond, true);
                if (inputConstraint != null)
                    inputConstraints.add(inputConstraint);

            }
        } catch (Exception e) {
            log
                    .error("Error visiting Load during Augmentation phase of Example Generator! "
                            + e.getMessage(), e);
            throw new FrontendException(
                    "Error visiting Load during Augmentation phase of Example Generator! "
                            + e.getMessage(), e);
        }
    }

    @Override
    public void visit(LOForEach forEach) throws FrontendException {
        if (limit && !((PreOrderDepthFirstWalker) currentWalker).getBranchFlag())
            return;
        DataBag outputConstraints = outputConstraintsMap.get(forEach);
        outputConstraintsMap.remove(forEach);
        LogicalPlan plan = forEach.getInnerPlan();
        boolean ableToHandle = true;
        List<Integer> cols = new ArrayList<Integer>();
        boolean cast = false;

        if (outputConstraints == null || outputConstraints.size() == 0)
            // we dont have to do anything in this case
            return;


        Operator op = plan.getSinks().get(0);
        if (op instanceof CastExpression) {
                cast = true;
                op = ((CastExpression) op).getExpression();
            }

            if (!(op instanceof ProjectExpression)) {
                ableToHandle = false;
            } else {
                cols.add(Integer.valueOf(((ProjectExpression) op).getColNum()));
            }

        if (ableToHandle) {
            // we can only handle simple projections
            DataBag output = BagFactory.getInstance().newDefaultBag();
            for (Iterator<Tuple> it = outputConstraints.iterator(); it
                    .hasNext();) {
                Tuple outputConstraint = it.next();
                try {
                    Tuple inputConstraint = BackPropConstraint(
                            outputConstraint, cols, ((LogicalRelationalOperator)plan
                                    .getPredecessors(forEach).get(0))
                                    .getSchema(), cast);
                    output.add(inputConstraint);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new FrontendException(
                            "Operator error during Augmenting Phase in Example Generator "
                                    + e.getMessage());
                }
            }
            outputConstraintsMap.put(plan.getPredecessors(forEach)
                    .get(0), output);
        }

    }

    @Override
    public void visit(LOLoad load) throws FrontendException {
        DataBag inputData = baseData.get(load);
       // check if the inputData exists
        if (inputData == null || inputData.size() == 0) {
            log.error("No (valid) input data found!");
            throw new RuntimeException("No (valid) input data found!");
        }

        DataBag newInputData = newBaseData.get(load);
        if (newInputData == null) {
            newInputData = BagFactory.getInstance().newDefaultBag();
            newBaseData.put(load, newInputData);
        }

        LogicalSchema schema;
        try {
            schema = load.getSchema();
            if (schema == null)
                throw new RuntimeException(
                        "Example Generator requires a schema. Please provide a schema while loading data");
        } catch (FrontendException e) {

            log
                    .error("Error visiting Load during Augmentation phase of Example Generator! "
                            + e.getMessage());
            throw new FrontendException(
                    "Error visiting Load during Augmentation phase of Example Generator! "
                            + e.getMessage());
        }

        Tuple exampleTuple = inputData.iterator().next();

        DataBag outputConstraints = outputConstraintsMap.get(load);
        outputConstraintsMap.remove(load);

        // first of all, we are required to guarantee that there is at least one
        // output tuple
        if (outputConstraints == null || outputConstraints.size() == 0) {
            outputConstraints = BagFactory.getInstance().newDefaultBag();
            outputConstraints.add(TupleFactory.getInstance().newTuple(
                    schema.getFields().size()));
        }

        // create example tuple to steal values from when we encounter
        // "don't care" fields (i.e. null fields)
        System.out.println(exampleTuple.toString());

        // run through output constraints; for each one synthesize a tuple and
        // add it to the base data
        // (while synthesizing individual fields, try to match fields that exist
        // in the real data)
        boolean newInput = false;
        for (Iterator<Tuple> it = outputConstraints.iterator(); it.hasNext();) {
            Tuple outputConstraint = it.next();

            // sanity check:
            if (outputConstraint.size() != schema.getFields().size())
                throw new RuntimeException(
                        "Internal error: incorrect number of fields in constraint tuple.");

            Tuple inputT = TupleFactory.getInstance().newTuple(
                    outputConstraint.size());
            ExampleTuple inputTuple = new ExampleTuple(inputT);

            try {
                for (int i = 0; i < inputTuple.size(); i++) {
                    Object d = outputConstraint.get(i);
                    if (d == null && i < exampleTuple.size())
                        d = exampleTuple.get(i);
                    inputTuple.set(i, d);
                }
                if (outputConstraint instanceof ExampleTuple)
                    inputTuple.synthetic = ((ExampleTuple) outputConstraint).synthetic;
                else
                    // raw tuple should have been synthesized
                    inputTuple.synthetic = true;
            } catch (ExecException e) {
                log
                        .error("Error visiting Load during Augmentation phase of Example Generator! "
                                + e.getMessage());
                throw new FrontendException(
                        "Error visiting Load during Augmentation phase of Example Generator! "
                                + e.getMessage());

            }
            try {
                if (inputTuple.synthetic || !inInput(inputTuple, inputData, schema))
                {
                    inputTuple.synthetic = true;

                    newInputData.add(inputTuple);

                    if (!newInput)
                        newInput = true;
                }
            } catch (ExecException e) {
                throw new FrontendException(
                  "Error visiting Load during Augmentation phase of Example Generator! "
                          + e.getMessage());
            }
        }
    }

    private boolean inInput(Tuple newTuple, DataBag input, LogicalSchema schema) throws ExecException {
        boolean result;
        for (Iterator<Tuple> iter = input.iterator(); iter.hasNext();) {
            result = true;
            Tuple tmp = iter.next();
            for (int i = 0; i < schema.size(); ++i)
                if (!newTuple.get(i).equals(tmp.get(i)))
                {
                    result = false;
                    break;
                }
            if (result)
                return true;
        }
        return false;
    }

    @Override
    public void visit(LOSort s) throws FrontendException {
        if (limit && !((PreOrderDepthFirstWalker) currentWalker).getBranchFlag())
            return;
        DataBag outputConstraints = outputConstraintsMap.get(s);
        outputConstraintsMap.remove(s);

        if (outputConstraints == null)
            outputConstraintsMap.put(s.getInput((LogicalPlan) plan), BagFactory.getInstance()
                    .newDefaultBag());
        else
            outputConstraintsMap.put(s.getInput((LogicalPlan) plan), outputConstraints);
    }

    @Override
    public void visit(LOSplit split) throws FrontendException {
        if (limit && !((PreOrderDepthFirstWalker) currentWalker).getBranchFlag())
          return;
    }

    @Override
    public void visit(LOStore store) throws FrontendException {
        if (limit && !((PreOrderDepthFirstWalker) currentWalker).getBranchFlag())
            return;
        DataBag outputConstraints = outputConstraintsMap.get(store);
        if (outputConstraints == null) {
            outputConstraintsMap.put(plan.getPredecessors(store)
                    .get(0), BagFactory.getInstance().newDefaultBag());
        } else {
            outputConstraintsMap.remove(store);
            outputConstraintsMap.put(plan.getPredecessors(store)
                    .get(0), outputConstraints);
        }
    }

    @Override
    public void visit(LOUnion u) throws FrontendException {
        if (limit && !((PreOrderDepthFirstWalker) currentWalker).getBranchFlag())
            return;
        DataBag outputConstraints = outputConstraintsMap.get(u);
        outputConstraintsMap.remove(u);
        if (outputConstraints == null || outputConstraints.size() == 0) {
            // we dont need to do anything
            // we just find the inputs, create empty bags as their
            // outputConstraints and return
            for (Operator op : u.getInputs((LogicalPlan) plan)) {
                DataBag constraints = BagFactory.getInstance().newDefaultBag();
                outputConstraintsMap.put(op, constraints);
            }
            return;
        }

        // since we have some outputConstraints, we apply them to the inputs
        // round-robin
        int count = 0;
        List<Operator> inputs = u.getInputs(((LogicalPlan) plan));
        int noInputs = inputs.size();

        for (Operator op : inputs) {
            DataBag constraint = BagFactory.getInstance().newDefaultBag();
            outputConstraintsMap.put(op, constraint);
        }
        for (Iterator<Tuple> it = outputConstraints.iterator(); it.hasNext();) {
            DataBag constraint = outputConstraintsMap.get(inputs.get(count));
            constraint.add(it.next());
            count = (count + 1) % noInputs;
        }

    }

    @Override
    public void visit(LOLimit lm) throws FrontendException {
        if (!limit) // not augment for LIMIT in this traversal
            return;

        if (oriLimitMap == null)
            oriLimitMap = new HashMap<LOLimit, Long>();

        DataBag outputConstraints = outputConstraintsMap.get(lm);
        outputConstraintsMap.remove(lm);

        DataBag inputConstraints = outputConstraintsMap.get(lm.getInput((LogicalPlan) plan));
        if (inputConstraints == null) {
            inputConstraints = BagFactory.getInstance().newDefaultBag();
            outputConstraintsMap.put(lm.getInput((LogicalPlan) plan), inputConstraints);
        }

        DataBag inputData = derivedData.get(lm.getInput((LogicalPlan) plan));

        if (outputConstraints != null && outputConstraints.size() > 0) { // there
            // 's
            // one
            // or
            // more
            // output
            // constraints
            // ;
            // generate
            // corresponding
            // input
            // constraints
            for (Iterator<Tuple> it = outputConstraints.iterator(); it
                  .hasNext();) {
                inputConstraints.add(it.next());
             // ... plus one more if only one
             if (inputConstraints.size() == 1) {
                inputConstraints.add(inputData.iterator().next());
                ((PreOrderDepthFirstWalker) currentWalker).setBranchFlag();
             }
          }
        } else if (inputConstraints.size() == 0){
            // add all input to input constraints ...
            inputConstraints.addAll(inputData);
            // ... plus one more if only one
            if (inputConstraints.size() == 1) {
                inputConstraints.add(inputData.iterator().next());
                ((PreOrderDepthFirstWalker) currentWalker).setBranchFlag();
            }
        }
        POLimit poLimit = (POLimit) logToPhysMap.get(lm);
        oriLimitMap.put(lm, Long.valueOf(poLimit.getLimit()));
        poLimit.setLimit(inputConstraints.size()-1);
        lm.setLimit(poLimit.getLimit());
    }

    Tuple GetGroupByInput(Object groupLabel, List<Integer> groupCols,
            int numFields) throws ExecException {
        Tuple t = TupleFactory.getInstance().newTuple(numFields);

        if (groupCols.size() == 1) {
            // GroupLabel would be a data atom
            t.set(groupCols.get(0), groupLabel);
        } else {
            if (!(groupLabel instanceof Tuple))
                throw new RuntimeException("Unrecognized group label!");
            Tuple group = (Tuple) groupLabel;
            for (int i = 0; i < groupCols.size(); i++) {
                t.set(groupCols.get(i), group.get(i));
            }
        }

        return t;
    }

    Tuple GetJoinInput(Tuple group, List<Integer> groupCols0, List<Integer> groupCols,
        int numFields) throws ExecException {
        Tuple t = TupleFactory.getInstance().newTuple(numFields);

        if (groupCols.size() == 1) {
            // GroupLabel would be a data atom
            t.set(groupCols.get(0), group.get(groupCols0.get(0)));
        } else {
            if (!(group instanceof Tuple))
                throw new RuntimeException("Unrecognized group label!");
            for (int i = 0; i < groupCols.size(); i++) {
                t.set(groupCols.get(i), group.get(groupCols0.get(i)));
            }
        }

        return t;
    }

    Tuple GetJoinInput(Tuple group, List<Integer> groupCols,
        int numFields) throws ExecException {
        Tuple t = TupleFactory.getInstance().newTuple(numFields);

        if (groupCols.size() == 1) {
            // GroupLabel would be a data atom
            t.set(groupCols.get(0), group);
        } else {
            if (!(group instanceof Tuple))
                throw new RuntimeException("Unrecognized group label!");
            for (int i = 0; i < groupCols.size(); i++) {
                t.set(groupCols.get(i), group.get(i));
            }
        }

        return t;
    }

    Tuple BackPropConstraint(Tuple outputConstraint, List<Integer> cols,
            LogicalSchema inputSchema, boolean cast) throws ExecException {
        Tuple inputConst = TupleFactory.getInstance().newTuple(
                inputSchema.getFields().size());

        Tuple inputConstraint = new ExampleTuple(inputConst);

        for (int outCol = 0; outCol < outputConstraint.size(); outCol++) {
            int inCol = cols.get(outCol);
            Object outVal = outputConstraint.get(outCol);
            Object inVal = inputConstraint.get(inCol);

            if (inVal == null && outVal != null) {
                // inputConstraint.set(inCol, outVal);
                inputConstraint.set(inCol, (cast) ? new DataByteArray(outVal
                        .toString().getBytes()) : outVal);

            } else {
                if (outVal != null) {
                    // unable to back-propagate, due to conflicting column
                    // constraints, so give up
                    return null;
                }
            }
        }

        return inputConstraint;
    }

    // generate a constraint tuple that conforms to the schema and passes the
    // predicate
    // (or null if unable to find such a tuple)

    ExampleTuple GenerateMatchingTuple(LogicalSchema schema, LogicalExpressionPlan plan,
            boolean invert) throws FrontendException, ExecException {
        return GenerateMatchingTuple(TupleFactory.getInstance().newTuple(
                schema.getFields().size()), plan, invert);
    }

    // generate a constraint tuple that conforms to the constraint and passes
    // the predicate
    // (or null if unable to find such a tuple)
    //
    // for now, constraint tuples are tuples whose fields are a blend of actual
    // data values and nulls,
    // where a null stands for "don't care"
    //
    // in the future, may want to replace "don't care" with a more rich
    // constraint language; this would
    // help, e.g. in the case of two filters in a row (you want the downstream
    // filter to tell the upstream filter
    // what predicate it wants satisfied in a given field)
    //

    ExampleTuple GenerateMatchingTuple(Tuple constraint, LogicalExpressionPlan predicate,
            boolean invert) throws ExecException, FrontendException {
        Tuple t = TupleFactory.getInstance().newTuple(constraint.size());
        ExampleTuple tOut = new ExampleTuple(t);
        for (int i = 0; i < t.size(); i++)
            tOut.set(i, constraint.get(i));

        GenerateMatchingTupleHelper(tOut, predicate
                .getSources().get(0), invert);
        tOut.synthetic = true;
        return tOut;

    }

    void GenerateMatchingTupleHelper(Tuple t, Operator pred,
            boolean invert) throws FrontendException, ExecException {
        if (pred instanceof BinaryExpression)
            GenerateMatchingTupleHelper(t, (BinaryExpression) pred,
                    invert);
        else if (pred instanceof NotExpression)
            GenerateMatchingTupleHelper(t, (NotExpression) pred, invert);
        else if (pred instanceof IsNullExpression)
            GenerateMatchingTupleHelper(t, (IsNullExpression) pred, invert);
        else if (pred instanceof UserFuncExpression)
            // Don't know how to generate input tuple for UDF, return null
            // to suppress the generation
            t = null;
        else
            throw new FrontendException("Unknown operator in filter predicate");
    }

    void GenerateMatchingTupleHelper(Tuple t, BinaryExpression pred,
            boolean invert) throws FrontendException, ExecException {

        if (pred instanceof AndExpression) {
            GenerateMatchingTupleHelper(t, (AndExpression) pred, invert);
            return;
        } else if (pred instanceof OrExpression) {
            GenerateMatchingTupleHelper(t, (OrExpression) pred, invert);
            return;
        }

        // now we are sure that the expression operators are the roots of the
        // plan

        boolean leftIsConst = false, rightIsConst = false;
        Object leftConst = null, rightConst = null;
        byte leftDataType = 0, rightDataType = 0;

        int leftCol = -1, rightCol = -1;

        if (pred instanceof AddExpression || pred instanceof SubtractExpression
                || pred instanceof MultiplyExpression || pred instanceof DivideExpression
                || pred instanceof ModExpression || pred instanceof RegexExpression)
            return; // We don't try to work around these operators right now

        if (pred.getLhs() instanceof ConstantExpression) {
            leftIsConst = true;
            leftConst = ((ConstantExpression) (pred.getLhs())).getValue();
        } else {
            LogicalExpression lhs = pred.getLhs();
            if (lhs instanceof CastExpression)
                lhs = ((CastExpression) lhs).getExpression();
            // if (!(pred.getLhsOperand() instanceof ProjectExpression && ((ProjectExpression)
            // pred
            // .getLhsOperand()).getProjection().size() == 1))
            // return; // too hard
            if (!(lhs instanceof ProjectExpression))
                return;
            leftCol = ((ProjectExpression) lhs).getColNum();
            leftDataType = ((ProjectExpression) lhs).getType();

            Object d = t.get(leftCol);
            if (d != null) {
                leftIsConst = true;
                leftConst = d;
            }
        }

        if (pred.getRhs() instanceof ConstantExpression) {
            rightIsConst = true;
            rightConst = ((ConstantExpression) (pred.getRhs())).getValue();
        } else {
            Operator rhs = pred.getRhs();
            if (rhs instanceof CastExpression)
                rhs = ((CastExpression) rhs).getExpression();
            // if (!(pred.getRhsOperand() instanceof ProjectExpression && ((ProjectExpression)
            // pred
            // .getRhsOperand()).getProjection().size() == 1))
            // return; // too hard
            if (!(rhs instanceof ProjectExpression))
                return;
            rightCol = ((ProjectExpression) rhs).getColNum();
            rightDataType = ((ProjectExpression) rhs).getType();

            Object d = t.get(rightCol);
            if (d != null) {
                rightIsConst = true;
                rightConst = d;
            }
        }

        if (leftIsConst && rightIsConst)
            return; // can't really change the result if both are constants

        // now we try to change some nulls to constants

        // convert some nulls to constants
        if (!invert) {
            if (pred instanceof EqualExpression) {
                if (leftIsConst) {
                    t.set(rightCol, generateData(rightDataType, leftConst
                            .toString()));
                } else if (rightIsConst) {
                    t.set(leftCol, generateData(leftDataType, rightConst
                            .toString()));
                } else {
                    t.set(leftCol, generateData(leftDataType, "0"));
                    t.set(rightCol, generateData(rightDataType, "0"));
                }
            } else if (pred instanceof NotEqualExpression) {
                if (leftIsConst) {
                    t.set(rightCol, generateData(rightDataType,
                            GetUnequalValue(leftConst).toString()));
                } else if (rightIsConst) {
                    t.set(leftCol, generateData(leftDataType, GetUnequalValue(
                            rightConst).toString()));
                } else {
                    t.set(leftCol, generateData(leftDataType, "0"));
                    t.set(rightCol, generateData(rightDataType, "1"));
                }
            } else if (pred instanceof GreaterThanExpression
                    || pred instanceof GreaterThanEqualExpression) {
                if (leftIsConst) {
                    t.set(rightCol, generateData(rightDataType,
                            GetSmallerValue(leftConst).toString()));
                } else if (rightIsConst) {
                    t.set(leftCol, generateData(leftDataType, GetLargerValue(
                            rightConst).toString()));
                } else {
                    t.set(leftCol, generateData(leftDataType, "1"));
                    t.set(rightCol, generateData(rightDataType, "0"));
                }
            } else if (pred instanceof LessThanExpression
                    || pred instanceof LessThanEqualExpression) {
                if (leftIsConst) {
                    t.set(rightCol, generateData(rightDataType, GetLargerValue(
                            leftConst).toString()));
                } else if (rightIsConst) {
                    t.set(leftCol, generateData(leftDataType, GetSmallerValue(
                            rightConst).toString()));
                } else {
                    t.set(leftCol, generateData(leftDataType, "0"));
                    t.set(rightCol, generateData(rightDataType, "1"));
                }
            }
        } else {
            if (pred instanceof EqualExpression) {
                if (leftIsConst) {
                    t.set(rightCol, generateData(rightDataType,
                            GetUnequalValue(leftConst).toString()));
                } else if (rightIsConst) {
                    t.set(leftCol, generateData(leftDataType, GetUnequalValue(
                            rightConst).toString()));
                } else {
                    t.set(leftCol, generateData(leftDataType, "0"));
                    t.set(rightCol, generateData(rightDataType, "1"));
                }
            } else if (pred instanceof NotEqualExpression) {
                if (leftIsConst) {
                    t.set(rightCol, generateData(rightDataType, leftConst
                            .toString()));
                } else if (rightIsConst) {
                    t.set(leftCol, generateData(leftDataType, rightConst
                            .toString()));
                } else {
                    t.set(leftCol, generateData(leftDataType, "0"));
                    t.set(rightCol, generateData(rightDataType, "0"));
                }
            } else if (pred instanceof GreaterThanExpression
                    || pred instanceof GreaterThanEqualExpression) {
                if (leftIsConst) {
                    t.set(rightCol, generateData(rightDataType, GetLargerValue(
                            leftConst).toString()));
                } else if (rightIsConst) {
                    t.set(leftCol, generateData(leftDataType, GetSmallerValue(
                            rightConst).toString()));
                } else {
                    t.set(leftCol, generateData(leftDataType, "0"));
                    t.set(rightCol, generateData(rightDataType, "1"));
                }
            } else if (pred instanceof LessThanExpression
                    || pred instanceof LessThanEqualExpression) {
                if (leftIsConst) {
                    t.set(rightCol, generateData(rightDataType,
                            GetSmallerValue(leftConst).toString()));
                } else if (rightIsConst) {
                    t.set(leftCol, generateData(leftDataType, GetLargerValue(
                            rightConst).toString()));
                } else {
                    t.set(leftCol, generateData(leftDataType, "1"));
                    t.set(rightCol, generateData(rightDataType, "0"));
                }
            }
        }

    }

    void GenerateMatchingTupleHelper(Tuple t, AndExpression op, boolean invert)
            throws FrontendException, ExecException {
        Operator input = op.getLhs();
        GenerateMatchingTupleHelper(t, input, invert);
        input = op.getRhs();
        GenerateMatchingTupleHelper(t, input, invert);

    }

    void GenerateMatchingTupleHelper(Tuple t, OrExpression op, boolean invert)
            throws FrontendException, ExecException {
        Operator input = op.getLhs();
        GenerateMatchingTupleHelper(t, input, invert);
        input = op.getRhs();
        GenerateMatchingTupleHelper(t, input, invert);

    }

    void GenerateMatchingTupleHelper(Tuple t, NotExpression op, boolean invert)
            throws FrontendException, ExecException {
        LogicalExpression input = op.getExpression();
        GenerateMatchingTupleHelper(t, input, !invert);

    }

    void GenerateMatchingTupleHelper(Tuple t, IsNullExpression op, boolean invert)
            throws FrontendException, ExecException {
        byte type = op.getExpression().getType();
        if (!invert)
            t.set(0, null);
        else
            t.set(0, generateData(type, "0"));
    }

    Object GetUnequalValue(Object v) {
        byte type = DataType.findType(v);

        if (type == DataType.BAG || type == DataType.TUPLE
                || type == DataType.MAP)
            return null;

        Object zero = generateData(type, "0");

        if (v.equals(zero))
            return generateData(type, "1");

        return zero;
    }

    Object GetSmallerValue(Object v) {
        byte type = DataType.findType(v);

        if (type == DataType.BAG || type == DataType.TUPLE
                || type == DataType.MAP)
            return null;

        switch (type) {
        case DataType.CHARARRAY:
            String str = (String) v;
            if (str.length() > 0)
                return str.substring(0, str.length() - 1);
            else
                return null;
        case DataType.BYTEARRAY:
            DataByteArray data = (DataByteArray) v;
            if (data.size() > 0)
                return new DataByteArray(data.get(), 0, data.size() - 1);
            else
                return null;
        case DataType.INTEGER:
            return Integer.valueOf((Integer) v - 1);
        case DataType.LONG:
            return Long.valueOf((Long) v - 1);
        case DataType.FLOAT:
            return Float.valueOf((Float) v - 1);
        case DataType.DOUBLE:
            return Double.valueOf((Double) v - 1);
        case DataType.BIGINTEGER:
            return ((BigInteger)v).subtract(BigInteger.ONE);
        case DataType.BIGDECIMAL:
            return ((BigDecimal)v).subtract(BigDecimal.ONE);
        case DataType.DATETIME:
            DateTime dt = (DateTime) v;
            if (dt.getMillisOfSecond() != 0) {
                return dt.minusMillis(1);
            } else if (dt.getSecondOfMinute() != 0) {
                return dt.minusSeconds(1);
            } else if (dt.getMinuteOfHour() != 0) {
                return dt.minusMinutes(1);
            } else if (dt.getHourOfDay() != 0) {
                return dt.minusHours(1);
            } else {
                return dt.minusDays(1);
            }
        default:
            return null;
        }

    }

    Object GetLargerValue(Object v) {
        byte type = DataType.findType(v);

        if (type == DataType.BAG || type == DataType.TUPLE
                || type == DataType.MAP)
            return null;

        switch (type) {
        case DataType.CHARARRAY:
            return (String) v + "0";
        case DataType.BYTEARRAY:
            String str = ((DataByteArray) v).toString();
            str = str + "0";
            return new DataByteArray(str);
        case DataType.INTEGER:
            return Integer.valueOf((Integer) v + 1);
        case DataType.LONG:
            return Long.valueOf((Long) v + 1);
        case DataType.FLOAT:
            return Float.valueOf((Float) v + 1);
        case DataType.DOUBLE:
            return Double.valueOf((Double) v + 1);
        case DataType.BIGINTEGER:
            return ((BigInteger)v).add(BigInteger.ONE);
        case DataType.BIGDECIMAL:
            return ((BigDecimal)v).add(BigDecimal.ONE);
        case DataType.DATETIME:
            DateTime dt = (DateTime) v;
            if (dt.getMillisOfSecond() != 0) {
                return dt.plusMillis(1);
            } else if (dt.getSecondOfMinute() != 0) {
                return dt.plusSeconds(1);
            } else if (dt.getMinuteOfHour() != 0) {
                return dt.plusMinutes(1);
            } else if (dt.getHourOfDay() != 0) {
                return dt.plusHours(1);
            } else {
                return dt.plusDays(1);
            }
        default:
            return null;
        }
    }

    Object generateData(byte type, String data) {
        switch (type) {
        case DataType.BOOLEAN:
            if (data.equalsIgnoreCase("true")) {
                return Boolean.TRUE;
            } else if (data.equalsIgnoreCase("false")) {
                return Boolean.FALSE;
            } else {
                return null;
            }
        case DataType.BYTEARRAY:
            return new DataByteArray(data.getBytes());
        case DataType.DOUBLE:
            return Double.valueOf(data);
        case DataType.FLOAT:
            return Float.valueOf(data);
        case DataType.INTEGER:
            return Integer.valueOf(data);
        case DataType.LONG:
            return Long.valueOf(data);
        case DataType.BIGINTEGER:
            return new BigInteger(data);
        case DataType.BIGDECIMAL:
            return new BigDecimal(data);
        case DataType.DATETIME:
            return new DateTime(data);
        case DataType.CHARARRAY:
            return data;
        default:
            return null;
        }
    }

}
