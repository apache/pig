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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.BinaryExpressionOperator;
import org.apache.pig.impl.logicalLayer.ExpressionOperator;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.LOAdd;
import org.apache.pig.impl.logicalLayer.LOAnd;
import org.apache.pig.impl.logicalLayer.LOCast;
import org.apache.pig.impl.logicalLayer.LOCogroup;
import org.apache.pig.impl.logicalLayer.LOConst;
import org.apache.pig.impl.logicalLayer.LOCross;
import org.apache.pig.impl.logicalLayer.LODistinct;
import org.apache.pig.impl.logicalLayer.LODivide;
import org.apache.pig.impl.logicalLayer.LOEqual;
import org.apache.pig.impl.logicalLayer.LOFilter;
import org.apache.pig.impl.logicalLayer.LOForEach;
import org.apache.pig.impl.logicalLayer.LOGreaterThan;
import org.apache.pig.impl.logicalLayer.LOGreaterThanEqual;
import org.apache.pig.impl.logicalLayer.LOLesserThan;
import org.apache.pig.impl.logicalLayer.LOLesserThanEqual;
import org.apache.pig.impl.logicalLayer.LOLoad;
import org.apache.pig.impl.logicalLayer.LOMod;
import org.apache.pig.impl.logicalLayer.LOMultiply;
import org.apache.pig.impl.logicalLayer.LONot;
import org.apache.pig.impl.logicalLayer.LONotEqual;
import org.apache.pig.impl.logicalLayer.LOOr;
import org.apache.pig.impl.logicalLayer.LOProject;
import org.apache.pig.impl.logicalLayer.LORegexp;
import org.apache.pig.impl.logicalLayer.LOSort;
import org.apache.pig.impl.logicalLayer.LOSplit;
import org.apache.pig.impl.logicalLayer.LOStore;
import org.apache.pig.impl.logicalLayer.LOSubtract;
import org.apache.pig.impl.logicalLayer.LOUnion;
import org.apache.pig.impl.logicalLayer.LOVisitor;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.pen.util.ExampleTuple;
import org.apache.pig.pen.util.PreOrderDepthFirstWalker;

//This is used to generate synthetic data
//Synthetic data generation is done by making constraint tuples for each operator as we traverse the plan
//and try to replace the constraints with values as far as possible. We only deal with simple conditions right now

public class AugmentBaseDataVisitor extends LOVisitor {

    Map<LOLoad, DataBag> baseData = null;
    Map<LOLoad, DataBag> newBaseData = new HashMap<LOLoad, DataBag>();
    Map<LogicalOperator, DataBag> derivedData = null;

    Map<LogicalOperator, DataBag> outputConstraintsMap = new HashMap<LogicalOperator, DataBag>();

    Log log = LogFactory.getLog(getClass());

    // Augmentation moves from the leaves to root and hence needs a
    // depthfirstwalker
    public AugmentBaseDataVisitor(LogicalPlan plan,
            Map<LOLoad, DataBag> baseData,
            Map<LogicalOperator, DataBag> derivedData) {
        super(plan, new PreOrderDepthFirstWalker<LogicalOperator, LogicalPlan>(
                plan));
        this.baseData = baseData;
        this.derivedData = derivedData;

    }

    public Map<LOLoad, DataBag> getNewBaseData() {
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

    @Override
    protected void visit(LOCogroup cg) throws VisitorException {
        // we first get the outputconstraints for the current cogroup
        DataBag outputConstraints = outputConstraintsMap.get(cg);
        outputConstraintsMap.remove(cg);
        boolean ableToHandle = true;
        // we then check if we can handle this cogroup and try to collect some
        // information about grouping
        List<List<Integer>> groupSpecs = new LinkedList<List<Integer>>();
        int numCols = -1;

        int minGroupSize = (cg.getInputs().size() == 1) ? 1 : 2;

        for (LogicalOperator op : cg.getInputs()) {
            List<LogicalPlan> groupByPlans = (List<LogicalPlan>) cg
                    .getGroupByPlans().get(op);
            List<Integer> groupCols = new ArrayList<Integer>();
            for (LogicalPlan plan : groupByPlans) {
                LogicalOperator leaf = plan.getLeaves().get(0);
                if (leaf instanceof LOProject) {
                    groupCols.add(((LOProject) leaf).getCol());
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
                int numInputs = cg.getInputs().size();
                if (outputConstraints != null) {
                    for (Iterator<Tuple> it = outputConstraints.iterator(); it
                            .hasNext();) {
                        Tuple outputConstraint = it.next();
                        Object groupLabel = outputConstraint.get(0);

                        for (int input = 0; input < numInputs; input++) {

                            int numInputFields = cg.getInputs().get(input)
                                    .getSchema().size();
                            List<Integer> groupCols = groupSpecs.get(input);

                            DataBag output = outputConstraintsMap.get(cg
                                    .getInputs().get(input));
                            if (output == null) {
                                output = BagFactory.getInstance()
                                        .newDefaultBag();
                                outputConstraintsMap.put(cg.getInputs().get(
                                        input), output);
                            }
                            for (int i = 0; i < minGroupSize; i++) {
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
                        int numInputFields = cg.getInputs().get(input)
                                .getSchema().size();
                        List<Integer> groupCols = groupSpecs.get(input);

                        DataBag output = outputConstraintsMap.get(cg
                                .getInputs().get(input));
                        if (output == null) {
                            output = BagFactory.getInstance().newDefaultBag();
                            outputConstraintsMap.put(cg.getInputs().get(input),
                                    output);
                        }
                        int numTupsToAdd = minGroupSize
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
            throw new VisitorException(
                    "Error visiting Cogroup during Augmentation phase of Example Generator! "
                            + e.getMessage());
        }
    }

    @Override
    protected void visit(LOCross cs) throws VisitorException {

    }

    @Override
    protected void visit(LODistinct dt) throws VisitorException {

    }

    @Override
    protected void visit(LOFilter filter) throws VisitorException {
        DataBag outputConstraints = outputConstraintsMap.get(filter);
        outputConstraintsMap.remove(filter);

        LogicalPlan filterCond = filter.getComparisonPlan();
        DataBag inputConstraints = outputConstraintsMap.get(filter.getInput());
        if (inputConstraints == null) {
            inputConstraints = BagFactory.getInstance().newDefaultBag();
            outputConstraintsMap.put(filter.getInput(), inputConstraints);
        }

        DataBag outputData = derivedData.get(filter);
        DataBag inputData = derivedData.get(filter.getInput());
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
                            + e.getMessage());
            throw new VisitorException(
                    "Error visiting Load during Augmentation phase of Example Generator! "
                            + e.getMessage());
        }
    }

    @Override
    protected void visit(LOForEach forEach) throws VisitorException {
        DataBag outputConstraints = outputConstraintsMap.get(forEach);
        outputConstraintsMap.remove(forEach);
        List<LogicalPlan> plans = forEach.getForEachPlans();
        boolean ableToHandle = true;
        List<Integer> cols = new ArrayList<Integer>();
        boolean cast = false;

        if (outputConstraints == null || outputConstraints.size() == 0)
            // we dont have to do anything in this case
            return;

        for (LogicalPlan plan : plans) {
            LogicalOperator op = plan.getLeaves().get(0);
            if (op instanceof LOCast) {
                cast = true;
                op = ((LOCast) op).getExpression();
            }

            if (!(op instanceof LOProject)) {
                ableToHandle = false;
                break;
            } else {
                cols.add(((LOProject) op).getCol());
            }
        }

        if (ableToHandle) {
            // we can only handle simple projections
            DataBag output = BagFactory.getInstance().newDefaultBag();
            for (Iterator<Tuple> it = outputConstraints.iterator(); it
                    .hasNext();) {
                Tuple outputConstraint = it.next();
                try {
                    Tuple inputConstraint = BackPropConstraint(
                            outputConstraint, cols, (forEach.getPlan()
                                    .getPredecessors(forEach)).get(0)
                                    .getSchema(), cast);
                    output.add(inputConstraint);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new VisitorException(
                            "Operator error during Augmenting Phase in Example Generator "
                                    + e.getMessage());
                }
            }
            outputConstraintsMap.put(forEach.getPlan().getPredecessors(forEach)
                    .get(0), output);
        }

    }

    @Override
    protected void visit(LOLoad load) throws VisitorException {
        DataBag inputData = baseData.get(load);

        DataBag newInputData = newBaseData.get(load);
        if (newInputData == null) {
            newInputData = BagFactory.getInstance().newDefaultBag();
            newBaseData.put(load, newInputData);
        }

        Schema schema;
        try {
            schema = load.getSchema();
            if (schema == null)
                throw new RuntimeException(
                        "Example Generator requires a schema. Please provide a schema while loading data");
        } catch (FrontendException e) {

            log
                    .error("Error visiting Load during Augmentation phase of Example Generator! "
                            + e.getMessage());
            throw new VisitorException(
                    "Error visiting Load during Augmentation phase of Example Generator! "
                            + e.getMessage());
        }
        DataBag outputConstraints = outputConstraintsMap.get(load);
        outputConstraintsMap.remove(load);
        // check if the inputData exists
        if (inputData == null || inputData.size() == 0) {
            log.error("No input data found!");
            throw new RuntimeException("No input data found!");
        }

        // first of all, we are required to guarantee that there is at least one
        // output tuple
        if (outputConstraints == null || outputConstraints.size() == 0) {
            outputConstraints = BagFactory.getInstance().newDefaultBag();
            outputConstraints.add(TupleFactory.getInstance().newTuple(
                    schema.getFields().size()));
        }

        // create example tuple to steal values from when we encounter
        // "don't care" fields (i.e. null fields)
        Tuple exampleTuple = inputData.iterator().next();

        // run through output constraints; for each one synthesize a tuple and
        // add it to the base data
        // (while synthesizing individual fields, try to match fields that exist
        // in the real data)
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
                    if (d == null)
                        d = exampleTuple.get(i);
                    inputTuple.set(i, d);
                }
            } catch (ExecException e) {
                log
                        .error("Error visiting Load during Augmentation phase of Example Generator! "
                                + e.getMessage());
                throw new VisitorException(
                        "Error visiting Load during Augmentation phase of Example Generator! "
                                + e.getMessage());

            }
            if (!inputTuple.equals(exampleTuple))
                inputTuple.synthetic = true;

            newInputData.add(inputTuple);
        }
    }

    @Override
    protected void visit(LOSort s) throws VisitorException {
        DataBag outputConstraints = outputConstraintsMap.get(s);
        outputConstraintsMap.remove(s);

        if (outputConstraints == null)
            outputConstraintsMap.put(s.getInput(), BagFactory.getInstance()
                    .newDefaultBag());
        else
            outputConstraintsMap.put(s.getInput(), outputConstraints);
    }

    @Override
    protected void visit(LOSplit split) throws VisitorException {

    }

    @Override
    protected void visit(LOStore store) throws VisitorException {
        DataBag outputConstraints = outputConstraintsMap.get(store);
        if (outputConstraints == null) {
            outputConstraintsMap.put(store.getPlan().getPredecessors(store)
                    .get(0), BagFactory.getInstance().newDefaultBag());
        } else {
            outputConstraintsMap.remove(store);
            outputConstraintsMap.put(store.getPlan().getPredecessors(store)
                    .get(0), outputConstraints);
        }
    }

    @Override
    protected void visit(LOUnion u) throws VisitorException {
        DataBag outputConstraints = outputConstraintsMap.get(u);
        outputConstraintsMap.remove(u);
        if (outputConstraints == null || outputConstraints.size() == 0) {
            // we dont need to do anything
            // we just find the inputs, create empty bags as their
            // outputConstraints and return
            for (LogicalOperator op : u.getInputs()) {
                DataBag constraints = BagFactory.getInstance().newDefaultBag();
                outputConstraintsMap.put(op, constraints);
            }
            return;
        }

        // since we have some outputConstraints, we apply them to the inputs
        // round-robin
        int count = 0;
        List<LogicalOperator> inputs = u.getInputs();
        int noInputs = inputs.size();

        for (LogicalOperator op : inputs) {
            DataBag constraint = BagFactory.getInstance().newDefaultBag();
            outputConstraintsMap.put(op, constraint);
        }
        for (Iterator<Tuple> it = outputConstraints.iterator(); it.hasNext();) {
            DataBag constraint = outputConstraintsMap.get(inputs.get(count));
            constraint.add(it.next());
            count = (count + 1) % noInputs;
        }

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

    Tuple BackPropConstraint(Tuple outputConstraint, List<Integer> cols,
            Schema inputSchema, boolean cast) throws ExecException {
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

    ExampleTuple GenerateMatchingTuple(Schema schema, LogicalPlan plan,
            boolean invert) throws ExecException {
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

    ExampleTuple GenerateMatchingTuple(Tuple constraint, LogicalPlan predicate,
            boolean invert) throws ExecException {
        Tuple t = TupleFactory.getInstance().newTuple(constraint.size());
        ExampleTuple tOut = new ExampleTuple(t);
        for (int i = 0; i < t.size(); i++)
            tOut.set(i, constraint.get(i));

        GenerateMatchingTupleHelper(tOut, (ExpressionOperator) predicate
                .getLeaves().get(0), invert);
        tOut.synthetic = true;
        return tOut;

    }

    void GenerateMatchingTupleHelper(Tuple t, ExpressionOperator pred,
            boolean invert) throws ExecException {
        if (pred instanceof BinaryExpressionOperator)
            GenerateMatchingTupleHelper(t, (BinaryExpressionOperator) pred,
                    invert);
        else if (pred instanceof LONot)
            GenerateMatchingTupleHelper(t, (LONot) pred, invert);
        else
            throw new ExecException("Unknown operator in filter predicate");
    }

    void GenerateMatchingTupleHelper(Tuple t, BinaryExpressionOperator pred,
            boolean invert) throws ExecException {

        if (pred instanceof LOAnd) {
            GenerateMatchingTupleHelper(t, (LOAnd) pred, invert);
            return;
        } else if (pred instanceof LOOr) {
            GenerateMatchingTupleHelper(t, (LOOr) pred, invert);
            return;
        }

        // now we are sure that the expression operators are the roots of the
        // plan

        boolean leftIsConst = false, rightIsConst = false;
        Object leftConst = null, rightConst = null;
        byte leftDataType = 0, rightDataType = 0;

        int leftCol = -1, rightCol = -1;

        if (pred instanceof LOAdd || pred instanceof LOSubtract
                || pred instanceof LOMultiply || pred instanceof LODivide
                || pred instanceof LOMod || pred instanceof LORegexp)
            return; // We don't try to work around these operators right now

        if (pred.getLhsOperand() instanceof LOConst) {
            leftIsConst = true;
            leftConst = ((LOConst) (pred.getLhsOperand())).getValue();
        } else {
            LogicalOperator lhs = pred.getLhsOperand();
            if (lhs instanceof LOCast)
                lhs = ((LOCast) lhs).getExpression();
            // if (!(pred.getLhsOperand() instanceof LOProject && ((LOProject)
            // pred
            // .getLhsOperand()).getProjection().size() == 1))
            // return; // too hard
            if (!(lhs instanceof LOProject && ((LOProject) lhs).getProjection()
                    .size() == 1))
                return;
            leftCol = ((LOProject) lhs).getCol();
            leftDataType = ((LOProject) lhs).getType();

            Object d = t.get(leftCol);
            if (d != null) {
                leftIsConst = true;
                leftConst = d;
            }
        }

        if (pred.getRhsOperand() instanceof LOConst) {
            rightIsConst = true;
            rightConst = ((LOConst) (pred.getRhsOperand())).getValue();
        } else {
            LogicalOperator rhs = pred.getRhsOperand();
            if (rhs instanceof LOCast)
                rhs = ((LOCast) rhs).getExpression();
            // if (!(pred.getRhsOperand() instanceof LOProject && ((LOProject)
            // pred
            // .getRhsOperand()).getProjection().size() == 1))
            // return; // too hard
            if (!(rhs instanceof LOProject && ((LOProject) rhs).getProjection()
                    .size() == 1))
                return;
            rightCol = ((LOProject) rhs).getCol();
            rightDataType = ((LOProject) rhs).getType();

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
            if (pred instanceof LOEqual) {
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
            } else if (pred instanceof LONotEqual) {
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
            } else if (pred instanceof LOGreaterThan
                    || pred instanceof LOGreaterThanEqual) {
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
            } else if (pred instanceof LOLesserThan
                    || pred instanceof LOLesserThanEqual) {
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
            if (pred instanceof LOEqual) {
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
            } else if (pred instanceof LONotEqual) {
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
            } else if (pred instanceof LOGreaterThan
                    || pred instanceof LOGreaterThanEqual) {
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
            } else if (pred instanceof LOLesserThan
                    || pred instanceof LOLesserThanEqual) {
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

    void GenerateMatchingTupleHelper(Tuple t, LOAnd op, boolean invert)
            throws ExecException {
        ExpressionOperator input = op.getLhsOperand();
        GenerateMatchingTupleHelper(t, input, invert);
        input = op.getRhsOperand();
        GenerateMatchingTupleHelper(t, input, invert);

    }

    void GenerateMatchingTupleHelper(Tuple t, LOOr op, boolean invert)
            throws ExecException {
        ExpressionOperator input = op.getLhsOperand();
        GenerateMatchingTupleHelper(t, input, invert);
        input = op.getRhsOperand();
        GenerateMatchingTupleHelper(t, input, invert);

    }

    void GenerateMatchingTupleHelper(Tuple t, LONot op, boolean invert)
            throws ExecException {
        ExpressionOperator input = op.getOperand();
        GenerateMatchingTupleHelper(t, input, !invert);

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
                return new String(str.substring(0, str.length() - 1));
            else
                return null;
        case DataType.BYTEARRAY:
            DataByteArray data = (DataByteArray) v;
            if (data.size() > 0)
                return new DataByteArray(data.get(), 0, data.size() - 1);
            else
                return null;
        case DataType.INTEGER:
            return new Integer((Integer) v - 1);
        case DataType.LONG:
            return new Long((Long) v - 1);
        case DataType.FLOAT:
            return new Float((Float) v - 1);
        case DataType.DOUBLE:
            return new Double((Double) v - 1);
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
            return new String((String) v + "0");
        case DataType.BYTEARRAY:
            String str = ((DataByteArray) v).toString();
            str = str + "0";
            return new DataByteArray(str);
        case DataType.INTEGER:
            return new Integer((Integer) v + 1);
        case DataType.LONG:
            return new Long((Long) v + 1);
        case DataType.FLOAT:
            return new Float((Float) v + 1);
        case DataType.DOUBLE:
            return new Double((Double) v + 1);
        default:
            return null;
        }
    }

    Object generateData(byte type, String data) {
        switch (type) {
        case DataType.BOOLEAN:
            return new Boolean(data);
        case DataType.BYTEARRAY:
            return new DataByteArray(data.getBytes());
        case DataType.DOUBLE:
            return new Double(data);
        case DataType.FLOAT:
            return new Float(data);
        case DataType.INTEGER:
            return new Integer(data);
        case DataType.LONG:
            return new Long(data);
        case DataType.CHARARRAY:
            return data;
        default:
            return null;
        }
    }

}
