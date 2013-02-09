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
package org.apache.pig.test.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.pig.FuncSpec;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.test.PORead;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.*;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.*;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.util.Utils;

public class GenPhyOp{
    static Random r = new Random();
    
    public static final byte GTE = 1;
    public static final byte GT = 2;
    public static final byte LTE = 3;
    public static final byte LT = 4;

    public static class PlansAndFlattens {
        public List<PhysicalPlan> plans;
        public List<Boolean> flattens;

        public PlansAndFlattens(List<PhysicalPlan> p, List<Boolean> f) {
            plans = p;
            flattens = f;
        }
    };
    
    public static PigContext pc;
    
    public static ConstantExpression exprConst() {
        ConstantExpression ret = new ConstantExpression(new OperatorKey("", r
                .nextLong()));
        return ret;
    }

    public static GreaterThanExpr compGreaterThanExpr() {
        GreaterThanExpr ret = new GreaterThanExpr(new OperatorKey("", r
                .nextLong()));
        return ret;
    }

    public static GreaterThanExpr compGreaterThanExpr(
            ExpressionOperator lhs,
            ExpressionOperator rhs,
            byte type) {
        GreaterThanExpr ret = new GreaterThanExpr(new OperatorKey("", r
                .nextLong()));
        ret.setLhs(lhs);
        ret.setRhs(rhs);
        ret.setOperandType(type);
        return ret;
    }

    public static POAnd compAndExpr(
            ExpressionOperator lhs,
            ExpressionOperator rhs) {
        POAnd ret = new POAnd(new OperatorKey("", r
                .nextLong()));
        ret.setLhs(lhs);
        ret.setRhs(rhs);
        ret.setOperandType(DataType.BOOLEAN);
        return ret;
    }

    public static POProject exprProject() {
        POProject ret = new POProject(new OperatorKey("", r.nextLong()));
        return ret;
    }

    public static POProject exprProject(int col) {
        POProject ret = new POProject(new OperatorKey("", r.nextLong()), 1, col);
        return ret;
    }

    public static GTOrEqualToExpr compGTOrEqualToExpr() {
        GTOrEqualToExpr ret = new GTOrEqualToExpr(new OperatorKey("", r
                .nextLong()));
        return ret;
    }

    public static EqualToExpr compEqualToExpr() {
        EqualToExpr ret = new EqualToExpr(new OperatorKey("", r.nextLong()));
        return ret;
    }

    public static EqualToExpr compEqualToExpr(
            ExpressionOperator lhs,
            ExpressionOperator rhs,
            byte type) {
        EqualToExpr ret = new EqualToExpr(new OperatorKey("", r
                .nextLong()));
        ret.setLhs(lhs);
        ret.setRhs(rhs);
        ret.setOperandType(type);
        return ret;
    }

    public static NotEqualToExpr compNotEqualToExpr() {
        NotEqualToExpr ret = new NotEqualToExpr(new OperatorKey("", r
                .nextLong()));
        return ret;
    }

    public static POIsNull compIsNullExpr() {
        POIsNull ret = new POIsNull(new OperatorKey("", r
                .nextLong()));
        return ret;
    }

    public static LessThanExpr compLessThanExpr() {
        LessThanExpr ret = new LessThanExpr(new OperatorKey("", r.nextLong()));
        return ret;
    }

    public static LTOrEqualToExpr compLTOrEqualToExpr() {
        LTOrEqualToExpr ret = new LTOrEqualToExpr(new OperatorKey("", r
                .nextLong()));
        return ret;
    }

    public static POLocalRearrange topLocalRearrangeOp() {
        POLocalRearrange ret = new POLocalRearrange(new OperatorKey("", r
                .nextLong()));
        List<PhysicalPlan> plans = new LinkedList<PhysicalPlan>();
        try {
            ret.setPlans(plans);
        } catch(PlanException pe) {
        }
        return ret;
    }
    
    public static POGlobalRearrange topGlobalRearrangeOp(){
        POGlobalRearrange ret = new POGlobalRearrange(new OperatorKey("", r
                .nextLong()));
        return ret;
    }
    
    public static POPackage topPackageOp(){
        POPackage ret = new POPackage(new OperatorKey("", r.nextLong()));
        return ret;
    }
    
    public static POForEach topForEachOp() {
        POForEach ret = new POForEach(new OperatorKey("", r
                .nextLong()));
        return ret;
    }

    public static POUnion topUnionOp() {
        POUnion ret = new POUnion(new OperatorKey("", r.nextLong()));
        return ret;
    }
    
    public static POPartialAgg topPOPartialAgg(){
        POPartialAgg partAgg =  new POPartialAgg(getOK());
        return partAgg;
    }
    
    /**
     * creates the PlansAndFlattens struct for 
     * generate grpCol, *.
     * 
     * @param grpCol - The column to be grouped on
     * @param sample - The sample tuple that is used to infer
     *                  result types and #projects for *
     * @return - The PlansAndFlattens struct which has the exprplan
     *              for generate grpCol, * set.
     * @throws ExecException
     */
    public static PlansAndFlattens topGenerateOpWithExPlan(
            int grpCol,
            Tuple sample) throws ExecException {
        POProject prj1 = new POProject(new OperatorKey("", r.nextLong()), -1, grpCol);
        prj1.setResultType(sample.getType(grpCol));
        prj1.setOverloaded(false);
        
        

        List<Boolean> toBeFlattened = new LinkedList<Boolean>();
        toBeFlattened.add(false);
        

        PhysicalPlan plan1 = new PhysicalPlan();
        plan1.add(prj1);
        
        List<PhysicalPlan> inputs = new LinkedList<PhysicalPlan>();
        inputs.add(plan1);
        
        POProject rest[] = new POProject[sample.size()];
        int i=-1;
        for (POProject project : rest) {
            project = new POProject(new OperatorKey("", r.nextLong()), -1, ++i);
            project.setResultType(sample.getType(i));
            project.setOverloaded(false);
            
            PhysicalPlan pl = new PhysicalPlan();
            pl.add(project);
            
            toBeFlattened.add(false);
            inputs.add(pl);
        }

        

        return new PlansAndFlattens(inputs, toBeFlattened);
    }
    
    /**
     * creates the PlansAndFlattens struct for 
     * generate grpCol, *.
     * 
     * @param grpCol - The column to be grouped on
     * @param sample - The sample tuple that is used to infer
     *                  result types and #projects for *
     * @return - The PlansAndFlattens struct which has the exprplan
     *              for generate grpCol, * set.
     * @throws ExecException
     * @throws PlanException 
     */
    public static PlansAndFlattens topGenerateOpWithExPlanLR(
            int grpCol,
            Tuple sample) throws ExecException, PlanException {
        POProject prj1 = new POProject(new OperatorKey("", r.nextLong()), -1, grpCol);
        prj1.setResultType(sample.getType(grpCol));
        prj1.setOverloaded(false);
        
        POCastDummy cst = new POCastDummy(new OperatorKey("",r.nextLong()));
        cst.setResultType(sample.getType(grpCol));

        List<Boolean> toBeFlattened = new LinkedList<Boolean>();
        toBeFlattened.add(false);
        

        PhysicalPlan plan1 = new PhysicalPlan();
        plan1.add(prj1);
        plan1.add(cst);
        plan1.connect(prj1, cst);
        
        List<PhysicalPlan> inputs = new LinkedList<PhysicalPlan>();
        inputs.add(plan1);
        
        POProject rest[] = new POProject[sample.size()];
        POCastDummy csts[] = new POCastDummy[sample.size()];
        int i=-1;
        for (POProject project : rest) {
            project = new POProject(new OperatorKey("", r.nextLong()), -1, ++i);
            project.setResultType(sample.getType(i));
            project.setOverloaded(false);
            
            csts[i] = new POCastDummy(new OperatorKey("",r.nextLong()));
            csts[i].setResultType(sample.getType(i));
            
            PhysicalPlan pl = new PhysicalPlan();
            pl.add(project);
            pl.add(csts[i]);
            pl.connect(project, csts[i]);
            
            toBeFlattened.add(false);
            inputs.add(pl);
        }

        

        return new PlansAndFlattens(inputs, toBeFlattened);
    }
    
    /**
     * creates the PlansAndFlattens struct for 
     * 'generate field'.
     * 
     * @param field - The column to be generated
     * @param sample - The sample tuple that is used to infer
     *                  result type
     * @return - The PlansAndFlattens struct which has the exprplan
     *              for 'generate field' set.
     * @throws ExecException
     */
    public static PlansAndFlattens topGenerateOpWithExPlanForFe(
            int field,
            Tuple sample) throws ExecException {
        POProject prj1 = new POProject(new OperatorKey("", r.nextLong()), -1, field);
        prj1.setResultType(sample.getType(field));
        prj1.setOverloaded(false);
        
        

        List<Boolean> toBeFlattened = new LinkedList<Boolean>();
        toBeFlattened.add(false);
        

        PhysicalPlan plan1 = new PhysicalPlan();
        plan1.add(prj1);
        
        List<PhysicalPlan> inputs = new LinkedList<PhysicalPlan>();
        inputs.add(plan1);
        
        return new PlansAndFlattens(inputs, toBeFlattened);
    }
    
    /**
     * creates the PlansAndFlattens struct for 
     * 'generate flatten(field)'.
     * 
     * @param field - The column to be generated
     * @param sample - The sample tuple that is used to infer
     *                  result type
     * @return - The PlansAndFlattens struct which has the exprplan
     *              for 'generate field' set.
     * @throws ExecException
     */
    public static PlansAndFlattens topGenerateOpWithExPlanForFeFlat(int field) throws ExecException {
        POProject prj1 = new POProject(new OperatorKey("", r.nextLong()), -1, field);
        prj1.setResultType(DataType.BAG);
        prj1.setOverloaded(false);

        List<Boolean> toBeFlattened = new LinkedList<Boolean>();
        toBeFlattened.add(true);

        PhysicalPlan plan1 = new PhysicalPlan();
        plan1.add(prj1);
        
        List<PhysicalPlan> inputs = new LinkedList<PhysicalPlan>();
        inputs.add(plan1);
        
        return new PlansAndFlattens(inputs, toBeFlattened);
    }
    
    /**
     * creates the PlansAndFlattens struct for 
     * 'generate field[0] field[1] ...'.
     *
     * @param fields - The columns to be generated
     * @param sample - The sample tuple that is used to infer
     *                  result type
     * @return - The PlansAndFlattens struct which has the exprplan
     *              for 'generate field[0] field[1]' set.
     * @throws ExecException
     * @throws PlanException 
     */
    public static PlansAndFlattens topGenerateOpWithExPlanForFe(
            int[] fields,
            Tuple sample) throws ExecException, PlanException {
        POProject[] prj = new POProject[fields.length];

        for(int i=0;i<prj.length;i++){
            prj[i] = new POProject(new OperatorKey("", r.nextLong()), -1, fields[i]);
            prj[i].setResultType(sample.getType(fields[i]));
            prj[i].setOverloaded(false);
        }
        
        
        POCastDummy[] cst = new POCastDummy[fields.length];

        List<Boolean> toBeFlattened = new LinkedList<Boolean>();
        for (POProject project : prj)
            toBeFlattened.add(false);
        
        

        List<PhysicalPlan> inputs = new LinkedList<PhysicalPlan>();
        PhysicalPlan[] plans = new PhysicalPlan[fields.length];
        for (int i=0;i<plans.length;i++) {
            plans[i] = new PhysicalPlan();
            plans[i].add(prj[i]);
            cst[i] = new POCastDummy(new OperatorKey("",r.nextLong()));
            cst[i].setResultType(sample.getType(fields[i]));
            plans[i].add(cst[i]);
            plans[i].connect(prj[i], cst[i]);
            inputs.add(plans[i]);
        }
        
        return new PlansAndFlattens(inputs, toBeFlattened);
    }
    
    /**
     * creates the PlansAndFlattens struct for 
     * 'generate field[0] field[1] ...'.
     * with the flatten list as specified
     * @param fields - The columns to be generated
     * @param toBeFlattened - The columns to be flattened
     * @param sample - The sample tuple that is used to infer
     *                  result type
     * @return - The PlansAndFlattens struct which has the exprplan
     *              for 'generate field[0] field[1]' set.
     * @throws ExecException
     * @throws PlanException 
     */
    public static PlansAndFlattens topGenerateOpWithExPlanForFe(
            int[] fields,
            Tuple sample,
            List<Boolean> toBeFlattened) throws ExecException, PlanException {
        POProject[] prj = new POProject[fields.length];

        for(int i=0;i<prj.length;i++){
            prj[i] = new POProject(new OperatorKey("", r.nextLong()), -1, fields[i]);
            prj[i].setResultType(sample.getType(fields[i]));
            prj[i].setOverloaded(false);
        }
        
        
        POCastDummy[] cst = new POCastDummy[fields.length];

        List<PhysicalPlan> inputs = new LinkedList<PhysicalPlan>();
        PhysicalPlan[] plans = new PhysicalPlan[fields.length];
        for (int i=0;i<plans.length;i++) {
            plans[i] = new PhysicalPlan();
            plans[i].add(prj[i]);
            cst[i] = new POCastDummy(new OperatorKey("",r.nextLong()));
            cst[i].setResultType(sample.getType(fields[i]));
            plans[i].add(cst[i]);
            plans[i].connect(prj[i], cst[i]);
            inputs.add(plans[i]);
        }
        
        return new PlansAndFlattens(inputs, toBeFlattened);
    }
    
    /**
     * creates the POLocalRearrange operator with the given index for
     * group by grpCol
     * @param index - The input index of this POLocalRearrange operator
     * @param grpCol - The column to be grouped on
     * @param sample - Sample tuple needed for topGenerateOpWithExPlan
     * @return - The POLocalRearrange operator
     * @throws ExecException
     */
    public static POLocalRearrange topLocalRearrangeOPWithPlan(int index, int grpCol, Tuple sample) throws ExecException, PlanException{
        POLocalRearrange lr = topLocalRearrangeOPWithPlanPlain(index, grpCol, sample);
        List<PhysicalPlan> plans = lr.getPlans(); 
        PhysicalPlan ep = plans.get(0);
        POCastDummy cst = new POCastDummy(new OperatorKey("", r.nextLong()));
        cst.setResultType(sample.getType(grpCol));
        ep.addAsLeaf(cst);
        lr.setPlans(plans);
        return lr;
    }
    
    /**
     * creates the POLocalRearrange operator with the given index for
     * group by grpCol
     * @param index - The input index of this POLocalRearrange operator
     * @param grpCol - The column to be grouped on
     * @param sample - Sample tuple needed for topGenerateOpWithExPlan
     * @return - The POLocalRearrange operator
     * @throws ExecException
     */
    public static POLocalRearrange topLocalRearrangeOPWithPlanPlain(int index, int grpCol, Tuple sample) throws ExecException, PlanException{
        POProject prj1 = new POProject(new OperatorKey("", r.nextLong()), -1, grpCol);
        prj1.setResultType(sample.getType(grpCol));
        prj1.setOverloaded(false);

        PhysicalPlan plan1 = new PhysicalPlan();
        plan1.add(prj1);
        
        List<PhysicalPlan> plans = new ArrayList<PhysicalPlan>();
        plans.add(plan1);
        POLocalRearrange ret = topLocalRearrangeOp();
        ret.setPlans(plans);
        ret.setIndex(index);
        ret.setResultType(DataType.TUPLE);
        ret.setKeyType(sample.getType(grpCol));
        return ret;
    }
    
    /**
     * creates the POForEach operator for
     * foreach A generate field
     * @param field - The column to be generated
     * @param sample - Sample tuple needed for topGenerateOpWithExPlanForFe
     * @return - The POForEach operator
     * @throws ExecException
     */
    public static POForEach topForEachOPWithPlan(int field, Tuple sample) throws ExecException, PlanException{
        PlansAndFlattens pf = topGenerateOpWithExPlanForFe(field, sample);
        
        POForEach ret = topForEachOp();
        ret.setInputPlans(pf.plans);
        ret.setToBeFlattened(pf.flattens);
        ret.setResultType(DataType.TUPLE);
        return ret;
    }

    /**
     * creates the POForEach operator for
     * foreach A generate field[0] field[1]
     * @param fields - The columns to be generated
     * @param sample - Sample tuple needed for topGenerateOpWithExPlanForFe
     * @return - The POForEach operator
     * @throws ExecException
     */
    public static POForEach topForEachOPWithPlan(int[] fields, Tuple sample) throws ExecException, PlanException{
        PlansAndFlattens pf = topGenerateOpWithExPlanForFe(fields, sample);
        
        POForEach ret = topForEachOp();
        ret.setInputPlans(pf.plans);
        ret.setToBeFlattened(pf.flattens);
        ret.setResultType(DataType.TUPLE);
        return ret;
    }
    
    /**
     * creates the POForEach operator for
     * foreach A generate field[0] field[1]
     * @param fields - The columns to be generated
     * @param sample - Sample tuple needed for topGenerateOpWithExPlanForFe
     * @return - The POForEach operator
     * @throws ExecException
     */
    public static POForEach topForEachOPWithPlan(
            int[] fields,
            Tuple sample,
            List<Boolean> toBeFlattened) throws ExecException, PlanException{
        PlansAndFlattens pf =
            topGenerateOpWithExPlanForFe(fields, sample, toBeFlattened);
        
        POForEach ret = topForEachOp();
        ret.setInputPlans(pf.plans);
        ret.setToBeFlattened(toBeFlattened);
        ret.setResultType(DataType.TUPLE);
        return ret;
    }
    
    /**
     * creates the POForEach operator for
     * foreach A generate flatten(field)
     * @param fields - The columns to be generated
     * @param sample - Sample tuple needed for topGenerateOpWithExPlanForFe
     * @return - The POForEach operator
     * @throws ExecException
     */
    public static POForEach topForEachOPWithPlan(int field) throws ExecException, PlanException{
        PlansAndFlattens pf = topGenerateOpWithExPlanForFeFlat(field);
        
        POForEach ret = topForEachOp();
        ret.setInputPlans(pf.plans);
        ret.setToBeFlattened(pf.flattens);
        ret.setResultType(DataType.TUPLE);
        return ret;
    }

    
    public static POLoad topLoadOp() {
        POLoad ret = new POLoad(new OperatorKey("", r.nextLong()));
        ret.setPc(pc);
        return ret;
    }

    public static POFilter topFilterOp() {
        POFilter ret = new POFilter(new OperatorKey("", r.nextLong()));
        ConstantExpression ex = GenPhyOp.exprConst();
        ex.setValue(new Boolean(true));
        PhysicalPlan pp = new PhysicalPlan();
        pp.add(ex);
        ret.setPlan(pp);
        return ret;
    }
    
    public static POFilter connectedFilterOp(PhysicalOperator input) {
        List<PhysicalOperator> ops = new ArrayList<PhysicalOperator>(1);
        ops.add(input);
        POFilter ret = new POFilter(new OperatorKey("", r.nextLong()), ops);
        return ret;
    }
    
    public static POLimit topLimitOp() {
        POLimit ret = new POLimit(new OperatorKey("", r.nextLong()));
        return ret;
    }

    public static POFilter topFilterOpWithExPlan(int lhsVal, int rhsVal)
            throws ExecException, PlanException {
        POFilter ret = new POFilter(new OperatorKey("", r.nextLong()));

        ConstantExpression ce1 = GenPhyOp.exprConst();
        ce1.setValue(lhsVal);

        ConstantExpression ce2 = GenPhyOp.exprConst();
        ce2.setValue(rhsVal);

        GreaterThanExpr gr = GenPhyOp.compGreaterThanExpr();
        gr.setLhs(ce1);
        gr.setRhs(ce2);
        gr.setOperandType(DataType.INTEGER);

        PhysicalPlan ep = new PhysicalPlan();
        ep.add(ce1);
        ep.add(ce2);
        ep.add(gr);

        ep.connect(ce1, gr);
        ep.connect(ce2, gr);

        ret.setPlan(ep);

        return ret;
    }

    public static POFilter topFilterOpWithProj(int col, int rhsVal)
            throws ExecException, PlanException {
        POFilter ret = new POFilter(new OperatorKey("", r.nextLong()));

        POProject proj = exprProject();
        proj.setResultType(DataType.INTEGER);
        proj.setColumn(col);
        proj.setOverloaded(false);

        ConstantExpression ce2 = GenPhyOp.exprConst();
        ce2.setValue(rhsVal);

        GreaterThanExpr gr = GenPhyOp.compGreaterThanExpr();
        gr.setLhs(proj);
        gr.setRhs(ce2);
        gr.setOperandType(DataType.INTEGER);

        PhysicalPlan ep = new PhysicalPlan();
        ep.add(proj);
        ep.add(ce2);
        ep.add(gr);

        ep.connect(proj, gr);
        ep.connect(ce2, gr);

        ret.setPlan(ep);

        return ret;
    }
    
    public static POFilter topFilterOpWithProj(int col, int rhsVal,
            byte CompType) throws ExecException, PlanException {
        POFilter ret = new POFilter(new OperatorKey("", r.nextLong()));

        POProject proj = exprProject();
        proj.setResultType(DataType.INTEGER);
        proj.setColumn(col);
        proj.setOverloaded(false);

        ConstantExpression ce2 = GenPhyOp.exprConst();
        ce2.setValue(rhsVal);

        BinaryComparisonOperator cop = null;
        switch (CompType) {
        case GenPhyOp.GTE:
            cop = GenPhyOp.compGTOrEqualToExpr();
            break;
        case GenPhyOp.GT:
            cop = GenPhyOp.compGreaterThanExpr();
            break;
        case GenPhyOp.LTE:
            cop = GenPhyOp.compLTOrEqualToExpr();
            break;
        case GenPhyOp.LT:
            cop = GenPhyOp.compLessThanExpr();
            break;
        }

        cop.setLhs(proj);
        cop.setRhs(ce2);
        cop.setOperandType(DataType.INTEGER);

        PhysicalPlan ep = new PhysicalPlan();
        ep.add(proj);
        ep.add(ce2);
        ep.add(cop);

        ep.connect(proj, cop);
        ep.connect(ce2, cop);

        ret.setPlan(ep);

        return ret;
    }
    
    public static POFilter topFilterOpWithProjWithCast(int col, int rhsVal, byte CompType)
            throws ExecException, PlanException {
        POFilter ret = new POFilter(new OperatorKey("", r.nextLong()));

        POProject proj = exprProject();
        proj.setResultType(DataType.INTEGER);
        proj.setColumn(col);
        proj.setOverloaded(false);

        ConstantExpression ce2 = GenPhyOp.exprConst();
        ce2.setValue(rhsVal);
        
        BinaryComparisonOperator cop = null;
        switch(CompType){
        case GenPhyOp.GTE:
            cop = GenPhyOp.compGTOrEqualToExpr();
            break;
        case GenPhyOp.GT:
            cop = GenPhyOp.compGreaterThanExpr();
            break;
        case GenPhyOp.LTE:
            cop = GenPhyOp.compLTOrEqualToExpr();
            break;
        case GenPhyOp.LT:
            cop = GenPhyOp.compLessThanExpr();
            break;
        }
        
        POCastDummy cst = new POCastDummy(new OperatorKey("",r.nextLong()));
        
        cop.setLhs(cst);
        cop.setRhs(ce2);
        cop.setOperandType(DataType.INTEGER);

        PhysicalPlan ep = new PhysicalPlan();
        ep.add(cst);
        ep.add(proj);
        ep.add(ce2);
        ep.add(cop);

        ep.connect(proj, cst);
        ep.connect(cst, cop);
        ep.connect(ce2, cop);

        ret.setPlan(ep);

        return ret;
    }
    
    public static PORead topReadOp(DataBag bag) {
        PORead ret = new PORead(new OperatorKey("", r.nextLong()), bag);
        return ret;
    }
    
    public static POStore dummyPigStorageOp() {
        POStore ret = new POStore(new OperatorKey("", r.nextLong()));
        ret.setSFile(new FileSpec("DummyFil", new FuncSpec(PigStorage.class.getName() + "()")));
        return ret;
    }

    public static POStore topStoreOp() {
        POStore ret = new POStore(new OperatorKey("", r.nextLong()));
        return ret;
    }

    public static void setR(Random r) {
        GenPhyOp.r = r;
    }
    
    public static MapReduceOper MROp(){
        MapReduceOper ret = new MapReduceOper(new OperatorKey("",r.nextLong()));
        return ret;
    }
    
    private static FileSpec getTempFileSpec() throws IOException {
        return new FileSpec(FileLocalizer.getTemporaryPath(pc).toString(),
                new FuncSpec(Utils.getTmpFileCompressorName(pc))
        );
    }
    
    public static POSplit topSplitOp() throws IOException{
        POSplit ret = new POSplit(new OperatorKey("",r.nextLong()));
        ret.setSplitStore(getTempFileSpec());
        return ret;
    }
    
    public static PhysicalPlan grpChain() throws ExecException, PlanException{
        PhysicalPlan grpChain = new PhysicalPlan();
        POLocalRearrange lr = GenPhyOp.topLocalRearrangeOp();
        POGlobalRearrange gr = GenPhyOp.topGlobalRearrangeOp();
        POPackage pk = GenPhyOp.topPackageOp();
        
        grpChain.add(lr);
        grpChain.add(gr);
        grpChain.add(pk);
        
        grpChain.connect(lr, gr);
        grpChain.connect(gr, pk);
        
        return grpChain;
    }
    
    public static PhysicalPlan loadedGrpChain() throws ExecException, PlanException{
        PhysicalPlan ret = new PhysicalPlan();
        POLoad ld = GenPhyOp.topLoadOp();
        POLocalRearrange lr = GenPhyOp.topLocalRearrangeOp();
        POGlobalRearrange gr = GenPhyOp.topGlobalRearrangeOp();
        POPackage pk = GenPhyOp.topPackageOp();
        
        ret.add(ld);
        ret.add(lr);
        ret.add(gr);
        ret.add(pk);
        
        ret.connect(ld, lr);
        ret.connect(lr, gr);
        ret.connect(gr, pk);
        
        return ret;
    }
    
    public static PhysicalPlan loadedFilter() throws ExecException, PlanException{
        PhysicalPlan ret = new PhysicalPlan();
        POLoad ld = GenPhyOp.topLoadOp();
        POFilter fl = GenPhyOp.topFilterOp();
        ret.add(ld);
        ret.add(fl);
        
        ret.connect(ld, fl);
        return ret;
    }
    
    public static POForEach topForEachOPWithUDF(List<String> clsName) throws PlanException{
        List<PhysicalPlan> ep4s = new ArrayList<PhysicalPlan>();
        List<Boolean> flattened3 = new ArrayList<Boolean>();
        for (String string : clsName) {
            PhysicalPlan ep4 = new PhysicalPlan();
            POProject prjStar4 = new POProject(new OperatorKey("", r.nextLong()));
            prjStar4.setResultType(DataType.TUPLE);
            prjStar4.setStar(true);
            ep4.add(prjStar4);
            
            List ufInps = new ArrayList();
            ufInps.add(prjStar4);
            POUserFunc uf = new POUserFunc(new OperatorKey("", r.nextLong()), -1, ufInps, new FuncSpec(string));
            ep4.add(uf);
            ep4.connect(prjStar4, uf);
            ep4s.add(ep4);
            flattened3.add(false);
        }
        
        POForEach fe3 = new POForEach(new OperatorKey("", r.nextLong()), 1,
            ep4s, flattened3);
        fe3.setResultType(DataType.TUPLE);
        return fe3;
    }
    
    public static PhysicalPlan arithPlan() throws PlanException{
        PhysicalPlan ep = new PhysicalPlan();
        ConstantExpression ce[] = new ConstantExpression[7];
        for(int i=0;i<ce.length;i++){
            ce[i] = GenPhyOp.exprConst();
            ce[i].setValue(i);
            ep.add(ce[i]);
        }
        
        Add ad = new Add(getOK());
        ep.add(ad);
        ep.connect(ce[0], ad);
        ep.connect(ce[1], ad);
        
        Divide div = new Divide(getOK());
        ep.add(div);
        ep.connect(ce[2], div);
        ep.connect(ce[3], div);
        
        Subtract sub = new Subtract(getOK());
        ep.add(sub);
        ep.connect(ad, sub);
        ep.connect(div, sub);
        
        Mod mod = new Mod(getOK());
        ep.add(mod);
        ep.connect(ce[4], mod);
        ep.connect(ce[5], mod);
        
        Multiply mul1 = new Multiply(getOK());
        ep.add(mul1);
        ep.connect(mod, mul1);
        ep.connect(ce[6], mul1);
        
        Multiply mul2 = new Multiply(getOK());
        ep.add(mul2);
        ep.connect(sub, mul2);
        ep.connect(mul1, mul2);
        
        return ep;
    }
    
    public static OperatorKey getOK(){
        return new OperatorKey("",r.nextLong());
    }
    
    public static void setPc(PigContext pc) {
        GenPhyOp.pc = pc;
    }
}
