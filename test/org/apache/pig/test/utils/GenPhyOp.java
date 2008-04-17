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
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.OperatorKey;
import org.apache.pig.impl.physicalLayer.plans.ExprPlan;
import org.apache.pig.impl.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POFilter;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POForEach;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POGenerate;
import org.apache.pig.impl.physicalLayer.topLevelOperators.PhysicalOperator;
// import org.apache.pig.impl.physicalLayer.topLevelOperators.POGlobalRearrange;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POLoad;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POLocalRearrange;
// import org.apache.pig.impl.physicalLayer.topLevelOperators.POPackage;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POStore;
// import org.apache.pig.impl.physicalLayer.topLevelOperators.StartMap;
import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.ConstantExpression;
import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.ExpressionOperator;
import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.POProject;
// import
// org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.binaryExprOps.comparators.EqualToExpr;
// import
// org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.binaryExprOps.comparators.GTOrEqualToExpr;
import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.binaryExprOps.comparators.EqualToExpr;
import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.binaryExprOps.comparators.GTOrEqualToExpr;
import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.binaryExprOps.comparators.GreaterThanExpr;
import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.binaryExprOps.comparators.LTOrEqualToExpr;
import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.binaryExprOps.comparators.LessThanExpr;
import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.binaryExprOps.comparators.NotEqualToExpr;

public class GenPhyOp {
    static Random r = new Random();

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

    public static POProject exprProject() {
        POProject ret = new POProject(new OperatorKey("", r.nextLong()));
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

    public static NotEqualToExpr compNotEqualToExpr() {
        NotEqualToExpr ret = new NotEqualToExpr(new OperatorKey("", r
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
        return ret;
    }
    
    public static POForEach topForEachOp() {
        POForEach ret = new POForEach(new OperatorKey("", r
                .nextLong()));
        return ret;
    }

    public static POGenerate topGenerateOp() {
        POGenerate ret = new POGenerate(new OperatorKey("", r.nextLong()));
        return ret;
    }
    
    /**
     * creates the POGenerate operator for 
     * generate grpCol, *.
     * 
     * @param grpCol - The column to be grouped on
     * @param sample - The sample tuple that is used to infer
     *                  result types and #projects for *
     * @return - The POGenerate operator which has the exprplan
     *              for generate grpCol, * set.
     * @throws IOException
     */
    public static POGenerate topGenerateOpWithExPlan(int grpCol, Tuple sample) throws IOException {
        POProject prj1 = new POProject(new OperatorKey("", r.nextLong()), -1, grpCol);
        prj1.setResultType(sample.getType(grpCol));
        prj1.setOverloaded(false);
        
        

        List<Boolean> toBeFlattened = new LinkedList<Boolean>();
        toBeFlattened.add(false);
        

        ExprPlan plan1 = new ExprPlan();
        plan1.add(prj1);
        
        List<ExprPlan> inputs = new LinkedList<ExprPlan>();
        inputs.add(plan1);
        
        POProject rest[] = new POProject[sample.size()];
        int i=-1;
        for (POProject project : rest) {
            project = new POProject(new OperatorKey("", r.nextLong()), -1, ++i);
            project.setResultType(sample.getType(i));
            project.setOverloaded(false);
            
            ExprPlan pl = new ExprPlan();
            pl.add(project);
            
            toBeFlattened.add(false);
            inputs.add(pl);
        }

        

        POGenerate ret = new POGenerate(new OperatorKey("", r.nextLong()),
                inputs, toBeFlattened);
        return ret;
    }
    
    /**
     * creates the POGenerate operator for 
     * 'generate field'.
     * 
     * @param field - The column to be generated
     * @param sample - The sample tuple that is used to infer
     *                  result type
     * @return - The POGenerate operator which has the exprplan
     *              for 'generate field' set.
     * @throws IOException
     */
    public static POGenerate topGenerateOpWithExPlanForFe(int field, Tuple sample) throws IOException {
        POProject prj1 = new POProject(new OperatorKey("", r.nextLong()), -1, field);
        prj1.setResultType(sample.getType(field));
        prj1.setOverloaded(false);
        
        

        List<Boolean> toBeFlattened = new LinkedList<Boolean>();
        toBeFlattened.add(false);
        

        ExprPlan plan1 = new ExprPlan();
        plan1.add(prj1);
        
        List<ExprPlan> inputs = new LinkedList<ExprPlan>();
        inputs.add(plan1);
        
        POGenerate ret = new POGenerate(new OperatorKey("", r.nextLong()),
                inputs, toBeFlattened);
        return ret;
    }
    
    /**
     * creates the POLocalRearrange operator with the given index for
     * group by grpCol
     * @param index - The input index of this POLocalRearrange operator
     * @param grpCol - The column to be grouped on
     * @param sample - Sample tuple needed for topGenerateOpWithExPlan
     * @return - The POLocalRearrange operator
     * @throws IOException
     */
    public static POLocalRearrange topLocalRearrangeOPWithPlan(int index, int grpCol, Tuple sample) throws IOException{
        POGenerate gen = topGenerateOpWithExPlan(grpCol, sample);
        PhysicalPlan<PhysicalOperator> pp = new PhysicalPlan<PhysicalOperator>();
        pp.add(gen);
        
        POLocalRearrange ret = topLocalRearrangeOp();
        ret.setPlan(pp);
        ret.setIndex(index);
        ret.setResultType(DataType.TUPLE);
        return ret;
    }
    
    /**
     * creates the POForEach operator for
     * foreach A generate field
     * @param field - The column to be generated
     * @param sample - Sample tuple needed for topGenerateOpWithExPlanForFe
     * @return - The POForEach operator
     * @throws IOException
     */
    public static POForEach topForEachOPWithPlan(int field, Tuple sample) throws IOException{
        POGenerate gen = topGenerateOpWithExPlanForFe(field, sample);
        PhysicalPlan<PhysicalOperator> pp = new PhysicalPlan<PhysicalOperator>();
        pp.add(gen);
        
        POForEach ret = topForEachOp();
        ret.setPlan(pp);
        ret.setResultType(DataType.TUPLE);
        return ret;
    }

    public static POLoad topLoadOp() {
        POLoad ret = new POLoad(new OperatorKey("", r.nextLong()));
        return ret;
    }

    public static POFilter topFilterOp() {
        POFilter ret = new POFilter(new OperatorKey("", r.nextLong()));
        return ret;
    }

    public static POFilter topFilterOpWithExPlan(int lhsVal, int rhsVal)
            throws IOException {
        POFilter ret = new POFilter(new OperatorKey("", r.nextLong()));

        ConstantExpression ce1 = GenPhyOp.exprConst();
        ce1.setValue(lhsVal);

        ConstantExpression ce2 = GenPhyOp.exprConst();
        ce2.setValue(rhsVal);

        GreaterThanExpr gr = GenPhyOp.compGreaterThanExpr();
        gr.setLhs(ce1);
        gr.setRhs(ce2);
        gr.setOperandType(DataType.INTEGER);

        ExprPlan ep = new ExprPlan();
        ep.add(ce1);
        ep.add(ce2);
        ep.add(gr);

        ep.connect(ce1, gr);
        ep.connect(ce2, gr);

        ret.setPlan(ep);

        return ret;
    }

    public static POFilter topFilterOpWithProj(int col, int rhsVal)
            throws IOException {
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

        ExprPlan ep = new ExprPlan();
        ep.add(proj);
        ep.add(ce2);
        ep.add(gr);

        ep.connect(proj, gr);
        ep.connect(ce2, gr);

        ret.setPlan(ep);

        return ret;
    }

    //    
    // public static POGlobalRearrange topGlobalRearrangeOp(){
    // POGlobalRearrange ret = new POGlobalRearrange(new
    // OperatorKey("",r.nextLong()));
    // return ret;
    // }
    //    
    // public static POPackage topPackageOp(){
    // POPackage ret = new POPackage(new OperatorKey("",r.nextLong()));
    // return ret;
    // }
    //    
    public static POStore topStoreOp() {
        POStore ret = new POStore(new OperatorKey("", r.nextLong()));
        return ret;
    }
    //    
    // public static StartMap topStartMapOp(){
    // StartMap ret = new StartMap(new OperatorKey("",r.nextLong()));
    //        return ret;
    //    }
}
