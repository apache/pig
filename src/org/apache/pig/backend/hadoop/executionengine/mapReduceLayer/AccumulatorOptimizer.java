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

package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.Accumulator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.BinaryExpressionOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ConstantExpression;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ExpressionOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POBinCond;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POCast;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POMapLookUp;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.PORelationToExprProject;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POUserFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.UnaryExpressionOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PODistinct;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSortedDistinct;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.VisitorException;

/**
 * A visitor to optimize plans that determines if a reduce plan
 * can run in accumulative mode. 
 */
public class AccumulatorOptimizer extends MROpPlanVisitor {

    private Log log = LogFactory.getLog(getClass());

    public AccumulatorOptimizer(MROperPlan plan) {
        super(plan, new DepthFirstWalker<MapReduceOper, MROperPlan>(plan));
    }

    public void visitMROp(MapReduceOper mr) throws VisitorException {
        // See if this is a map-reduce job
        List<PhysicalOperator> pos = mr.reducePlan.getRoots();
        if (pos == null || pos.size() == 0) {        
            return;
        }
        
       // See if this is a POPackage
        PhysicalOperator po_package = pos.get(0);
        if (!po_package.getClass().equals(POPackage.class)) {            
            return; 
        }
        
        // if POPackage is for distinct, just return
        if (((POPackage)po_package).isDistinct()) {
            return;
        }
        
        // if any input to POPackage is inner, just return
        boolean[] isInner = ((POPackage)po_package).getInner();
        for(boolean b: isInner) {
            if (b) {
                return;
            }
        }
        
        List<PhysicalOperator> l = mr.reducePlan.getSuccessors(po_package);
        // there should be only one POForEach
        if (l == null || l.size() == 0 || l.size() > 1) {        	
            return;
        }
        
        PhysicalOperator po_foreach = l.get(0);
        if (!(po_foreach instanceof POForEach)) {            
            return; 
        }
        
        boolean foundUDF = false;
        List<PhysicalPlan> list = ((POForEach)po_foreach).getInputPlans();
        for(PhysicalPlan p: list) {
            PhysicalOperator po = p.getLeaves().get(0);
            
            // only expression operators are allowed
            if (!(po instanceof ExpressionOperator)) {
                return;
            }
            
            if (((ExpressionOperator)po).containUDF()) {
                foundUDF = true;
            }
            
            if (!check(po)) {
                return;
            }
        }
        
        if (foundUDF) {
            // if all tests are passed, reducer can run in accumulative mode
            log.info("Reducer is to run in accumulative mode.");
            po_package.setAccumulative();
            po_foreach.setAccumulative();
        }
    }
     
    /**
     * Check if an operator is qualified to be under POForEach
     * to turn on accumulator. The operator must be in the following list or
     * an <code>POUserFunc</code>.
     *
     * If the operator has sub-operators, they must also belong to this list.
     * <li>ConstantExpression</li>
     * <li>POProject, whose result type is not BAG, or TUPLE and overloaded</li>
     * <li>POMapLookup</li>
     * <li>POCase</li>
     * <li>UnaryExpressionOperator</li>
     * <li>BinaryExpressionOperator</li>
     * <li>POBinCond</li>
     *
     * If the operator is <code>POUserFunc</code>, it must implement
     * <code>Accumulator</code> interface and its inputs pass the check
     * by calling <code>checkUDFInput()</code>
     *
     * @param po the operator to be checked on
     * @return <code>true</code> if it is ok, <code>false</code>
     *    if not.
     */
    @SuppressWarnings("unchecked")
    private boolean check(PhysicalOperator po) {
        if (po instanceof ConstantExpression) {
            return true;
        }
        
        if (po instanceof POCast) {
            return check(po.getInputs().get(0));
        }
        
        if (po instanceof POMapLookUp) {
            return check(po.getInputs().get(0));
        }
        
        if (po instanceof POProject) {
            // POProject can not project data bag
            if (((POProject)po).getResultType() == DataType.BAG) {
                return false;
            }
            
            // POProject can not overload a data bag
            if (((POProject)po).getResultType() == DataType.TUPLE && ((POProject)po).isOverloaded()) {
                return false;
            }
            
            return true;
        }    	
         
        if (po instanceof UnaryExpressionOperator) {
            return check(((UnaryExpressionOperator)po).getExpr());
        }
        
        if (po instanceof BinaryExpressionOperator) {
            return check(((BinaryExpressionOperator)po).getLhs()) &&
                    check(((BinaryExpressionOperator)po).getRhs());
        }
        
        if (po instanceof POBinCond) {
            return check(((POBinCond)po).getLhs()) &&
                check(((POBinCond)po).getRhs()) && check(((POBinCond)po).getCond());
        }
                
        if (po instanceof POUserFunc) {
            String className = ((POUserFunc)po).getFuncSpec().getClassName();
            Class c = null;
            try {
                c = PigContext.resolveClassName(className);
            }catch(Exception e) {
                return false;
            }
            if (!Accumulator.class.isAssignableFrom(c)) {
                return false;
            }    		    	
            
            // check input of UDF
             List<PhysicalOperator> inputs = po.getInputs();
             for(PhysicalOperator p: inputs) {
                 if (!checkUDFInput(p)) {
                     return false;
                 }
             }
             
             return true;
        }
        
        return false;
    }
    
    /**
     * Check operators under POUserFunc to verify if this
      * is a valid UDF to run as accumulator. The inputs to
     * <code>POUserFunc</code> must be in the following list.
     * If the operator has sub-operators, they must also belong
     * to this list.
     *
     * <li>PORelationToExprProject</li>
     * <li>ConstantExpression</li>
     * <li>POProject</li>
     * <li>POCase</li>
      * <li>UnaryExpressionOperator</li>
     * <li>BinaryExpressionOperator</li>
     * <li>POBinCond</li>
     * <li>POSortedDistinct</li>
     * <li>POForEach</li>
     *
     */
    private boolean checkUDFInput(PhysicalOperator po) {    	
        if (po instanceof PORelationToExprProject) {
            return checkUDFInput(po.getInputs().get(0));
        }

        if (po instanceof POProject) {
            if(po.getInputs() == null )
                return true;
            else 
                return checkUDFInput(po.getInputs().get(0));
        }
        
        if (po instanceof ConstantExpression) {
            return true;
        }
        
        if (po instanceof UnaryExpressionOperator) {
            return checkUDFInput(((UnaryExpressionOperator)po).getExpr());
        }
        
        if (po instanceof BinaryExpressionOperator) {
            return checkUDFInput(((BinaryExpressionOperator)po).getLhs()) ||
            checkUDFInput(((BinaryExpressionOperator)po).getRhs());
        }
        
        if (po instanceof POCast) {
            return checkUDFInput(po.getInputs().get(0));
        }
        
        if (po instanceof POBinCond) {
            return checkUDFInput(((POBinCond)po).getLhs()) &&
            checkUDFInput(((POBinCond)po).getRhs()) && checkUDFInput(((POBinCond)po).getCond());
        }
        
        if (po instanceof POSortedDistinct) {    		    		
            return true;    	
        }
        
        if (po instanceof POForEach) {
            List<PhysicalPlan> list = ((POForEach)po).getInputPlans();
            if (list.size() != 1) {
                return false;
            }
            
            PhysicalOperator p = list.get(0).getLeaves().get(0);
            if (checkUDFInput(p)) {
                return checkUDFInput(po.getInputs().get(0));
            }
        }  
        
        return false;
    }
}
