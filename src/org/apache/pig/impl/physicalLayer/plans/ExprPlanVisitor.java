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
package org.apache.pig.impl.physicalLayer.plans;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.ConstantExpression;
import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.ExpressionOperator;
import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.POProject;
//import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.binaryExprOps.comparators.EqualToExpr;
//import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.binaryExprOps.comparators.GTOrEqualToExpr;
import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.binaryExprOps.arithmeticOperators.Add;
import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.binaryExprOps.arithmeticOperators.Divide;
import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.binaryExprOps.arithmeticOperators.Mod;
import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.binaryExprOps.arithmeticOperators.Multiply;
import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.binaryExprOps.arithmeticOperators.Subtract;
import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.binaryExprOps.comparators.EqualToExpr;
import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.binaryExprOps.comparators.GTOrEqualToExpr;
import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.binaryExprOps.comparators.GreaterThanExpr;
import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.binaryExprOps.comparators.LTOrEqualToExpr;
import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.binaryExprOps.comparators.LessThanExpr;
import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.binaryExprOps.comparators.NotEqualToExpr;

/**
 * The visitor to be used for visiting expression plans.
 * Create the visitor with the ExpressionPlan to be visited.
 * Call the visit() method for a depth first traversal.
 *
 */
public class ExprPlanVisitor extends PhyPlanVisitor<ExpressionOperator, ExprPlan> {

    private final Log log = LogFactory.getLog(getClass());
    
    public ExprPlanVisitor(ExprPlan plan) {
        super(plan);
        // TODO Auto-generated constructor stub
    }
    
    public void visitConstant(ConstantExpression cnst){
        //do nothing
    }
    
    public void visitProject(POProject proj){
        //do nothing
    }
    
    public void visitGreaterThan(GreaterThanExpr grt){
        //do nothing
    }
    
    public void visitLessThan(LessThanExpr lt){
        //do nothing
    }
    
    public void visitGTOrEqual(GTOrEqualToExpr gte){
        //do nothing
    }
    
    public void visiLTOrEqual(LTOrEqualToExpr lte){
        //do nothing
    }
    
    public void visitEqualTo(EqualToExpr eq){
        //do nothing
    }
    
    public void visitNotEqualTo(NotEqualToExpr eq){
        //do nothing
    }
    
    public void visitAdd(Add add) {
        //do nothing
    }
    
    public void visitSubtract(Subtract sub) {
        //do nothing
    }
    
    public void visitMultiply(Multiply mul) {
        //do nothing
    }
    
    public void visitDivide(Divide dv) {
        //do nothing
    }
    
    public void visitMod(Mod mod) {
        //do nothing
    }

}
