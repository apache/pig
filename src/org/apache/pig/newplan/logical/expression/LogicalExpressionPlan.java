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

package org.apache.pig.newplan.logical.expression;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.BaseOperatorPlan;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.optimizer.LogicalPlanPrinter;

/**
 * A plan containing LogicalExpressionOperators.
 */
public class LogicalExpressionPlan extends BaseOperatorPlan {
    
    @Override
    public boolean isEqual(OperatorPlan other) throws FrontendException {
        if (other != null && other instanceof LogicalExpressionPlan) {
            LogicalExpressionPlan otherPlan = (LogicalExpressionPlan)other;
            List<Operator> roots = getSources();
            List<Operator> otherRoots = otherPlan.getSources();
            if (roots.size() == 0 && otherRoots.size() == 0) return true;
            if (roots.size() > 1 || otherRoots.size() > 1) {
                throw new FrontendException("Found LogicalExpressionPlan with more than one root.  Unexpected.", 2224);
            }
            return roots.get(0).isEqual(otherRoots.get(0));            
        } else {
            return false;
        }
    }
    
    @Override
    public void explain(PrintStream ps, String format, boolean verbose) 
    throws FrontendException {
        ps.println("#-----------------------------------------------");
        ps.println("# New Logical Expression Plan:");
        ps.println("#-----------------------------------------------");

        LogicalPlanPrinter npp = new LogicalPlanPrinter(this, ps);
        npp.visit();
    }
    
    /**
     * Merge all nodes in lgExpPlan, keep the connections
     * @param lgExpPlan plan to merge
     * @return sources of the merged plan
     */
    public List<Operator> merge(LogicalExpressionPlan lgExpPlan) {
        
        List<Operator> sources = lgExpPlan.getSources();
        
        Iterator<Operator> iter = lgExpPlan.getOperators();
        while (iter.hasNext()) {
            LogicalExpression op = (LogicalExpression)iter.next();
            op.setPlan(this);
            add(op);
        }
        
        iter = lgExpPlan.getOperators();
        while (iter.hasNext()) {
            LogicalExpression startOp = (LogicalExpression)iter.next();
            ArrayList<Operator> endOps = (ArrayList<Operator>)lgExpPlan.fromEdges.get(startOp);
            if (endOps!=null) {
                for (Operator endOp : endOps) {
                        connect(startOp, endOp);
                }
            }
        }
        
        return sources;
    }

    public LogicalExpressionPlan deepCopy() throws FrontendException {
        LogicalExpressionPlan result = new LogicalExpressionPlan();
        LogicalExpression root = (LogicalExpression)getSources().get( 0 );
        LogicalExpression newRoot = root.deepCopy( result );
        result.add( newRoot );
        return result;
    }

}
