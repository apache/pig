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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import static org.junit.Assert.*;

/**
 * Additional utility functions for new logical plan
 */
public class NewLogicalPlanUtil {
    
    /**
     * Find single relational operator in plan that is instance of class c
     * @param plan
     * @param c
     * @return
     */
    public static LogicalRelationalOperator getRelOpFromPlan(LogicalPlan plan, Class<?> c){
        List<LogicalRelationalOperator>ops = getRelOpsFromPlan(plan,c);
        if(ops.size() == 0)
            return null;
        else if(ops.size() > 1){
            fail("more than one operator of class " + c + " found in lp " + plan);
            return null;//never gets here
        }else 
            return ops.get(0);
        
    }

    /**
     * Find relational operators that are instance of class c
     * @param plan
     * @param c
     * @return
     */
    public static List<LogicalRelationalOperator> getRelOpsFromPlan(
            LogicalPlan plan, Class<?> c) {
        Iterator<Operator> it = plan.getOperators();
        ArrayList<LogicalRelationalOperator> ops = 
            new ArrayList<LogicalRelationalOperator>();
        
        while(it.hasNext()){
            Operator op = it.next();
            if(op.getClass() == c){
                ops.add((LogicalRelationalOperator) op);
            }
            if(op instanceof LOForEach){
                ops.addAll(getRelOpsFromPlan(((LOForEach)op).getInnerPlan(), c));
            }
        }
        return ops;
    }
    
}
