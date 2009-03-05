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

package org.apache.pig.test.utils.planComparer;

import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;

import java.util.Iterator;

/***
 * This class is used for LogicalPlan comparison based on:-
 * - Graph connectivity
 * - Schema of each operator
 *
 * Extend this class for adding extra comparison features.
 */
public class LogicalPlanComparer
                extends PlanStructuralComparer<LogicalOperator, LogicalPlan> {

    /***
     * This method does a structural comparison of two plans.
     *
     * @param plan1
     * @param plan2
     * @param messages
     * @return
     */
    @Override
    public boolean structurallyEquals(LogicalPlan plan1,
                                      LogicalPlan plan2,
                                      StringBuilder messages) {
        // Stage 1: Compare connectivity
        if (!super.structurallyEquals(plan1, plan2, messages)) {
            return false ;
        }

        // Stage 2: Compare node types
        if (findMismatchNodeType(plan1, plan2, messages) > 0) {
            return false ;
        }

        // Stage 3: Compare schemas
        if (findMismatchSchema(plan1, plan2, messages))  {
            return false ;
        }

        return true ;
    }

    /***
     * Compare schemas of the same logical operator on different plans
     *
     * @param plan1
     * @param plan2
     * @param messages
     * @return
     */
    private boolean findMismatchSchema(LogicalPlan plan1, LogicalPlan plan2, StringBuilder messages) {
        Iterator<OperatorKey> keyIter = plan1.getKeys().keySet().iterator() ;

        // for each Logical Operator pair, we compare schema
        while(keyIter.hasNext()) {

            OperatorKey key = keyIter.next() ;
            LogicalOperator op1 = plan1.getOperator(key) ;
            LogicalOperator op2 = plan2.getOperator(plan1ToPlan2.get(key)) ;

            Schema schema1 = null ;
            Schema schema2 = null ;

            try {
                schema1 = op1.getSchema() ;
                schema2 = op2.getSchema() ;
            }
            catch(FrontendException fe) {
                throw new RuntimeException("Cannot get schema from logical plan") ;
            }

            if (!Schema.equals(schema1, schema2, false, false)) {
                messages.append("Schema mismatch ") ;
                messages.append(op1.getClass().getSimpleName()) ;
                appendOpKey(op1.getOperatorKey(), messages) ;
                StringBuilder schemaStr1 = new StringBuilder() ;
                StringBuilder schemaStr2 = new StringBuilder() ;
                try {
                    Schema.stringifySchema(schemaStr1 ,schema1, DataType.BAG) ;
                    Schema.stringifySchema(schemaStr2 ,schema2, DataType.BAG) ;
                } catch (FrontendException fee) {
                    throw new RuntimeException("Cannot stringify schema") ;
                }
                messages.append(":") ;
                messages.append(schemaStr1.toString()) ;
                messages.append(" vs ") ;
                messages.append(schemaStr2.toString()) ;
                messages.append("\n") ;
                return true;
            }

        }
        return false;
    }


    /***
     * Find type mismatch between vertices in two plans
     * with the same key.
     * This method assumes the key sets from two plans are the same.
     * @param plan1
     * @param plan2
     * @param messages
     * @return
     */
    private int findMismatchNodeType(LogicalPlan plan1,
                                     LogicalPlan plan2,
                                     StringBuilder messages) {
        int diffCount = 0 ;

        Iterator<OperatorKey> keyIter1 = plan1.getKeys().keySet().iterator() ;

        // for each vertex, find diff for edges
        while(keyIter1.hasNext()) {
            OperatorKey opKey = keyIter1.next() ;
            LogicalOperator op1 = plan1.getOperator(opKey) ;
            LogicalOperator op2 = plan2.getOperator(plan1ToPlan2.get(opKey)) ;
            if (op1.getClass() != op2.getClass()) {
                if (messages != null) {
                    messages.append("Mismatch type:") ;
                    appendOpKey(opKey, messages) ;
                    messages.append(" ") ;
                    messages.append(op1.getClass().getSimpleName()) ;
                    messages.append(" vs ") ;
                    messages.append(op2.getClass().getSimpleName()) ;
                    messages.append("\n") ;
                }
                diffCount++ ;
            }
        }
        return diffCount ;
    }

}