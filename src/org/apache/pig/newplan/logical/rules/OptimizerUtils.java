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

import java.util.Iterator;

import org.apache.pig.builtin.Nondeterministic;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.UserFuncExpression;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LogicalPlan;

public class OptimizerUtils {
    /**
     * Find generate op from the foreach operator.
     * @param foreach the LOForEach instance
     * @return LOGenerate instance
     */
    public static LOGenerate findGenerate(LOForEach foreach) {
        LogicalPlan inner = foreach.getInnerPlan();
        return (LOGenerate)inner.getSinks().get(0);
    }

    /**
     * Check if a given LOGenerate operator has any flatten fields.
     * @param gen the given LOGenerate instance
     * @return true if LOGenerate instance contains flatten fields, false otherwise
     */
    public static boolean hasFlatten(LOGenerate gen) {
        boolean hasFlatten = false;
        boolean[] flattenFlags = gen.getFlattenFlags();
        if( flattenFlags != null ) {
            for( boolean flatten : flattenFlags ) {
                if( flatten ) {
                    hasFlatten = true;
                    break;
                }
            }
        }
        return hasFlatten;
    }

    /**
     * Helper method to find if a given LOForEach instance contains any flatten fields.
     * @param foreach foreach the LOForEach instance
     * @return true if LOForEach instance contains flatten fields, false otherwise
     */
    public static boolean hasFlatten(LOForEach foreach) {
        LOGenerate gen = findGenerate( foreach );
        return hasFlatten( gen );
    }

    /**
     * Helper method to determine if the logical expression plan for a Filter contains
     * non-deterministic operations and should therefore be treated extra carefully
     * during optimization.
     *
     * @param filterPlan
     * @return true of the filter plan contains a non-deterministic UDF
     * @throws FrontendException 
     */
    public static boolean planHasNonDeterministicUdf(LogicalExpressionPlan filterPlan)
    throws FrontendException {
        Iterator<Operator> it = filterPlan.getOperators();
        while( it.hasNext() ) {
            Operator op = it.next();
            if( op instanceof UserFuncExpression ) {
                if(! ((UserFuncExpression)op).isDeterministic() ){
                    return true;
                }
            }
        }
        return false;
    }
}
