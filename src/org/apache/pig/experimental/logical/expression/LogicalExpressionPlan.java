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

package org.apache.pig.experimental.logical.expression;

import java.util.List;

import org.apache.pig.experimental.plan.BaseOperatorPlan;
import org.apache.pig.experimental.plan.Operator;
import org.apache.pig.experimental.plan.OperatorPlan;

/**
 * A plan containing LogicalExpressionOperators.
 */
public class LogicalExpressionPlan extends BaseOperatorPlan {
    
    @Override
    public boolean isEqual(OperatorPlan other) {
        if (other != null && other instanceof LogicalExpressionPlan) {
            LogicalExpressionPlan otherPlan = (LogicalExpressionPlan)other;
            List<Operator> roots = getSources();
            List<Operator> otherRoots = otherPlan.getSources();
            if (roots.size() == 0 && otherRoots.size() == 0) return true;
            if (roots.size() > 1 || otherRoots.size() > 1) {
                throw new RuntimeException("Found LogicalExpressionPlan with more than one root.  Unexpected.");
            }
            return roots.get(0).isEqual(otherRoots.get(0));            
        } else {
            return false;
        }
    }
}
