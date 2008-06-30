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

package org.apache.pig.impl.logicalLayer.validators;

import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.plan.CompilationMessageCollector;
import org.apache.pig.impl.plan.CompilationMessageCollector.MessageType;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.PlanValidationException;
import org.apache.pig.impl.plan.PlanValidator;
import org.apache.pig.impl.plan.VisitorException;

public class TypeCheckingValidator extends PlanValidator<LogicalOperator, LogicalPlan> {
    
    public TypeCheckingValidator() {   
    }
    
    public void validate(LogicalPlan plan,
                         CompilationMessageCollector msgCollector) 
                                        throws PlanValidationException {
        // The first msgCollector is used in visitor
        // The second msgCollector is used by the validator
        super.validateSkipCollectException(new TypeCheckingVisitor(plan,  msgCollector),
                                           msgCollector) ;

    }
}
