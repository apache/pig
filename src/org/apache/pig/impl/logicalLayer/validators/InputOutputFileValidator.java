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

import org.apache.pig.impl.PigContext ;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.plan.CompilationMessageCollector;
import org.apache.pig.impl.plan.CompilationMessageCollector.MessageType;
import org.apache.pig.impl.plan.DepthFirstWalker ;
import org.apache.pig.impl.plan.PlanValidationException;
import org.apache.pig.impl.plan.PlanValidator ;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.omg.CORBA.LocalObject;

/**
 * This validator does check
 * 1) Input files referred to by all LOLoads exist
 * 2) Output files referred to by all LOStores do not exist
 */
public class InputOutputFileValidator extends PlanValidator<LogicalOperator, LogicalPlan>{
    
    private PigContext pigCtx = null ;
    
    public InputOutputFileValidator(PigContext pigContext) {  
        pigCtx = pigContext ;
    }

    public void validate(LogicalPlan plan,
                         CompilationMessageCollector messageCollector) 
                                            throws PlanValidationException {
        
        super.validate(new InputOutputFileVisitor(plan, 
                                                  messageCollector, 
                                                  pigCtx),
                       messageCollector);
    
    }
    
    
    
}
