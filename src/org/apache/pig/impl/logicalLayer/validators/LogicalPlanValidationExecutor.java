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

import java.util.List; 
import java.util.ArrayList; 

import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.plan.* ;
import org.apache.pig.impl.plan.CompilationMessageCollector.MessageType;
import org.apache.pig.impl.PigContext ;

/**
 * This class is responsible for all logical plan validations
 * after the parsing stage. 
 * All the validation logics should be added in the constructor.
 *
 */

public class LogicalPlanValidationExecutor 
                        implements PlanValidationExecutor<LogicalPlan> {
    
    private List<PlanValidator<LogicalOperator, LogicalPlan>> validatorList 
                            = new ArrayList<PlanValidator<LogicalOperator, LogicalPlan>>();
    
    /**
     * All the necessary validation logics can be plugged-in here.
     * Logics are executed from head to tail of the List.
     * 
     * In the PIG-111 Configuration patch, we can call 
     * pigContext.getProperties which holds current configuration
     * set. This allows us to let users enable/disable some validation
     * logics. The code will look like this:-
     * 
     * if ((Boolean)pigContext.getProperties().getProperty("pig.compiler.validationX") ) {
     *      validatorList.add(new ValidationLogicX(plan)) ;
     * }
     * 
     */
    
    public LogicalPlanValidationExecutor(LogicalPlan plan,
                                         PigContext pigContext) {
   
        // Default validations
        if (!pigContext.inExplain) {
            // When running explain we don't want to check for input
            // files.
            validatorList.add(new InputOutputFileValidator(pigContext)) ;
        }
        // This one has to be done before the type checker.
        //validatorList.add(new TypeCastInserterValidator()) ;
        validatorList.add(new TypeCheckingValidator()) ;
        
       
    }    

    public void validate(LogicalPlan plan,
            CompilationMessageCollector msgCollector) throws PlanValidationException {
        if (msgCollector == null) {
            throw new AssertionError(" messageCollector in " 
                         + "LogicalPlanValidationExecutor cannot be null") ;
        }
        
        try {
            for(PlanValidator<LogicalOperator, LogicalPlan> validator : validatorList) {
                validator.validate(plan, msgCollector) ;
            }
        } 
        catch(PlanValidationException pve) {            
            msgCollector.collect("Severe problem found during validation "
                                 + pve.toString(),
                                 MessageType.Error) ;            
            throw pve;
        }     
    }
}
