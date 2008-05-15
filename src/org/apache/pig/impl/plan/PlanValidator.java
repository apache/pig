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
package org.apache.pig.impl.plan;

import org.apache.pig.impl.plan.CompilationMessageCollector.MessageType;

/***
 * Master of all plan validation classes.
 * @param <P>
 */
public abstract class PlanValidator<O extends Operator, P extends OperatorPlan<O>> {
   
    /***
     * If there are errors during validation, all of the errors have
     * to be collected in the supplied messageCollector. The exception should
     * be thrown only when the validation logic finds something too bad 
     * that other validation logics should not try to do more work.
     * 
     */
    public abstract void validate(P plan, CompilationMessageCollector messageCollector) 
                                                    throws PlanValidationException ;
    
    /**
     * This convenient method is used when: 
     * - if an exception being thrown from the current validation logic
     *   indicates that the whole validation pipeline should stop.
     * @param visitor
     * @param messageCollector
     * @throws PlanValidationException
     */
    protected void validate(PlanVisitor<O, P> visitor, 
                            CompilationMessageCollector messageCollector) 
                                             throws PlanValidationException {
        try {
            visitor.visit() ;
        } 
        catch(VisitorException ve) {
            messageCollector.collect("Unexpected exception in " 
                                      + this.getClass().getSimpleName(),
                                      MessageType.Error) ;
            throw new PlanValidationException("An unexpected exception caused " 
                                              + "the validation to stop", ve) ;
        }
    }
    
    /**
     * This convenient method is used when: 
     * - if an exception being thrown from the current validation logic
     *   indicates that the whole validation pipeline should keep going
     *   by continuing with the next validation logic in the pipeline
     *   (skip the rest of the current logic)
     * @param visitor
     * @param messageCollector
     * @throws PlanValidationException
     */
    protected void validateTolerateException(PlanVisitor<O, P> visitor, 
                            CompilationMessageCollector messageCollector) 
                                             throws PlanValidationException {
        try {
            visitor.visit() ;
        } 
        catch(VisitorException ve) {
            messageCollector.collect("Unexpected exception in " 
                                  + this.getClass().getSimpleName(),
                                  MessageType.Error) ;
        }
    }

    /**
     * This convenient method is used when: 
     * - if an exception being thrown from the current validation logic
     *   indicates that the whole validation pipeline should stop.
     *
     *   This method also assumes that the appropriate error message
     *   has already been recorded in the message collector so
     *   there is no need to duplicate the error message again here.
     *
     * @param visitor
     * @param messageCollector
     * @throws PlanValidationException
     */
    protected void validateSkipCollectException(PlanVisitor<O, P> visitor,
                            CompilationMessageCollector messageCollector)
                                             throws PlanValidationException {
        try {
            visitor.visit() ;
        }
        catch(VisitorException ve) {
            throw new PlanValidationException("An unexpected exception caused "
                                              + "the validation to stop", ve) ;
        }
    }

}
