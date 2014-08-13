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

import org.apache.pig.backend.hadoop.executionengine.HExecutionEngine;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;
import org.apache.pig.impl.logicalLayer.FrontendException;

/**
 * 
 * Implementors of this interface would define validations based on logical
 * operators within a Pig script. The validations could be called from
 * {@link HExecutionEngine#compile(org.apache.pig.newplan.logical.relational.LogicalPlan, java.util.Properties)}
 * 
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface LogicalRelationalNodeValidator {

    /**
     * Validates logical operators as defined in the logical plan of a pig
     * script.
     * 
     * @throws FrontendException
     */
    public void validate() throws FrontendException;
}
