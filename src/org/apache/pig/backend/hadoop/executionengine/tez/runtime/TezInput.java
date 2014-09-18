/**
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

package org.apache.pig.backend.hadoop.executionengine.tez.runtime;

import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.tez.runtime.api.LogicalInput;

/**
 * This interface is implemented by PhysicalOperators that can have Tez inputs
 * attached directly to the operator.
 */

public interface TezInput {

    public String[] getTezInputs();

    public void replaceInput(String oldInputKey, String newInputKey);

    /**
     * Add to the list of inputs to skip download if already available in vertex cache
     *
     * @param inputsToSkip
     */
    public void addInputsToSkip(Set<String> inputsToSkip);

    public void attachInputs(Map<String, LogicalInput> inputs,
            Configuration conf) throws ExecException;

}
