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

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.tez.runtime.api.LogicalOutput;

/**
 * This interface is implemented by PhysicalOperators that can have Tez outputs
 * attached directly to the operator.
 */

public interface TezOutput {

    public String[] getTezOutputs();

    public void replaceOutput(String oldOutputKey, String newOutputKey);

    public void attachOutputs(Map<String, LogicalOutput> outputs,
            Configuration conf) throws ExecException;

}
