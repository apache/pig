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

package org.apache.pig.backend.executionengine;

import java.io.OutputStream;
import java.io.Serializable;
import java.util.Map;

import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.OperatorKey;

/**
 * A logical plan has a root of the operator tree. 
 * A table of operators collects the set of operators making up a logical tree.
 * It is possible to navigate a logical tree by looking up the input operator 
 * keys of the logical operator in the operator table.
 *
 */
public interface ExecLogicalPlan extends  Serializable {
    
    public OperatorKey getRoot();
    
    public Map<OperatorKey, LogicalOperator> getOpTable();
    
    public void explain(OutputStream out);
}
