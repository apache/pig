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

import java.util.Map;
import java.util.Properties;
import java.io.OutputStream;
import java.io.Serializable;

import org.apache.pig.impl.plan.OperatorKey;

/**
 * 
 *
 */

public interface ExecPhysicalPlan extends Serializable {

    /*
     * As the fron end is not really supposed to do much with the physical representation
     * the handle to the physical plan can just be an GUId in the back-end state, which
     * brings up the problem is persistency...
     */
        
    /**
     * A job may have properties, like a priority, degree of parallelism...
     * Some of such properties may be inherited from the ExecutionEngine
     * configuration, other may have been set specifically for this job.
     * For instance, a job scheduler may attribute low priority to
     * jobs automatically started for maintenance purpose.
     * 
     * @return set of properties
     */
    public Properties getConfiguration();
        
    /**
     * Some properties of the job may be changed, say the priority...
     * 
     * @param configuration
     * @throws some changes may not be allowed, for instance the some
     * job-level properties cannot override Execution-Engine-level properties
     * or maybe some properties can only be changes only in certain states of the
     * job, say, once the job is started, parallelism level may not be changed...
     */
    public void updateConfiguration(Properties configuration)
        throws ExecException;
                
    /**    
     * To provide an "explanation" about how the physical plan has been constructed
     * 
     */
    public void explain(OutputStream out);
    
    public Map<OperatorKey, ExecPhysicalOperator> getOpTable();
    
    public OperatorKey getRoot();
}
