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
package org.apache.pig.newplan.logical.relational;

import java.io.IOException;

import org.apache.pig.impl.streaming.ExecutableManager;
import org.apache.pig.impl.streaming.StreamingCommand;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.PlanVisitor;

public class LOStream extends LogicalRelationalOperator {

    private static final long serialVersionUID = 2L;
    //private static Log log = LogFactory.getLog(LOFilter.class);
    
    // the StreamingCommand object for the
    // Stream Operator this operator represents
    private StreamingCommand command;
    transient private ExecutableManager executableManager;
        
    public LOStream(LogicalPlan plan, ExecutableManager exeManager, StreamingCommand cmd) {
        super("LODistinct", plan);
        command = cmd;
        executableManager = exeManager;
    }
    
    /**
     * Get the StreamingCommand object associated
     * with this operator
     * 
     * @return the StreamingCommand object
     */
    public StreamingCommand getStreamingCommand() {
        return command;
    }
    
    /**
     * @return the ExecutableManager
     */
    public ExecutableManager getExecutableManager() {
        return executableManager;
    }

    @Override
    public LogicalSchema getSchema() {
        if (schema!=null)
            return schema;
        LogicalRelationalOperator input = null;
        try {
            input = (LogicalRelationalOperator)plan.getPredecessors(this).get(0);
        }catch(Exception e) {
            throw new RuntimeException("Unable to get predecessor of LOStream.", e);
        }
        
        schema = input.getSchema();
        return schema;
    }   
    
    @Override
    public void accept(PlanVisitor v) throws IOException {
        if (!(v instanceof LogicalRelationalNodesVisitor)) {
            throw new IOException("Expected LogicalPlanVisitor");
        }
        ((LogicalRelationalNodesVisitor)v).visit(this);
    }
    
    @Override
    public boolean isEqual(Operator other) {
        if (other != null && other instanceof LOStream) { 
            return checkEquality((LogicalRelationalOperator)other);
        } else {
            return false;
        }
    }

}
