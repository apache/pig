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

package org.apache.pig.experimental.logical.relational;

import org.apache.pig.FuncSpec;
import org.apache.pig.experimental.plan.PlanVisitor;

public class LOLoad extends LogicalRelationalOperator {
    
    private LogicalSchema scriptSchema;

    /**
     * 
     * @param loader FuncSpec for load function to use for this load.
     * @param schema schema user specified in script, or null if not
     * specified.
     * @param plan logical plan this load is part of.
     */
    public LOLoad(FuncSpec loader, LogicalSchema schema, LogicalPlan plan) {
       super("LOLoad", plan);
       scriptSchema = schema;
    }
    
    /**
     * Get the schema for this load.  The schema will be either be what was
     * given by the user in the script or what the load functions getSchema
     * call returned.  Otherwise null will be returned, indicating that the
     * schema is unknown.
     * @return schema, or null if unknown
     */
    @Override
    public LogicalSchema getSchema() {
        if (schema != null) return schema;
        
        // TODO get schema from LoaderMetadata interface.
        LogicalSchema fromMetadata = getSchemaFromMetaData();
        
        if (scriptSchema != null && fromMetadata != null) {
            schema = LogicalSchema.merge(scriptSchema, fromMetadata);
            return schema;
        }
        
        if (scriptSchema != null)  schema = scriptSchema;
        else if (fromMetadata != null) schema = fromMetadata;
        return schema;
    }

    private LogicalSchema getSchemaFromMetaData() {
        return null;
    }

    @Override
    public void accept(PlanVisitor v) {
        if (!(v instanceof LogicalPlanVisitor)) {
            throw new RuntimeException("Expected LogicalPlanVisitor");
        }
        ((LogicalPlanVisitor)v).visitLOLoad(this);

    }

}
