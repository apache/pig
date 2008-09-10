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

package org.apache.pig.impl.logicalLayer.optimizer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.LOCast;
import org.apache.pig.impl.logicalLayer.LOForEach;
import org.apache.pig.impl.logicalLayer.LOLoad;
import org.apache.pig.impl.logicalLayer.LOProject;
import org.apache.pig.impl.logicalLayer.LOStream;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.optimizer.OptimizerException;

/**
 * A visitor to discover if any schema has been specified for a file being
 * loaded.  If so, a projection will be injected into the plan to cast the
 * data being loaded to the appropriate types.  The optimizer can then come
 * along and move those casts as far down as possible, or in some cases remove
 * them altogether.  This visitor does not handle finding the schemas for the 
 * file, that has already been done as part of parsing.
 *
 */
public class TypeCastInserter extends LogicalTransformer {

    private String operatorClassName;

    public TypeCastInserter(LogicalPlan plan, String operatorClassName) {
        super(plan, new DepthFirstWalker<LogicalOperator, LogicalPlan>(plan));
        this.operatorClassName = operatorClassName;
    }

    @Override
    public boolean check(List<LogicalOperator> nodes) throws OptimizerException {
        try {
            Schema s = getOperator(nodes).getSchema();
            if (s == null) return false;
    
            boolean sawOne = false;
            List<Schema.FieldSchema> fss = s.getFields();
            List<Byte> types = new ArrayList<Byte>(s.size());
            for (Schema.FieldSchema fs : fss) {
                if (fs.type != DataType.BYTEARRAY) sawOne = true;
                types.add(fs.type);
            }

            // If all we've found are byte arrays, we don't need a projection.
            return sawOne;
        } catch (FrontendException fe) {
            throw new OptimizerException("Caught exception while trying to " +
                " check if type casts are needed", fe);
        }
    }
    
    private LogicalOperator getOperator(List<LogicalOperator> nodes) throws FrontendException {
        LogicalOperator lo = nodes.get(0);
        if(operatorClassName == LogicalOptimizer.LOLOAD_CLASSNAME) {
            if (lo == null || !(lo instanceof LOLoad)) {
                throw new RuntimeException("Expected load, got " +
                    lo.getClass().getName());
            }
    
            return lo;
        } else if(operatorClassName == LogicalOptimizer.LOSTREAM_CLASSNAME){
            if (lo == null || !(lo instanceof LOStream)) {
                throw new RuntimeException("Expected stream, got " +
                    lo.getClass().getName());
            }
    
            return lo;
        } else {
            // we should never be called with any other operator class name
            throw new FrontendException("TypeCastInserter invoked with an invalid operator class name:" + operatorClassName);
        }
   
    }

    @Override
    public void transform(List<LogicalOperator> nodes) throws OptimizerException {
        try {
            LogicalOperator lo = getOperator(nodes);
            Schema s = lo.getSchema();
            String scope = lo.getOperatorKey().scope;
            // For every field, build a logical plan.  If the field has a type
            // other than byte array, then the plan will be cast(project).  Else
            // it will just be project.
            ArrayList<LogicalPlan> genPlans = new ArrayList<LogicalPlan>(s.size());
            ArrayList<Boolean> flattens = new ArrayList<Boolean>(s.size());
            Map<String, Byte> typeChanges = new HashMap<String, Byte>();
            for (int i = 0; i < s.size(); i++) {
                LogicalPlan p = new LogicalPlan();
                genPlans.add(p);
                flattens.add(false);
                List<Integer> toProject = new ArrayList<Integer>(1);
                toProject.add(i);
                LOProject proj = new LOProject(p, OperatorKey.genOpKey(scope),
                    lo, toProject);
                p.add(proj);
                Schema.FieldSchema fs = s.getField(i);
                if (fs.type != DataType.BYTEARRAY) {
                    LOCast cast = new LOCast(p, OperatorKey.genOpKey(scope),
                        proj, fs.type);
                    p.add(cast);
                    p.connect(proj, cast);
                    
                    cast.setFieldSchema(fs.clone());
                    typeChanges.put(fs.canonicalName, fs.type);
                    // Reset the loads field schema to byte array so that it
                    // will reflect reality.
                    fs.type = DataType.BYTEARRAY;
                }
            }

            // Build a foreach to insert after the load, giving it a cast for each
            // position that has a type other than byte array.
            LOForEach foreach = new LOForEach(mPlan,
                OperatorKey.genOpKey(scope), genPlans, flattens);

            // Insert the foreach into the plan and patch up the plan.
            insertAfter(lo, foreach, null);

            rebuildSchemas();

        } catch (Exception e) {
            throw new OptimizerException(
                "Unable to insert type casts into plan", e);
        }
    }
}

 
