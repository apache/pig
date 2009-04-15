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
import org.apache.pig.FuncSpec;
import org.apache.pig.PigException;
import org.apache.pig.impl.streaming.StreamingCommand;
import org.apache.pig.impl.streaming.StreamingCommand.HandleSpec;

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
            LogicalOperator op = getOperator(nodes);
            Schema s = op.getSchema();
            if (s == null) return false;
    
            boolean sawOne = false;
            List<Schema.FieldSchema> fss = s.getFields();
            List<Byte> types = new ArrayList<Byte>(s.size());
            Schema determinedSchema = null;
            if(LOLoad.class.getName().equals(operatorClassName)) {
                determinedSchema = ((LOLoad)op).getDeterminedSchema();
            }
            for (int i = 0; i < fss.size(); i++) {
                if (fss.get(i).type != DataType.BYTEARRAY) {
                    if(determinedSchema == null || 
                            (fss.get(i).type != determinedSchema.getField(i).type)) {
                            // Either no schema was determined by loader OR the type 
                            // from the "determinedSchema" is different
                            // from the type specified - so we need to cast
                            sawOne = true;
                        }
                }
                types.add(fss.get(i).type);
            }

            // If all we've found are byte arrays, we don't need a projection.
            return sawOne;
        } catch(OptimizerException oe) {
            throw oe;
        } catch (Exception e) {
            int errCode = 2004;
            String msg = "Internal error while trying to check if type casts are needed";
            throw new OptimizerException(msg, errCode, PigException.BUG, e);
        }
    }
    
    private LogicalOperator getOperator(List<LogicalOperator> nodes) throws FrontendException {
        if((nodes == null) || (nodes.size() <= 0)) {
            int errCode = 2052;
            String msg = "Internal error. Cannot retrieve operator from null or empty list.";
            throw new OptimizerException(msg, errCode, PigException.BUG);
        }
        
        LogicalOperator lo = nodes.get(0);
        if(LOLoad.class.getName().equals(operatorClassName)) {
            if (lo == null || !(lo instanceof LOLoad)) {
                int errCode = 2005;
                String msg = "Expected " + LOLoad.class.getSimpleName() + ", got " + lo.getClass().getSimpleName();
                throw new OptimizerException(msg, errCode, PigException.BUG);
            }
    
            return lo;
        } else if(LOStream.class.getName().equals(operatorClassName)){
            if (lo == null || !(lo instanceof LOStream)) {
                int errCode = 2005;
                String msg = "Expected " + LOStream.class.getSimpleName() + ", got " + lo.getClass().getSimpleName();
                throw new OptimizerException(msg, errCode, PigException.BUG);
            }
    
            return lo;
        } else {
            // we should never be called with any other operator class name
            int errCode = 1034;
            String msg = "TypeCastInserter invoked with an invalid operator class name:" + operatorClassName;
            throw new OptimizerException(msg, errCode, PigException.INPUT);
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
            // if we are inserting casts in a load and if the loader
            // implements determineSchema(), insert casts only where necessary
            // Note that in this case, the data coming out of the loader is not
            // a BYTEARRAY but is whatever determineSchema() says it is.
            Schema determinedSchema = null;
            if(LOLoad.class.getName().equals(operatorClassName)) {
                determinedSchema = ((LOLoad)lo).getDeterminedSchema();
            }
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
                    if(determinedSchema == null || (fs.type != determinedSchema.getField(i).type)) {
                            // Either no schema was determined by loader OR the type 
                            // from the "determinedSchema" is different
                            // from the type specified - so we need to cast
                            LOCast cast = new LOCast(p, 
                                        OperatorKey.genOpKey(scope), fs.type);
                            p.add(cast);
                            p.connect(proj, cast);
                            
                            cast.setFieldSchema(fs.clone());
                            FuncSpec loadFuncSpec = null;
                            if(lo instanceof LOLoad) {
                                loadFuncSpec = ((LOLoad)lo).getInputFile().getFuncSpec();
                            } else if (lo instanceof LOStream) {
                                StreamingCommand command = ((LOStream)lo).getStreamingCommand();
                                HandleSpec streamOutputSpec = command.getOutputSpec(); 
                                loadFuncSpec = new FuncSpec(streamOutputSpec.getSpec());
                            } else {
                                int errCode = 2006;
                                String msg = "TypeCastInserter invoked with an invalid operator class name: " + lo.getClass().getSimpleName();
                                throw new OptimizerException(msg, errCode, PigException.BUG);
                            }
                            cast.setLoadFuncSpec(loadFuncSpec);
                            typeChanges.put(fs.canonicalName, fs.type);
                            if(determinedSchema == null) {
                                // Reset the loads field schema to byte array so that it
                                // will reflect reality.
                                fs.type = DataType.BYTEARRAY;
                            } else {
                                // Reset the type to what determinedSchema says it is
                                fs.type = determinedSchema.getField(i).type;
                            }
                        }
                }
            }

            // Build a foreach to insert after the load, giving it a cast for each
            // position that has a type other than byte array.
            LOForEach foreach = new LOForEach(mPlan,
                OperatorKey.genOpKey(scope), genPlans, flattens);
            foreach.setAlias(lo.getAlias());
            // Insert the foreach into the plan and patch up the plan.
            insertAfter(lo, foreach, null);

            rebuildSchemas();

        } catch (OptimizerException oe) {
            throw oe;
        } catch (Exception e) {
            int errCode = 2007;
            String msg = "Unable to insert type casts into plan"; 
            throw new OptimizerException(msg, errCode, PigException.BUG, e);
        }
    }
}

 
