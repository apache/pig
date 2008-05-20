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

package org.apache.pig.backend.local.executionengine;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;
import java.util.Properties;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.pig.impl.PigContext;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.executionengine.ExecutionEngine;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.backend.executionengine.ExecJob.JOB_STATUS;
import org.apache.pig.backend.executionengine.ExecScopedLogicalOperator;
import org.apache.pig.backend.executionengine.ExecPhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.HJob;
import org.apache.pig.builtin.BinStorage;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.*;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.logicalLayer.parser.NodeIdGenerator;
import org.apache.pig.impl.mapReduceLayer.LocalLauncher;
import org.apache.pig.impl.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.physicalLayer.topLevelOperators.PhysicalOperator;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POStore;
import java.util.Iterator;


public class LocalExecutionEngine implements ExecutionEngine {

    protected PigContext pigContext;
    protected DataStorage ds;
    protected NodeIdGenerator nodeIdGenerator;

    // key: the operator key from the logical plan that originated the physical plan
    // val: the operator key for the root of the phyisical plan
    protected Map<OperatorKey, OperatorKey> logicalToPhysicalKeys;
    
    protected Map<OperatorKey, PhysicalOperator> physicalOpTable;
    
    // map from LOGICAL key to into about the execution
    protected Map<OperatorKey, LocalResult> materializedResults;
    
    public LocalExecutionEngine(PigContext pigContext) {
        this.pigContext = pigContext;
        this.ds = pigContext.getLfs();
        this.nodeIdGenerator = NodeIdGenerator.getGenerator(); 
        this.logicalToPhysicalKeys = new HashMap<OperatorKey, OperatorKey>();
        this.physicalOpTable = new HashMap<OperatorKey, PhysicalOperator>();
        this.materializedResults = new HashMap<OperatorKey, LocalResult>();
    }

    public DataStorage getDataStorage() {
        return this.ds;
    }
    
    public void init() throws ExecException {
        ;
    }

    public void close() throws ExecException {
        ;
    }
        
    public Properties getConfiguration() throws ExecException {
        Properties conf = new Properties();
        return conf;
    }
        
    public void updateConfiguration(Properties newConfiguration) 
        throws ExecException {
        ;
    }
        
    public Map<String, Object> getStatistics() throws ExecException {
        throw new UnsupportedOperationException();
    }

    
    public PhysicalPlan compile(LogicalPlan plan,
                                Properties properties) throws ExecException {
        if (plan == null) {
            throw new ExecException("No Plan to compile");
        }

        return compile(new LogicalPlan[]{ plan } , properties);
    }

    public PhysicalPlan compile(LogicalPlan[] plans,
                                Properties properties) throws ExecException {
        // TODO FIX Plug in logical to physical translator
        /*
        if (plans == null) {
            throw new ExecException("No Plans to compile");
        }

        OperatorKey physicalKey = null;
        for (int i = 0; i < plans.length; ++i) {
            LogicalPlan curPlan = null;

            curPlan = plans[ i ];
     
            OperatorKey logicalKey = curPlan.getRoot();
            
            physicalKey = logicalToPhysicalKeys.get(logicalKey);
            
            if (physicalKey == null) {
                physicalKey = doCompile(curPlan.getRoot(),
                                        curPlan.getOpTable(),
                                        properties);
                
                logicalToPhysicalKeys.put(logicalKey, physicalKey);
            }
        }
        
        return new LocalPhysicalPlan(physicalKey, physicalOpTable);
        */
        return null;
    }

    public ExecJob execute(PhysicalPlan plan,
                            String jobName) throws ExecException {
        try {
            PhysicalOperator leaf = (PhysicalOperator)plan.getLeaves().get(0);
            FileSpec spec = null;
            if(!(leaf instanceof POStore)){
                POStore str = new POStore(new OperatorKey("HExecEngine",
                    NodeIdGenerator.getGenerator().getNextNodeId("HExecEngine")));
                str.setPc(pigContext);
                spec = new FileSpec(FileLocalizer.getTemporaryPath(null,
                    pigContext).toString(),
                    BinStorage.class.getName());
                str.setSFile(spec);
            }
            else{
                spec = ((POStore)leaf).getSFile();
            }

            LocalLauncher launcher = new LocalLauncher();
            launcher.launchPig(plan, jobName, pigContext);
            return new HJob(ExecJob.JOB_STATUS.COMPLETED, pigContext, spec);
        } catch (Exception e) {
            // There are a lot of exceptions thrown by the launcher.  If this
            // is an ExecException, just let it through.  Else wrap it.
            if (e instanceof ExecException) throw (ExecException)e;
            else throw new ExecException(e.getMessage(), e);
        }


        // TODO Fix connect to local job runner
        /*
        DataBag results = BagFactory.getInstance().newDefaultBag();
        try {
            PhysicalOperator pp = (PhysicalOperator)physicalOpTable.get(plan.getRoot());

            pp.open();
            
            Tuple t;
            while ((t = (Tuple) pp.getNext()) != null) {
                results.add(t);
            }
            
            pp.close();
        }
        catch (IOException e) {
            throw new ExecException(e);
        }
        
        return new LocalJob(results, JOB_STATUS.COMPLETED);
        */
    }

    public LocalJob submit(PhysicalPlan plan,
                           String jobName) throws ExecException {
        throw new UnsupportedOperationException();
    }

    public void explain(PhysicalPlan plan, PrintStream stream) {
        // TODO FIX
    }

    public Collection<ExecJob> runningJobs(Properties properties) throws ExecException {
        return new HashSet<ExecJob>();
    }
    
    public Collection<String> activeScopes() throws ExecException {
        throw new UnsupportedOperationException();
    }
    
    public void reclaimScope(String scope) throws ExecException {
        throw new UnsupportedOperationException();
    }
    
    private OperatorKey doCompile(OperatorKey logicalKey,
                                  Map<OperatorKey, LogicalOperator> logicalOpTable,
                                  Properties properties) 
            throws ExecException {
        
        // TODO FIX
        /*
        LocalResult materializedResult = materializedResults.get(logicalKey);
        
        if (materializedResult != null) {
            ExecPhysicalOperator pp = new POLoad(logicalKey.getScope(),
                                             nodeIdGenerator.getNextNodeId(logicalKey.getScope()),
                                             physicalOpTable,
                                             pigContext, 
                                             materializedResult.outFileSpec,
                                             LogicalOperator.FIXED);
            
            OperatorKey ppKey = new OperatorKey(pp.getScope(), pp.getId());
            
            return ppKey;
        }

        OperatorKey physicalKey = new OperatorKey();
        
        if (compileOperator(logicalKey, logicalOpTable, properties, physicalKey)) {
            for (int i = 0; i < logicalOpTable.get(logicalKey).getInputs().size(); ++i) {
                ((PhysicalOperator)physicalOpTable.get(physicalKey)).inputs[i] = 
                    doCompile(logicalOpTable.get(logicalKey).getInputs().get(i), logicalOpTable, properties);
            }
        }

        return physicalKey;
        */
        return null;
    }
    
    private boolean compileOperator(OperatorKey logicalKey, 
                                    Map<OperatorKey, LogicalOperator> logicalOpTable,
                                    Properties properties,
                                    OperatorKey physicalKey) 
            throws ExecException {
                // TODO FIX
                /* 
        ExecPhysicalOperator pp;
        LogicalOperator lo = logicalOpTable.get(logicalKey);
        String scope = lo.getOperatorKey().getScope();
        boolean compileInputs = true;
        
        if (lo instanceof LOEval) {
            
            pp = new POEval(scope,
                           nodeIdGenerator.getNextNodeId(scope),
                           physicalOpTable,
                           ((LOEval) lo).getSpec(),
                           lo.getOutputType());
        } 
        else if (lo instanceof LOCogroup) {
            pp = new POCogroup(scope,
                               nodeIdGenerator.getNextNodeId(scope),
                               physicalOpTable,
                               ((LOCogroup) lo).getSpecs(),
                               lo.getOutputType());
        }  
        else if (lo instanceof LOLoad) {
            pp = new POLoad(scope,
                            nodeIdGenerator.getNextNodeId(scope),
                            physicalOpTable,
                            pigContext, 
                            ((LOLoad)lo).getInputFileSpec(),
                            lo.getOutputType());
        }
        else if (lo instanceof LOSplitOutput) {
            LOSplitOutput loso = (LOSplitOutput)lo;
            LOSplit los = ((LOSplit)(logicalOpTable.get(loso.getInputs().get(0))));
            
            pp = new POSplit(scope,
                             nodeIdGenerator.getNextNodeId(scope),
                             physicalOpTable,
                             doCompile(los.getInputs().get(0),
                                       logicalOpTable,
                                       properties), 
                             los.getConditions(),
                             loso.getReadFrom(),
                             lo.getOutputType());
            
            compileInputs = false;
        }
        else if (lo instanceof LOStore) {
            pp = new POStore(scope,
                             nodeIdGenerator.getNextNodeId(scope),
                             physicalOpTable,
                             lo.getInputs().get(0),
                             materializedResults,
                             ((LOStore)lo).getOutputFileSpec(),
                             ((LOStore)lo).isAppend(),
                             pigContext);
        } 
        else if (lo instanceof LOUnion) {
            pp = new POUnion(scope,
                             nodeIdGenerator.getNextNodeId(scope),
                             physicalOpTable,
                             ((LOUnion)lo).getInputs().size(),
                             lo.getOutputType());
        } 
        else if (lo instanceof LOSort) {
            pp = new POSort(scope,
                            nodeIdGenerator.getNextNodeId(scope),
                            physicalOpTable,
                            ((LOSort)lo).getSortSpec(),
                            lo.getOutputType());
        }
        else {
            throw new ExecException("Internal error: Unknown logical operator.");
        }
        
        physicalKey.scope = pp.getScope();
        physicalKey.id = pp.getId();
        
        logicalToPhysicalKeys.put(logicalKey, physicalKey);
        
        return compileInputs;
        */
            return false;
    }
}


