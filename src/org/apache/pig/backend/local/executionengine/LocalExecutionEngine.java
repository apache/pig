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
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.pig.FuncSpec;
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
import org.apache.pig.backend.executionengine.util.ExecTools;
import org.apache.pig.backend.hadoop.executionengine.HJob;
import org.apache.pig.builtin.BinStorage;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.LogToPhyTranslationVisitor;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PlanPrinter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.local.executionengine.physicalLayer.LocalLogToPhyTranslationVisitor;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.tools.pigstats.PigStats;

import java.util.Iterator;

public class LocalExecutionEngine implements ExecutionEngine {

    protected PigContext pigContext;
    protected DataStorage ds;
    protected NodeIdGenerator nodeIdGenerator;

    // key: the operator key from the logical plan that originated the physical
    // plan
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
        return this.pigContext.getProperties();
    }

    public void updateConfiguration(Properties newConfiguration)
            throws ExecException {
        // there is nothing to do here.
    }

    public Map<String, Object> getStatistics() throws ExecException {
        throw new UnsupportedOperationException();
    }

    // public PhysicalPlan compile(LogicalPlan plan,
    // Properties properties) throws ExecException {
    // if (plan == null) {
    // throw new ExecException("No Plan to compile");
    // }
    //
    // try {
    // LogToPhyTranslationVisitor translator =
    // new LogToPhyTranslationVisitor(plan);
    // translator.setPigContext(pigContext);
    // translator.visit();
    // return translator.getPhysicalPlan();
    // } catch (VisitorException ve) {
    // throw new ExecException(ve);
    // }
    // }

    public PhysicalPlan compile(LogicalPlan plan, Properties properties)
            throws ExecException {
        if (plan == null) {
            throw new ExecException("No Plan to compile");
        }

        try {
            LocalLogToPhyTranslationVisitor translator = new LocalLogToPhyTranslationVisitor(
                    plan);
            translator.setPigContext(pigContext);
            translator.visit();
            return translator.getPhysicalPlan();
        } catch (VisitorException ve) {
            throw new ExecException(ve);
        }
    }

    public List<ExecJob> execute(PhysicalPlan plan, String jobName)
        throws ExecException {
        try {
            PhysicalOperator leaf = (PhysicalOperator) plan.getLeaves().get(0);
            if (!(leaf instanceof POStore)) {
                String scope = leaf.getOperatorKey().getScope();
                POStore str = new POStore(new OperatorKey(scope,
                        NodeIdGenerator.getGenerator().getNextNodeId(scope)));
                FileSpec spec = new FileSpec(FileLocalizer.getTemporaryPath(null,pigContext).toString(), 
                                             new FuncSpec(BinStorage.class.getName()));
                str.setSFile(spec);
                plan.addAsLeaf(str);
            }

            LocalPigLauncher launcher = new LocalPigLauncher();

            List<ExecJob> jobs = new ArrayList<ExecJob>();
                    
            PigStats stats = launcher.launchPig(plan, jobName, pigContext);
            for (FileSpec fspec: launcher.getSucceededFiles()) {
                jobs.add(new LocalJob(ExecJob.JOB_STATUS.COMPLETED, pigContext, fspec, stats));
            }
            
            for (FileSpec fspec: launcher.getFailedFiles()) {
                jobs.add(new LocalJob(ExecJob.JOB_STATUS.FAILED, pigContext, fspec, stats));
            }

            launcher.reset();

            return jobs;

        } catch (Exception e) {
            // There are a lot of exceptions thrown by the launcher. If this
            // is an ExecException, just let it through. Else wrap it.
            if (e instanceof ExecException)
                throw (ExecException) e;
            else
                throw new ExecException(e.getMessage(), e);
        }
    }

    public List<ExecJob> submit(PhysicalPlan plan, String jobName)
            throws ExecException {
        throw new UnsupportedOperationException();
    }

    public void explain(PhysicalPlan plan, PrintStream stream, 
                        String format, boolean isVerbose) {
        try {
            ExecTools.checkLeafIsStore(plan, pigContext);

            LocalPigLauncher launcher = new LocalPigLauncher();
            launcher.explain(plan, pigContext, stream, 
                             format, isVerbose);
        } catch (Exception ve) {
            throw new RuntimeException(ve);
        }
    }

    public Collection<ExecJob> runningJobs(Properties properties)
            throws ExecException {
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
            Properties properties) throws ExecException {

        return null;
    }

}
