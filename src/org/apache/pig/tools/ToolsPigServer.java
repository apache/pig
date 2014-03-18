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
package org.apache.pig.tools;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.tools.grunt.GruntParser;
import org.apache.pig.tools.pigstats.PigStats;

/**
 * ToolsPigServer is a subclass of PigServer intended only for Pig tools.  Users
 * should not use this interface, as we make no promises about its stability or
 * continued existence.
 */
@InterfaceAudience.LimitedPrivate({"Penny"})
@InterfaceStability.Unstable
public class ToolsPigServer extends PigServer {

    private PigPlans plans = null;

    /**
     * @param execTypeString can be 'mapreduce' or 'local'.  Local mode will 
     * use Hadoop's local job runner to execute the job on the local machine.
     * Mapreduce mode will connect to a cluster to execute the job.
     * @throws ExecException if throws by PigServer
     * @throws IOException if throws by PigServer
     */
    public ToolsPigServer(String execTypeString) throws ExecException, IOException {
        super(execTypeString);
    }

    /**
     * @param ctx the context to use to construct the PigServer
     * @throws ExecException if throws by PigServer
     * @throws IOException if throws by PigServer
     */
    public ToolsPigServer(PigContext ctx) throws ExecException, IOException {
        super(ctx);
    }

    /**
     * @param execType execution type to start the engine in.
     * @param properties to use for this run
     * @throws ExecException if throws by PigServer
     */
    public ToolsPigServer(ExecType execType, Properties properties) throws ExecException {
        super(execType, properties);
    }

    /**
     * Register a script without running it.  This method is not compatible with
     * {@link #registerQuery(String)}, {@link #registerScript(String)}, 
     * {@link #store(String, String)} or {@link #openIterator(String)}.  It can be
     * used with {@link #getPlans()} and {@link #runPlan(LogicalPlan, String)} in this class 
     * only. The proper control flow is for the caller to call registerNoRun() and then
     * getPlans() to get a copy of the plans.  The user can then modify the
     * logical plan.  It can then be returned via runPlan(), which will execute
     * the plan.
     * @param fileName File containing Pig Latin script to register.
     * @param params  the key is the parameter name, and the value is the parameter value
     * @param paramFiles   files which have the parameter setting
     * @throws IOException if it encounters problems reading the script
     * @throws FrontendException if it encounters problems parsing the script
     */
    public void registerNoRun(String fileName,
                              Map<String, String> params,
                              List<String> paramFiles) throws IOException, FrontendException {

        // Do parameter substitution
        String substituted = null;
        FileInputStream fis = null;
        try{
            fis = new FileInputStream(fileName);
            substituted = pigContext.doParamSubstitution(fis, paramMapToList(params), paramFiles);
        }catch (FileNotFoundException e){
            log.error(e.getLocalizedMessage());
            throw new IOException(e);
        } finally {
            if (fis != null) {
                fis.close();
            }
        }

        // Parse in grunt so that register commands are recognized
        try {
            GruntParser grunt = new GruntParser(new StringReader(substituted), this);
            grunt.setInteractive(false);
            setBatchOn();
            //grunt.setLoadOnly(true);
            grunt.parseOnly();
        } catch (org.apache.pig.tools.pigscript.parser.ParseException e) {
            log.error(e.getLocalizedMessage());
            throw new IOException(e);
        }

        Graph g = getClonedGraph();
        LogicalPlan lp = g.getPlan(null);
        plans = new PigPlans(lp);
    }

    /**
     * Get a class containing the Pig plans.  For now it just contains
     * the new logical plan.  At some point in the future it should contain
     * the MR plan as well.
     * @return the pig plans.
     */
    public PigPlans getPlans() {
        return plans;
    }

    /**
     * Given a (modified) new logical plan, run the script.
     * @param newPlan plan to run
     * @param jobName name to give the MR jobs associated with this run
     * @return list of exec jobs describing the jobs that were run.
     * @throws FrontendException if plan translation fails.
     * @throws ExecException if running the job fails.
     */
    public List<ExecJob> runPlan(LogicalPlan newPlan,
                                 String jobName) throws FrontendException, ExecException {
        PigStats stats = launchPlan(newPlan, jobName);
        return getJobs(stats);
    }

    public static class PigPlans {

        public LogicalPlan lp;

        public PigPlans(LogicalPlan lp) {
            this.lp = lp;
        }
    };
}




