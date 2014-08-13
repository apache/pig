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

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Properties;

import org.apache.pig.backend.BackendException;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.streaming.ExecutableManager;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.ScriptState;

/**
 *
 * The main interface bridging the front end and back end of Pig. This allows Pig
 * to be ran on multiple Execution Engines, and not being limited to only Hadoop
 * MapReduce. The ExecutionEngines must support the following methods as these
 * are all the access points for the Pig frontend for processing. Traditionally
 * there is one ExecutionEngine created per processing job, but this is not necessary.
 * The ExecutionEngine instance comes from the ExecType, and it can choose to reuse
 * ExecutionEngine instances. All specifications for methods are listed below
 * as well as expected behavior, and the ExecutionEngine must conform to these.
 *
 */


@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface ExecutionEngine {

    /**
     * This method is responsible for the initialization of the ExecutionEngine.
     * All necessary setup tasks and configuration should execute in the init()
     * method. This method will be called via the PigContext object.
     */
    public void init() throws ExecException;

    /**
     * Responsible for updating the properties for the ExecutionEngine. The
     * update may require reinitialization of the engine, perhaps through
     * another call to init() if appropriate. This decision is delegated to the
     * ExecutionEngine -- that is, the caller will not call init() after
     * updating the properties.
     *
     * The Properties passed in should replace any configuration that occurred
     * from previous Properties object. The Properties object should also be
     * updated to reflect the deprecation/modifications that the ExecutionEngine
     * may trigger.
     *
     * @param newConfiguration -- Properties object holding all configuration vals
     */
    public void setConfiguration(Properties newConfiguration)
            throws ExecException;

    /**
     * Responsible for setting a specific property and value. This method may be
     * called as a result of a user "SET" command in the script or elsewhere in
     * Pig to set certain properties.
     *
     * The properties object of the PigContext should be updated with the
     * property and value with deprecation/other configuration done by the
     * ExecutionEngine reflected. The ExecutionEngine should also update its
     * internal configuration view as well.
     *
     * @param property to update
     * @param value to set for property
     */
    public void setProperty(String property, String value);

    /**
     * Returns the Properties representation of the ExecutionEngine
     * configuration. The Properties object returned does not have to be the
     * same object between distinct calls to getConfiguration(). The ExecutionEngine
     * may create a new Properties object populated with all the properties
     * each time.
     */
    public Properties getConfiguration();

    /**
     * This method is responsible for the actual execution of a LogicalPlan. No
     * assumptions is made about the architecture of the ExecutionEngine, except
     * that it is capable of executing the LogicalPlan representation of a
     * script. The ExecutionEngine should take care of all cleanup after
     * executing the logical plan and returns an implementation of PigStats that
     * contains the relevant information/statistics of the execution of the
     * script.
     *
     * @param lp -- plan to compile
     * @param grpName -- group name for submission
     * @param pc -- context for execution
     * @throws ExecException
     */
    public PigStats launchPig(LogicalPlan lp, String grpName, PigContext pc)
            throws FrontendException, ExecException;

    /**
     * This method handles the backend processing of the Explain command. Once
     * again, no assumptions is made about the architecture of the
     * ExecutionEngine, except that it is capable of "explaining" the
     * LogicalPlan representation of a script. The ExecutionEngine should print
     * all of it's explain statements in the PrintStream provided UNLESS the
     * File object is NOT null. In that case, the file is the directory for
     * which the ExecutionEngine must write out the explain statements into
     * semantically distinct files. For example, if the ExecutionEngine operates
     * on a PhysicalPlan and an ExecutionPlan then there would be two separate
     * files detailing each. The suffix param indicates the suffix of each of
     * these file names.
     *
     * @param lp -- plan to explain
     * @param pc -- context for explain processing
     * @param ps -- print stream to write all output to (if dir param is null)
     * @param format -- format to print explain
     * @param verbose
     * @param dir -- directory to write output to. if not null, write to files
     * @param suffix -- if writing to files, suffix to be used for each file
     *
     *
     * @throws PlanException
     * @throws VisitorException
     * @throws IOException
     */
    public void explain(LogicalPlan lp, PigContext pc, PrintStream ps,
            String format, boolean verbose, File dir, String suffix)
            throws PlanException, VisitorException, IOException;

    /**
     * Returns the DataStorage the ExecutionEngine is using.
     *
     * @return DataStorage the ExecutionEngine is using.
     */
    public DataStorage getDataStorage();

    /**
     * Creates a ScriptState object which will be accessible as a ThreadLocal
     * variable inside the ScriptState class. This method is called when first
     * initializing the ScriptState as to delegate to the ExecutionEngine the
     * version of ScriptState to use to manage the execution at hand.
     *
     * @return ScriptState object to manage execution of the script
     */
    public ScriptState instantiateScriptState();

    /**
     * Creates a PigStats object which will be accessible as a ThreadLocal
     * variable inside the PigStats class. This method is called when first
     * initializing the PigStats.
     *
     * @return PigStats object.
     */
    public PigStats instantiatePigStats();

    /**
     * Returns the ExecutableManager to be used in Pig Streaming.
     *
     * @return ExecutableManager to be used in Pig Streaming.
     */
    public ExecutableManager getExecutableManager();

    /**
     * This method is called when a user requests to kill a job associated with
     * the given job id. If it is not possible for a user to kill a job, throw a
     * exception. It is imperative for the job id's being displayed to be unique
     * such that the correct jobs are being killed when the user supplies the
     * id.
     *
     * @throws BackendException
     */
    public void killJob(String jobID) throws BackendException;

}
