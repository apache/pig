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

package org.apache.pig.backend.hadoop.executionengine;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketImplFactory;
import java.net.URL;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.pig.PigConstants;
import org.apache.pig.PigException;
import org.apache.pig.backend.BackendException;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecutionEngine;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.datastorage.HDataStorage;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceLauncher;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil;
import org.apache.pig.backend.hadoop.streaming.HadoopExecutableManager;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.PigImplConstants;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.streaming.ExecutableManager;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.optimizer.LogicalPlanOptimizer;
import org.apache.pig.newplan.logical.optimizer.SchemaResetter;
import org.apache.pig.newplan.logical.optimizer.UidResetter;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LogToPhyTranslationVisitor;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.rules.InputOutputFileValidator;
import org.apache.pig.newplan.logical.visitor.SortInfoSetter;
import org.apache.pig.newplan.logical.visitor.StoreAliasSetter;
import org.apache.pig.pen.POOptimizeDisabler;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.ScriptState;
import org.apache.pig.tools.pigstats.mapreduce.MRScriptState;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

public class MRExecutionEngine implements ExecutionEngine {

    public static final String JOB_TRACKER_LOCATION = "mapred.job.tracker";
    private static final String FILE_SYSTEM_LOCATION = "fs.default.name";
    private static final String ALTERNATIVE_FILE_SYSTEM_LOCATION = "fs.defaultFS";

    private static final String HADOOP_SITE = "hadoop-site.xml";
    private static final String CORE_SITE = "core-site.xml";
    private static final String YARN_SITE = "yarn-site.xml";
    private final Log log = LogFactory.getLog(getClass());
    public static final String LOCAL = "local";

    protected PigContext pigContext;

    protected DataStorage ds;

    @SuppressWarnings("deprecation")
    protected JobConf jobConf;

    // key: the operator key from the logical plan that originated the physical
    // plan
    // val: the operator key for the root of the phyisical plan
    protected Map<OperatorKey, OperatorKey> logicalToPhysicalKeys;

    protected Map<Operator, PhysicalOperator> newLogToPhyMap;
    private LogicalPlan newPreoptimizedPlan;

    protected Launcher launcher;

    public MRExecutionEngine(PigContext pigContext) {
        this.pigContext = pigContext;
        this.logicalToPhysicalKeys = new HashMap<OperatorKey, OperatorKey>();

        this.ds = null;

        // to be set in the init method
        this.jobConf = null;

        this.launcher = new MapReduceLauncher();
    }

    @SuppressWarnings("deprecation")
    public JobConf getJobConf() {
        return this.jobConf;
    }

    @Override
    public DataStorage getDataStorage() {
        return this.ds;
    }

    @Override
    public void init() throws ExecException {
        init(this.pigContext.getProperties());
    }

    @SuppressWarnings({ "deprecation", "resource" })
    private void init(Properties properties) throws ExecException {
        // First set the ssh socket factory
        setSSHFactory();

        String cluster = null;
        String nameNode = null;

        // We need to build a configuration object first in the manner described
        // below
        // and then get back a properties object to inspect the
        // JOB_TRACKER_LOCATION
        // and FILE_SYSTEM_LOCATION. The reason to do this is if we looked only
        // at
        // the existing properties object, we may not get the right settings. So
        // we want
        // to read the configurations in the order specified below and only then
        // look
        // for JOB_TRACKER_LOCATION and FILE_SYSTEM_LOCATION.

        // Hadoop by default specifies two resources, loaded in-order from the
        // classpath:
        // 1. hadoop-default.xml : Read-only defaults for hadoop.
        // 2. hadoop-site.xml: Site-specific configuration for a given hadoop
        // installation.
        // Now add the settings from "properties" object to override any
        // existing properties
        // All of the above is accomplished in the method call below

        JobConf jc = null;
        if (!this.pigContext.getExecType().isLocal()) {
            // Check existence of user provided configs
            String isHadoopConfigsOverriden = properties
                    .getProperty("pig.use.overriden.hadoop.configs");
            if (isHadoopConfigsOverriden != null
                    && isHadoopConfigsOverriden.equals("true")) {
                jc = new JobConf(ConfigurationUtil.toConfiguration(properties));
            } else {
                // Check existence of hadoop-site.xml or core-site.xml in
                // classpath
                // if user provided confs are not being used
                Configuration testConf = new Configuration();
                ClassLoader cl = testConf.getClassLoader();
                URL hadoop_site = cl.getResource(HADOOP_SITE);
                URL core_site = cl.getResource(CORE_SITE);

                if (hadoop_site == null && core_site == null) {
                    throw new ExecException(
                            "Cannot find hadoop configurations in classpath (neither hadoop-site.xml nor core-site.xml was found in the classpath)."
                                    + " If you plan to use local mode, please put -x local option in command line",
                            4010);
                }
                jc = new JobConf();
            }
            jc.addResource("pig-cluster-hadoop-site.xml");
            jc.addResource(YARN_SITE);

            // Trick to invoke static initializer of DistributedFileSystem to
            // add hdfs-default.xml
            // into configuration
            new DistributedFileSystem();

            // the method below alters the properties object by overriding the
            // hadoop properties with the values from properties and recomputing
            // the properties
            recomputeProperties(jc, properties);
        } else {
            // If we are running in local mode we dont read the hadoop conf file
            if (properties.getProperty("mapreduce.framework.name") == null) {
                properties.setProperty("mapreduce.framework.name", "local");
            }
            properties.setProperty(JOB_TRACKER_LOCATION, LOCAL);
            properties.setProperty(FILE_SYSTEM_LOCATION, "file:///");
            properties
                    .setProperty(ALTERNATIVE_FILE_SYSTEM_LOCATION, "file:///");

            jc = new JobConf(false);
            jc.addResource("core-default.xml");
            jc.addResource("mapred-default.xml");
            jc.addResource("yarn-default.xml");
            recomputeProperties(jc, properties);
        }

        cluster = jc.get(JOB_TRACKER_LOCATION);
        nameNode = jc.get(FILE_SYSTEM_LOCATION);
        if (nameNode == null)
            nameNode = (String) pigContext.getProperties().get(
                    ALTERNATIVE_FILE_SYSTEM_LOCATION);

        if (cluster != null && cluster.length() > 0) {
            if (!cluster.contains(":") && !cluster.equalsIgnoreCase(LOCAL)) {
                cluster = cluster + ":50020";
            }
            properties.setProperty(JOB_TRACKER_LOCATION, cluster);
        }

        if (nameNode != null && nameNode.length() > 0) {
            if (!nameNode.contains(":") && !nameNode.equalsIgnoreCase(LOCAL)) {
                nameNode = nameNode + ":8020";
            }
            properties.setProperty(FILE_SYSTEM_LOCATION, nameNode);
        }

        log.info("Connecting to hadoop file system at: "
                + (nameNode == null ? LOCAL : nameNode));
        // constructor sets DEFAULT_REPLICATION_FACTOR_KEY
        ds = new HDataStorage(properties);

        if (cluster != null && !cluster.equalsIgnoreCase(LOCAL)) {
            log.info("Connecting to map-reduce job tracker at: "
                    + jc.get(JOB_TRACKER_LOCATION));
        }

        // Set job-specific configuration knobs
        jobConf = jc;
    }

    @SuppressWarnings("unchecked")
    public PhysicalPlan compile(LogicalPlan plan, Properties properties)
            throws FrontendException {
        if (plan == null) {
            int errCode = 2041;
            String msg = "No Plan to compile";
            throw new FrontendException(msg, errCode, PigException.BUG);
        }

        newPreoptimizedPlan = new LogicalPlan(plan);

        if (pigContext.inIllustrator) {
            // disable all PO-specific optimizations
            POOptimizeDisabler pod = new POOptimizeDisabler(plan);
            pod.visit();
        }

        UidResetter uidResetter = new UidResetter(plan);
        uidResetter.visit();

        SchemaResetter schemaResetter = new SchemaResetter(plan, true /*
                                                                     * skip
                                                                     * duplicate
                                                                     * uid check
                                                                     */);
        schemaResetter.visit();

        HashSet<String> disabledOptimizerRules;
        try {
            disabledOptimizerRules = (HashSet<String>) ObjectSerializer
                    .deserialize(pigContext.getProperties().getProperty(
                            PigImplConstants.PIG_OPTIMIZER_RULES_KEY));
        } catch (IOException ioe) {
            int errCode = 2110;
            String msg = "Unable to deserialize optimizer rules.";
            throw new FrontendException(msg, errCode, PigException.BUG, ioe);
        }
        if (disabledOptimizerRules == null) {
            disabledOptimizerRules = new HashSet<String>();
        }

        String pigOptimizerRulesDisabled = this.pigContext.getProperties()
                .getProperty(PigConstants.PIG_OPTIMIZER_RULES_DISABLED_KEY);
        if (pigOptimizerRulesDisabled != null) {
            disabledOptimizerRules.addAll(Lists.newArrayList((Splitter.on(",")
                    .split(pigOptimizerRulesDisabled))));
        }

        if (pigContext.inIllustrator) {
            disabledOptimizerRules.add("MergeForEach");
            disabledOptimizerRules.add("PartitionFilterOptimizer");
            disabledOptimizerRules.add("LimitOptimizer");
            disabledOptimizerRules.add("SplitFilter");
            disabledOptimizerRules.add("PushUpFilter");
            disabledOptimizerRules.add("MergeFilter");
            disabledOptimizerRules.add("PushDownForEachFlatten");
            disabledOptimizerRules.add("ColumnMapKeyPrune");
            disabledOptimizerRules.add("AddForEach");
            disabledOptimizerRules.add("GroupByConstParallelSetter");
        }

        StoreAliasSetter storeAliasSetter = new StoreAliasSetter(plan);
        storeAliasSetter.visit();

        // run optimizer
        LogicalPlanOptimizer optimizer = new LogicalPlanOptimizer(plan, 100,
                disabledOptimizerRules);
        optimizer.optimize();

        // compute whether output data is sorted or not
        SortInfoSetter sortInfoSetter = new SortInfoSetter(plan);
        sortInfoSetter.visit();

        if (!pigContext.inExplain) {
            // Validate input/output file. Currently no validation framework in
            // new logical plan, put this validator here first.
            // We might decide to move it out to a validator framework in future
            InputOutputFileValidator validator = new InputOutputFileValidator(
                    plan, pigContext);
            validator.validate();
        }

        // translate new logical plan to physical plan
        LogToPhyTranslationVisitor translator = new LogToPhyTranslationVisitor(
                plan);

        translator.setPigContext(pigContext);
        translator.visit();
        newLogToPhyMap = translator.getLogToPhyMap();
        return translator.getPhysicalPlan();
    }

    public Map<Operator, PhysicalOperator> getLogToPhyMap() {
        return newLogToPhyMap;
    }

    public Map<LOForEach, Map<LogicalRelationalOperator, PhysicalOperator>> getForEachInnerLogToPhyMap(
            LogicalPlan plan) {
        Map<LOForEach, Map<LogicalRelationalOperator, PhysicalOperator>> result = new HashMap<LOForEach, Map<LogicalRelationalOperator, PhysicalOperator>>();
        Iterator<Operator> outerIter = plan.getOperators();
        while (outerIter.hasNext()) {
            Operator oper = outerIter.next();
            if (oper instanceof LOForEach) {
                LogicalPlan innerPlan = ((LOForEach) oper).getInnerPlan();
                Map<LogicalRelationalOperator, PhysicalOperator> innerOpMap = new HashMap<LogicalRelationalOperator, PhysicalOperator>();
                Iterator<Operator> innerIter = innerPlan.getOperators();
                while (innerIter.hasNext()) {
                    Operator innerOper = innerIter.next();
                    innerOpMap.put(((LogicalRelationalOperator) innerOper),
                            newLogToPhyMap.get(innerOper));
                }
                result.put((LOForEach) oper, innerOpMap);
            }
        }
        return result;
    }

    public LogicalPlan getNewPlan() {
        return newPreoptimizedPlan;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected void setSSHFactory() {
        Properties properties = this.pigContext.getProperties();
        String g = properties.getProperty("ssh.gateway");
        if (g == null || g.length() == 0)
            return;
        try {
            Class clazz = Class
                    .forName("org.apache.pig.shock.SSHSocketImplFactory");
            SocketImplFactory f = (SocketImplFactory) clazz.getMethod(
                    "getFactory", new Class[0]).invoke(0, new Object[0]);
            Socket.setSocketImplFactory(f);
        } catch (SocketException e) {
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Method to apply pig properties to JobConf (replaces properties with
     * resulting jobConf values)
     * 
     * @param conf
     *            JobConf with appropriate hadoop resource files
     * @param properties
     *            Pig properties that will override hadoop properties;
     *            properties might be modified
     */
    @SuppressWarnings("deprecation")
    protected void recomputeProperties(JobConf jobConf, Properties properties) {
        // We need to load the properties from the hadoop configuration
        // We want to override these with any existing properties we have.
        if (jobConf != null && properties != null) {
            // set user properties on the jobConf to ensure that defaults
            // and deprecation is applied correctly
            Enumeration<Object> propertiesIter = properties.keys();
            while (propertiesIter.hasMoreElements()) {
                String key = (String) propertiesIter.nextElement();
                String val = properties.getProperty(key);
                // We do not put user.name, See PIG-1419
                if (!key.equals("user.name"))
                    jobConf.set(key, val);
            }
            // clear user defined properties and re-populate
            properties.clear();
            Iterator<Map.Entry<String, String>> iter = jobConf.iterator();
            while (iter.hasNext()) {
                Map.Entry<String, String> entry = iter.next();
                properties.put(entry.getKey(), entry.getValue());
            }
        }
    }

    @Override
    public ScriptState instantiateScriptState() {
        return new MRScriptState(UUID.randomUUID().toString());
    }

    @Override
    public PigStats launchPig(LogicalPlan lp, String grpName, PigContext pc)
            throws FrontendException, ExecException {

        try {
            PhysicalPlan pp = compile(lp, pc.getProperties());
            return launcher.launchPig(pp, grpName, pigContext);
        } catch (ExecException e) {
            throw (ExecException) e;
        } catch (FrontendException e) {
            throw (FrontendException) e;
        } catch (Exception e) {
            throw new ExecException(e);
        } finally {
            launcher.reset();
        }
    }

    @Override
    public void explain(LogicalPlan lp, PigContext pc, PrintStream ps,
            String format, boolean verbose, File file, String suffix)
            throws PlanException, VisitorException, IOException,
            FrontendException {

        PrintStream pps = ps;
        PrintStream eps = ps;

        try {

            if (file != null) {
                pps = new PrintStream(new File(file, "physical_plan-" + suffix));
                eps = new PrintStream(new File(file, "exec_plan-" + suffix));
            }

            PhysicalPlan pp = compile(lp, pc.getProperties());
            pp.explain(pps, format, verbose);

            MapRedUtil.checkLeafIsStore(pp, pigContext);
            launcher.explain(pp, pigContext, eps, format, verbose);
        } finally {
            launcher.reset();
            pps.close();
            eps.close();
        }
    }

    @Override
    public Properties getConfiguration() {
        if (jobConf == null)
            return null;
        return ConfigurationUtil.toProperties(jobConf);
    }

    @Override
    public void setConfiguration(Properties newConfiguration)
            throws ExecException {
        init(newConfiguration);
    }

    @Override
    public void setProperty(String property, String value) {
        // mPigServer.getPigContext().getProperties().setProperty(key, value);
        // PIG-2508 properties need to be managed through JobConf
        // since all other code depends on access to properties,
        // we need to re-populate from updated JobConf
        // java.util.HashSet<?> keysBefore = new
        // java.util.HashSet<Object>(mPigServer.getPigContext().getProperties().keySet());
        // set current properties on jobConf
        Properties properties = pigContext.getProperties();
        Enumeration<Object> propertiesIter = pigContext.getProperties().keys();
        while (propertiesIter.hasMoreElements()) {
            String pkey = (String) propertiesIter.nextElement();
            String val = properties.getProperty(pkey);
            // We do not put user.name, See PIG-1419
            if (!pkey.equals("user.name"))
                jobConf.set(pkey, val);
        }
        // set new value, JobConf will handle deprecation etc.
        jobConf.set(property, value);
        // re-initialize to reflect updated JobConf
        properties.clear();
        Iterator<Map.Entry<String, String>> iter = jobConf.iterator();
        while (iter.hasNext()) {
            Map.Entry<String, String> entry = iter.next();
            properties.put(entry.getKey(), entry.getValue());
        }
        // keysBefore.removeAll(mPigServer.getPigContext().getProperties().keySet());
        // log.info("PIG-2508: keys dropped from properties: " + keysBefore);
    }

    @Override
    public ExecutableManager getExecutableManager() {
        return new HadoopExecutableManager();
    }

    @Override
    public void kill() throws BackendException {
        if (launcher != null) {
            launcher.kill();
        }
    }

    @Override
    public void killJob(String jobID) throws BackendException {
        if (launcher != null) {
            launcher.killJob(jobID, jobConf);
        }
    }

}
