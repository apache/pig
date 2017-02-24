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
import java.net.URL;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.pig.PigException;
import org.apache.pig.backend.BackendException;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecutionEngine;
import org.apache.pig.backend.hadoop.HKerberos;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.datastorage.HDataStorage;
import org.apache.pig.backend.hadoop.executionengine.fetch.FetchLauncher;
import org.apache.pig.backend.hadoop.executionengine.fetch.FetchOptimizer;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRConfiguration;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PhyPlanSetter;
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
import org.apache.pig.impl.util.Utils;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LogToPhyTranslationVisitor;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.tools.pigstats.PigStats;

import com.google.common.collect.Maps;

public abstract class HExecutionEngine implements ExecutionEngine {

    private static final Log LOG = LogFactory.getLog(HExecutionEngine.class);

    public static final String HADOOP_SITE = "hadoop-site.xml";
    public static final String CORE_SITE = "core-site.xml";
    public static final String YARN_SITE = "yarn-site.xml";
    public static final String CORE_DEFAULT_SITE = "core-default.xml";
    public static final String MAPRED_DEFAULT_SITE = "mapred-default.xml";
    public static final String YARN_DEFAULT_SITE = "yarn-default.xml";

    public static final String LOCAL = "local";

    protected PigContext pigContext;
    protected DataStorage ds;
    protected Launcher launcher;

    // key: the operator key from the logical plan that originated the physical plan
    // val: the operator key for the root of the phyisical plan
    protected Map<OperatorKey, OperatorKey> logicalToPhysicalKeys;
    protected Map<Operator, PhysicalOperator> newLogToPhyMap;

    public HExecutionEngine(PigContext pigContext) {
        this.pigContext = pigContext;
        this.ds = null;
        this.logicalToPhysicalKeys = Maps.newHashMap();
    }

    @Deprecated
    public JobConf getJobConf() {
        JobConf jc = new JobConf(false);
        Utils.recomputeProperties(jc, pigContext.getProperties());
        return jc;
    }

    @Override
    public DataStorage getDataStorage() {
        return this.ds;
    }

    @Override
    public void init() throws ExecException {
        init(this.pigContext.getProperties());
    }

    // Loads S3 properties from core-site.xml including aws keys that are needed
    // for both local and non-local mode.
    public JobConf getS3Conf() throws ExecException {
        JobConf jc = new JobConf();
        jc.addResource(CORE_SITE);
        JobConf s3Jc = new JobConf(false);
        Iterator<Entry<String, String>> i = jc.iterator();
        while (i.hasNext()) {
            Entry<String, String> e = i.next();
            String key = e.getKey();
            String value = e.getValue();
            if (key.startsWith("fs.s3") || key.startsWith("fs.s3n")) {
                s3Jc.set(key, value);
            }
        }
        return s3Jc;
    }

    public JobConf getLocalConf() {
        JobConf jc = new JobConf(false);

        jc.addResource(CORE_DEFAULT_SITE);
        jc.addResource(MAPRED_DEFAULT_SITE);
        jc.addResource(YARN_DEFAULT_SITE);

        return jc;
    }

    public JobConf getExecConf(Properties properties) throws ExecException {
        JobConf jc = null;
        // Check existence of user provided configs
        String isHadoopConfigsOverriden = properties.getProperty("pig.use.overriden.hadoop.configs");
        if (isHadoopConfigsOverriden != null && isHadoopConfigsOverriden.equals("true")) {
            jc = new JobConf(ConfigurationUtil.toConfiguration(properties));
        } else {
            // Check existence of hadoop-site.xml or core-site.xml in
            // classpath if user provided confs are not being used
            Configuration testConf = new Configuration();
            ClassLoader cl = testConf.getClassLoader();
            URL hadoop_site = cl.getResource(HADOOP_SITE);
            URL core_site = cl.getResource(CORE_SITE);

            if (hadoop_site == null && core_site == null) {
                throw new ExecException(
                        "Cannot find hadoop configurations in classpath "
                                + "(neither hadoop-site.xml nor core-site.xml was found in the classpath)."
                                + " If you plan to use local mode, please put -x local option in command line",
                                4010);
            }
            jc = new JobConf();
        }
        jc.addResource("pig-cluster-hadoop-site.xml");
        jc.addResource(YARN_SITE);
        return jc;
    }

    @SuppressWarnings("resource")
    private void init(Properties properties) throws ExecException {
        String cluster = null;
        String nameNode = null;

        // We need to build a configuration object first in the manner described
        // below and then get back a properties object to inspect the
        // JOB_TRACKER_LOCATION and FILE_SYSTEM_LOCATION. The reason to do this
        // is if we looked only at the existing properties object, we may not
        // get the right settings. So we want to read the configurations in the
        // order specified below and only then look for JOB_TRACKER_LOCATION and
        // FILE_SYSTEM_LOCATION.

        // Hadoop by default specifies two resources, loaded in-order from the
        // classpath:
        // 1. hadoop-default.xml : Read-only defaults for hadoop.
        // 2. hadoop-site.xml: Site-specific configuration for a given hadoop
        // installation.
        // Now add the settings from "properties" object to override any
        // existing properties All of the above is accomplished in the method
        // call below

        JobConf jc;
        if (!this.pigContext.getExecType().isLocal()) {
            jc = getExecConf(properties);

            // Trick to invoke static initializer of DistributedFileSystem to
            // add hdfs-default.xml into configuration
            new DistributedFileSystem();
        } else {
            // If we are running in local mode we dont read the hadoop conf file
            if (properties.getProperty(MRConfiguration.FRAMEWORK_NAME) == null) {
                properties.setProperty(MRConfiguration.FRAMEWORK_NAME, LOCAL);
            }
            properties.setProperty(MRConfiguration.JOB_TRACKER, LOCAL);
            properties.remove("fs.default.name"); //Deprecated in Hadoop 2.x
            properties.setProperty(FileSystem.FS_DEFAULT_NAME_KEY, "file:///");

            jc = getLocalConf();
            JobConf s3Jc = getS3Conf();
            ConfigurationUtil.mergeConf(jc, s3Jc);
        }

        // the method below alters the properties object by overriding the
        // hadoop properties with the values from properties and recomputing
        // the properties
        Utils.recomputeProperties(jc, properties);

        // Ensure we have been logged in using the kerberos keytab (if provided) before continuing.
        HKerberos.tryKerberosKeytabLogin(jc);

        cluster = jc.get(MRConfiguration.JOB_TRACKER);
        nameNode = jc.get(FileSystem.FS_DEFAULT_NAME_KEY);

        LOG.info("Connecting to hadoop file system at: "
                + (nameNode == null ? LOCAL : nameNode));
        // constructor sets DEFAULT_REPLICATION_FACTOR_KEY
        ds = new HDataStorage(properties);

        if (cluster != null && !cluster.equalsIgnoreCase(LOCAL)) {
            LOG.info("Connecting to map-reduce job tracker at: "
                    + jc.get(MRConfiguration.JOB_TRACKER));
        }
    }

    public PhysicalPlan compile(LogicalPlan plan, Properties properties) throws FrontendException {
        if (plan == null) {
            int errCode = 2041;
            String msg = "No Plan to compile";
            throw new FrontendException(msg, errCode, PigException.BUG);
        }

        // translate new logical plan to physical plan
        LogToPhyTranslationVisitor translator = new LogToPhyTranslationVisitor(plan);

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
        Map<LOForEach, Map<LogicalRelationalOperator, PhysicalOperator>> result = Maps.newHashMap();
        Iterator<Operator> outerIter = plan.getOperators();
        while (outerIter.hasNext()) {
            Operator oper = outerIter.next();
            if (oper instanceof LOForEach) {
                LogicalPlan innerPlan = ((LOForEach) oper).getInnerPlan();
                Map<LogicalRelationalOperator, PhysicalOperator> innerOpMap = Maps.newHashMap();
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

    @Override
    public PigStats launchPig(LogicalPlan lp, String grpName, PigContext pc)
            throws FrontendException, ExecException {

        try {
            PhysicalPlan pp = compile(lp, pc.getProperties());
            //if the compiled physical plan fulfills the requirements of the
            //fetch optimizer, then further transformations / MR jobs creations are
            //skipped; a SimpleFetchPigStats will be returned through which the result
            //can be directly fetched from the underlying storage
            if (FetchOptimizer.isPlanFetchable(pc, pp)) {
                new PhyPlanSetter(pp).visit();
                return new FetchLauncher(pc).launchPig(pp);
            }
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
        boolean isFetchable = false;
        try {
            if (file != null) {
                pps = new PrintStream(new File(file, "physical_plan-" + suffix));
                eps = new PrintStream(new File(file, "exec_plan-" + suffix));
            }

            PhysicalPlan pp = compile(lp, pc.getProperties());
            pp.explain(pps, format, verbose);

            MapRedUtil.checkLeafIsStore(pp, pigContext);
            isFetchable = FetchOptimizer.isPlanFetchable(pc, pp);
            if (isFetchable) {
                new FetchLauncher(pigContext).explain(pp, pc, eps, format);
                return;
            }
            launcher.explain(pp, pigContext, eps, format, verbose);
        } finally {
            launcher.reset();
            if (isFetchable)
                pigContext.getProperties().remove(PigImplConstants.CONVERTED_TO_FETCH);
            //Only close the stream if we opened it.
            if (file != null) {
                pps.close();
                eps.close();
            }
        }
    }

    @Override
    public Properties getConfiguration() {
        Properties properties = new Properties();
        properties.putAll(pigContext.getProperties());
        return properties;
    }

    @Override
    public void setConfiguration(Properties newConfiguration) throws ExecException {
        init(newConfiguration);
    }

    @Override
    public void setProperty(String property, String value) {
        Properties properties = pigContext.getProperties();
        if (Configuration.isDeprecated(property)) {
            properties.putAll(ConfigurationUtil.expandForAlternativeNames(property, value));
        } else {
            properties.put(property, value);
        }
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
            launcher.killJob(jobID, getJobConf());
        }
    }

    @Override
    public void destroy() {
        if (launcher != null) {
            launcher.destroy();
        }
    }

}
