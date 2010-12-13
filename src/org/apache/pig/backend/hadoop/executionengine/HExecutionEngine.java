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

import java.io.IOException;
import java.io.PrintStream;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketImplFactory;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.pig.ExecType;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigException;
import org.apache.pig.SortInfo;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.datastorage.HDataStorage;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceLauncher;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.LogToPhyTranslationVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.LOForEach;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.newplan.DependencyOrderWalker;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.LogicalPlanMigrationVistor;
import org.apache.pig.newplan.logical.expression.ConstantExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.optimizer.SchemaResetter;
import org.apache.pig.newplan.logical.relational.LOLimit;
import org.apache.pig.newplan.logical.relational.LOSort;
import org.apache.pig.newplan.logical.relational.LOSplit;
import org.apache.pig.newplan.logical.relational.LOSplitOutput;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LogicalRelationalNodesVisitor;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.rules.InputOutputFileValidator;
import org.apache.pig.tools.pigstats.OutputStats;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.pen.POOptimizeDisabler;

public class HExecutionEngine {
    
    public static final String JOB_TRACKER_LOCATION = "mapred.job.tracker";
    private static final String FILE_SYSTEM_LOCATION = "fs.default.name";
    
    private static final String HADOOP_SITE = "hadoop-site.xml";
    private static final String CORE_SITE = "core-site.xml";
    private final Log log = LogFactory.getLog(getClass());
    public static final String LOCAL = "local";
    
    protected PigContext pigContext;
    
    protected DataStorage ds;
    
    @SuppressWarnings("deprecation")
    protected JobConf jobConf;

    // key: the operator key from the logical plan that originated the physical plan
    // val: the operator key for the root of the phyisical plan
    protected Map<OperatorKey, OperatorKey> logicalToPhysicalKeys;
    
    // map from LOGICAL key to into about the execution
    protected Map<OperatorKey, MapRedResult> materializedResults;
    
    protected Map<LogicalOperator, PhysicalOperator> logToPhyMap;
    protected Map<LogicalOperator, LogicalRelationalOperator> opsMap;
    protected Map<Operator, PhysicalOperator> newLogToPhyMap;
    private Map<LOForEach, Map<LogicalOperator, LogicalRelationalOperator>> forEachInnerOpMap;
    
    public HExecutionEngine(PigContext pigContext) {
        this.pigContext = pigContext;
        this.logicalToPhysicalKeys = new HashMap<OperatorKey, OperatorKey>();      
        this.materializedResults = new HashMap<OperatorKey, MapRedResult>();
        
        this.ds = null;
        
        // to be set in the init method
        this.jobConf = null;
    }
    
    @SuppressWarnings("deprecation")
    public JobConf getJobConf() {
        return this.jobConf;
    }
    
    public Map<OperatorKey, MapRedResult> getMaterializedResults() {
        return this.materializedResults;
    }
        
    public DataStorage getDataStorage() {
        return this.ds;
    }

    public void init() throws ExecException {
        init(this.pigContext.getProperties());
    }
    
    @SuppressWarnings("deprecation")
    public void init(Properties properties) throws ExecException {
        //First set the ssh socket factory
        setSSHFactory();
        
        String cluster = null;
        String nameNode = null;
        Configuration configuration = null;
    
        // We need to build a configuration object first in the manner described below
        // and then get back a properties object to inspect the JOB_TRACKER_LOCATION
        // and FILE_SYSTEM_LOCATION. The reason to do this is if we looked only at
        // the existing properties object, we may not get the right settings. So we want
        // to read the configurations in the order specified below and only then look
        // for JOB_TRACKER_LOCATION and FILE_SYSTEM_LOCATION.
            
        // Hadoop by default specifies two resources, loaded in-order from the classpath:
        // 1. hadoop-default.xml : Read-only defaults for hadoop.
        // 2. hadoop-site.xml: Site-specific configuration for a given hadoop installation.
        // Now add the settings from "properties" object to override any existing properties
        // All of the above is accomplished in the method call below
           
        JobConf jc = null;
        if ( this.pigContext.getExecType() == ExecType.MAPREDUCE ) {
        	
            // Check existence of hadoop-site.xml or core-site.xml
        	Configuration testConf = new Configuration();
            ClassLoader cl = testConf.getClassLoader();
            URL hadoop_site = cl.getResource( HADOOP_SITE );
            URL core_site = cl.getResource( CORE_SITE );
            
            if( hadoop_site == null && core_site == null ) {
            	throw new ExecException("Cannot find hadoop configurations in classpath (neither hadoop-site.xml nor core-site.xml was found in the classpath)." +
            			"If you plan to use local mode, please put -x local option in command line", 
            			4010);
            }

            jc = new JobConf();
            jc.addResource("pig-cluster-hadoop-site.xml");
            
            // Trick to invoke static initializer of DistributedFileSystem to add hdfs-default.xml 
            // into configuration
            new DistributedFileSystem();
            
            //the method below alters the properties object by overriding the
            //hadoop properties with the values from properties and recomputing
            //the properties
            recomputeProperties(jc, properties);
        } else {
            // If we are running in local mode we dont read the hadoop conf file
            jc = new JobConf(false);
            jc.addResource("core-default.xml");
            jc.addResource("mapred-default.xml");
            recomputeProperties(jc, properties);
            
            properties.setProperty(JOB_TRACKER_LOCATION, LOCAL );
            properties.setProperty(FILE_SYSTEM_LOCATION, "file:///");
        }
        
        cluster = properties.getProperty(JOB_TRACKER_LOCATION);
        nameNode = properties.getProperty(FILE_SYSTEM_LOCATION);

        if (cluster != null && cluster.length() > 0) {
            if(!cluster.contains(":") && !cluster.equalsIgnoreCase(LOCAL)) {
                cluster = cluster + ":50020";
            }
            properties.setProperty(JOB_TRACKER_LOCATION, cluster);
        }

        if (nameNode!=null && nameNode.length() > 0) {
            if(!nameNode.contains(":")  && !nameNode.equalsIgnoreCase(LOCAL)) {
                nameNode = nameNode + ":8020";
            }
            properties.setProperty(FILE_SYSTEM_LOCATION, nameNode);
        }
     
        log.info("Connecting to hadoop file system at: "  + (nameNode==null? LOCAL: nameNode) )  ;
        ds = new HDataStorage(properties);
                
        // The above HDataStorage constructor sets DEFAULT_REPLICATION_FACTOR_KEY in properties.
        configuration = ConfigurationUtil.toConfiguration(properties);
        
            
        if(cluster != null && !cluster.equalsIgnoreCase(LOCAL)){
            log.info("Connecting to map-reduce job tracker at: " + properties.get(JOB_TRACKER_LOCATION));
        }

        // Set job-specific configuration knobs
        jobConf = new JobConf(configuration);
    }

    public Properties getConfiguration() throws ExecException {
        return this.pigContext.getProperties();
    }
        
    public void updateConfiguration(Properties newConfiguration) 
            throws ExecException {
        init(newConfiguration);
    }
        
    public void close() throws ExecException {}

    public Map<String, Object> getStatistics() throws ExecException {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("unchecked")
    public PhysicalPlan compile(LogicalPlan plan,
                                Properties properties) throws FrontendException {
        if (plan == null) {
            int errCode = 2041;
            String msg = "No Plan to compile";
            throw new FrontendException(msg, errCode, PigException.BUG);
        }

        try {
            if (getConfiguration().getProperty("pig.usenewlogicalplan", "true").equals("true")) {
                log.info("pig.usenewlogicalplan is set to true. New logical plan will be used.");
                
                // translate old logical plan to new plan
                LogicalPlanMigrationVistor visitor = new LogicalPlanMigrationVistor(plan);
                visitor.visit();
                opsMap = visitor.getOldToNewLOOpMap();
                forEachInnerOpMap = visitor.getForEachInnerMap();
                org.apache.pig.newplan.logical.relational.LogicalPlan newPlan = visitor.getNewLogicalPlan();
                
                if (pigContext.inIllustrator) {
                    // disable all PO-specific optimizations
                    POOptimizeDisabler pod = new POOptimizeDisabler(newPlan);
                    pod.visit();
                }
                
                SchemaResetter schemaResetter = new SchemaResetter(newPlan);
                schemaResetter.visit();
                
                HashSet<String> optimizerRules = null;
                try {
                    optimizerRules = (HashSet<String>) ObjectSerializer
                            .deserialize(pigContext.getProperties().getProperty(
                                    "pig.optimizer.rules"));
                } catch (IOException ioe) {
                    int errCode = 2110;
                    String msg = "Unable to deserialize optimizer rules.";
                    throw new FrontendException(msg, errCode, PigException.BUG, ioe);
                }
                
                if (pigContext.inIllustrator) {
                    // disable MergeForEach in illustrator
                    if (optimizerRules == null)
                        optimizerRules = new HashSet<String>();
                    optimizerRules.add("MergeForEach");
                    optimizerRules.add("PartitionFilterOptimizer");
                    optimizerRules.add("LimitOptimizer");
                    optimizerRules.add("SplitFilter");
                    optimizerRules.add("PushUpFilter");
                    optimizerRules.add("MergeFilter");
                    optimizerRules.add("PushDownForEachFlatten");
                    optimizerRules.add("ColumnMapKeyPrune");
                    optimizerRules.add("AddForEach");
                    optimizerRules.add("GroupByConstParallelSetter");
                }
                
                // run optimizer
                org.apache.pig.newplan.logical.optimizer.LogicalPlanOptimizer optimizer = 
                    new org.apache.pig.newplan.logical.optimizer.LogicalPlanOptimizer(newPlan, 100, optimizerRules);
                optimizer.optimize();
                
                // compute whether output data is sorted or not
                SortInfoSetter sortInfoSetter = new SortInfoSetter(newPlan);
                sortInfoSetter.visit();
                
                if (pigContext.inExplain==false) {
                    // Validate input/output file. Currently no validation framework in
                    // new logical plan, put this validator here first.
                    // We might decide to move it out to a validator framework in future
                    InputOutputFileValidator validator = new InputOutputFileValidator(newPlan, pigContext);
                    validator.validate();
                }
                
                // translate new logical plan to physical plan
                org.apache.pig.newplan.logical.relational.LogToPhyTranslationVisitor translator = 
                    new org.apache.pig.newplan.logical.relational.LogToPhyTranslationVisitor(newPlan);
                
                translator.setPigContext(pigContext);
                translator.visit();
                newLogToPhyMap = translator.getLogToPhyMap();
                return translator.getPhysicalPlan();
                
            }else{       
                LogToPhyTranslationVisitor translator = 
                    new LogToPhyTranslationVisitor(plan);
                translator.setPigContext(pigContext);
                translator.visit();
                return translator.getPhysicalPlan();
            }
        } catch (ExecException ve) {
            int errCode = 2042;
            String msg = "Error in new logical plan. Try -Dpig.usenewlogicalplan=false.";
            throw new FrontendException(msg, errCode, PigException.BUG, ve);
        }
    }
    
    public Map<LogicalOperator, PhysicalOperator> getLogToPhyMap() {
        if (logToPhyMap != null)
            return logToPhyMap;
        else if (newLogToPhyMap != null) {
            Map<LogicalOperator, PhysicalOperator> result = new HashMap<LogicalOperator, PhysicalOperator>();
            for (LogicalOperator lo: opsMap.keySet()) {
                result.put(lo, newLogToPhyMap.get(opsMap.get(lo))); 
            }
            return result;
        } else
            return null;
    }
    
    public Map<LOForEach, Map<LogicalOperator, PhysicalOperator>> getForEachInnerLogToPhyMap() {
        Map<LOForEach, Map<LogicalOperator, PhysicalOperator>> result =
            new HashMap<LOForEach, Map<LogicalOperator, PhysicalOperator>>();
        for (Map.Entry<LOForEach, Map<LogicalOperator, LogicalRelationalOperator>> entry :
            forEachInnerOpMap.entrySet()) {
            Map<LogicalOperator, PhysicalOperator> innerOpMap = new HashMap<LogicalOperator, PhysicalOperator>();
            for (Map.Entry<LogicalOperator, LogicalRelationalOperator> innerEntry : entry.getValue().entrySet()) {
                innerOpMap.put(innerEntry.getKey(), newLogToPhyMap.get(innerEntry.getValue()));
            }
            result.put(entry.getKey(), innerOpMap);
        }
        return result;
    }
    
    public static class SortInfoSetter extends LogicalRelationalNodesVisitor {

        public SortInfoSetter(OperatorPlan plan) throws FrontendException {
            super(plan, new DependencyOrderWalker(plan));
        }

        @Override
        public void visit(LOStore store) throws FrontendException {
            
            Operator storePred = store.getPlan().getPredecessors(store).get(0);
            if(storePred == null){
                int errCode = 2051;
                String msg = "Did not find a predecessor for Store." ;
                throw new FrontendException(msg, errCode, PigException.BUG);    
            }
            
            SortInfo sortInfo = null;
            if(storePred instanceof LOLimit) {
                storePred = store.getPlan().getPredecessors(storePred).get(0);
            } else if (storePred instanceof LOSplitOutput) {
                LOSplitOutput splitOutput = (LOSplitOutput)storePred;
                // We assume this is the LOSplitOutput we injected for this case:
                // b = order a by $0; store b into '1'; store b into '2';
                // In this case, we should mark both '1' and '2' as sorted
                LogicalExpressionPlan conditionPlan = splitOutput.getFilterPlan();
                if (conditionPlan.getSinks().size()==1) {
                    Operator root = conditionPlan.getSinks().get(0);
                    if (root instanceof ConstantExpression) {
                        Object value = ((ConstantExpression)root).getValue();
                        if (value instanceof Boolean && (Boolean)value==true) {
                            Operator split = splitOutput.getPlan().getPredecessors(splitOutput).get(0);
                            if (split instanceof LOSplit)
                                storePred = store.getPlan().getPredecessors(split).get(0);
                        }
                    }
                }
            }
            // if this predecessor is a sort, get
            // the sort info.
            if(storePred instanceof LOSort) {
                try {
                    sortInfo = ((LOSort)storePred).getSortInfo();
                } catch (FrontendException e) {
                    throw new FrontendException(e);
                }
            }
            store.setSortInfo(sortInfo);
        }
    }

    public List<ExecJob> execute(PhysicalPlan plan,
                                 String jobName) throws ExecException, FrontendException {
        MapReduceLauncher launcher = new MapReduceLauncher();
        List<ExecJob> jobs = new ArrayList<ExecJob>();

        Map<String, PhysicalOperator> leafMap = new HashMap<String, PhysicalOperator>();
        for (PhysicalOperator physOp : plan.getLeaves()) {
            log.info(physOp);
            if (physOp instanceof POStore) {
                FileSpec spec = ((POStore) physOp).getSFile();
                if (spec != null)
                    leafMap.put(spec.toString(), physOp);
            }
        }
        try {
            PigStats stats = launcher.launchPig(plan, jobName, pigContext);

            for (OutputStats output : stats.getOutputStats()) {
                POStore store = output.getPOStore();               
                String alias = store.getAlias();
                if (output.isSuccessful()) {
                    jobs.add(new HJob(ExecJob.JOB_STATUS.COMPLETED, pigContext, store, alias, stats));
                } else {
                    HJob j = new HJob(ExecJob.JOB_STATUS.FAILED, pigContext, store, alias, stats);  
                    j.setException(launcher.getError(store.getSFile()));
                    jobs.add(j);
                }
            }

            return jobs;
        } catch (Exception e) {
            // There are a lot of exceptions thrown by the launcher.  If this
            // is an ExecException, just let it through.  Else wrap it.
            if (e instanceof ExecException){
            	throw (ExecException)e;
            } else if (e instanceof FrontendException) {
            	throw (FrontendException)e;
            } else {
                int errCode = 2043;
                String msg = "Unexpected error during execution.";
                throw new ExecException(msg, errCode, PigException.BUG, e);
            }
        } finally {
            launcher.reset();
        }

    }

    public void explain(PhysicalPlan plan, PrintStream stream, String format, boolean verbose) {
        try {
            MapRedUtil.checkLeafIsStore(plan, pigContext);

            MapReduceLauncher launcher = new MapReduceLauncher();
            launcher.explain(plan, pigContext, stream, format, verbose);

        } catch (Exception ve) {
            throw new RuntimeException(ve);
        }
    }
  
    @SuppressWarnings("unchecked")
    private void setSSHFactory(){
        Properties properties = this.pigContext.getProperties();
        String g = properties.getProperty("ssh.gateway");
        if (g == null || g.length() == 0) return;
        try {
            Class clazz = Class.forName("org.apache.pig.shock.SSHSocketImplFactory");
            SocketImplFactory f = (SocketImplFactory)clazz.getMethod("getFactory", new Class[0]).invoke(0, new Object[0]);
            Socket.setSocketImplFactory(f);
        } 
        catch (SocketException e) {}
        catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    /**
     * Method to recompute pig properties by overriding hadoop properties
     * with pig properties
     * @param conf JobConf with appropriate hadoop resource files
     * @param properties Pig properties that will override hadoop properties; properties might be modified
     */
    @SuppressWarnings("deprecation")
    private void recomputeProperties(JobConf jobConf, Properties properties) {
        // We need to load the properties from the hadoop configuration
        // We want to override these with any existing properties we have.
        if (jobConf != null && properties != null) {
            Properties hadoopProperties = new Properties();
            Iterator<Map.Entry<String, String>> iter = jobConf.iterator();
            while (iter.hasNext()) {
                Map.Entry<String, String> entry = iter.next();
                hadoopProperties.put(entry.getKey(), entry.getValue());
            }

            //override hadoop properties with user defined properties
            Enumeration<Object> propertiesIter = properties.keys();
            while (propertiesIter.hasMoreElements()) {
                String key = (String) propertiesIter.nextElement();
                String val = properties.getProperty(key);

                // We do not put user.name, See PIG-1419
                if (!key.equals("user.name"))
                    hadoopProperties.put(key, val);
            }
            
            //clear user defined properties and re-populate
            properties.clear();
            Enumeration<Object> hodPropertiesIter = hadoopProperties.keys();
            while (hodPropertiesIter.hasMoreElements()) {
                String key = (String) hodPropertiesIter.nextElement();
                String val = hadoopProperties.getProperty(key);
                properties.put(key, val);
            }
        }
    } 
    
    public static FileSpec checkLeafIsStore(
            PhysicalPlan plan,
            PigContext pigContext) throws ExecException {
        try {
            PhysicalOperator leaf = plan.getLeaves().get(0);
            FileSpec spec = null;
            if(!(leaf instanceof POStore)){
                String scope = leaf.getOperatorKey().getScope();
                POStore str = new POStore(new OperatorKey(scope,
                    NodeIdGenerator.getGenerator().getNextNodeId(scope)));
                spec = new FileSpec(FileLocalizer.getTemporaryPath(
                    pigContext).toString(),
                    new FuncSpec(Utils.getTmpFileCompressorName(pigContext)));
                str.setSFile(spec);
                plan.addAsLeaf(str);
            } else{
                spec = ((POStore)leaf).getSFile();
            }
            return spec;
        } catch (Exception e) {
            int errCode = 2045;
            String msg = "Internal error. Not able to check if the leaf node is a store operator.";
            throw new ExecException(msg, errCode, PigException.BUG, e);
        }
    }
}
