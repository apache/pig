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
import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.pig.ExecType;
import org.apache.pig.PigException;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.backend.executionengine.ExecPhysicalOperator;
import org.apache.pig.backend.executionengine.ExecutionEngine;
import org.apache.pig.backend.executionengine.util.ExecTools;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.datastorage.HDataStorage;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.LogToPhyTranslationVisitor;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceLauncher;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.tools.pigstats.PigStats;

public class HExecutionEngine implements ExecutionEngine {
    
    public static final String JOB_TRACKER_LOCATION = "mapred.job.tracker";
    private static final String FILE_SYSTEM_LOCATION = "fs.default.name";
    
    private static final String MAPRED_SITE = "mapred-site.xml";
    private static final String HDFS_SITE = "hdfs-site.xml";
    private static final String MAPRED_SYS_DIR = "mapred.system.dir";
    
    private final Log log = LogFactory.getLog(getClass());
    public static final String LOCAL = "local";
    
    protected PigContext pigContext;
    
    protected DataStorage ds;
    
    protected JobClient jobClient;

    // key: the operator key from the logical plan that originated the physical plan
    // val: the operator key for the root of the phyisical plan
    protected Map<OperatorKey, OperatorKey> logicalToPhysicalKeys;
    
    protected Map<OperatorKey, ExecPhysicalOperator> physicalOpTable;
    
    // map from LOGICAL key to into about the execution
    protected Map<OperatorKey, MapRedResult> materializedResults;
    
    public HExecutionEngine(PigContext pigContext) {
        this.pigContext = pigContext;
        this.logicalToPhysicalKeys = new HashMap<OperatorKey, OperatorKey>();
        this.physicalOpTable = new HashMap<OperatorKey, ExecPhysicalOperator>();
        this.materializedResults = new HashMap<OperatorKey, MapRedResult>();
        
        this.ds = null;
        
        // to be set in the init method
        this.jobClient = null;
    }
    
    public JobClient getJobClient() {
        return this.jobClient;
    }
    
    public Map<OperatorKey, MapRedResult> getMaterializedResults() {
        return this.materializedResults;
    }
    
    public Map<OperatorKey, ExecPhysicalOperator> getPhysicalOpTable() {
        return this.physicalOpTable;
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
           
        JobConf jobConf = null;
        if( this.pigContext.getExecType() == ExecType.LOCAL ) {
            // We dont load any configurations here
            jobConf = new JobConf( false );
        } else {
            jobConf = new JobConf();
            jobConf.addResource("pig-cluster-hadoop-site.xml");
        }
            
        //the method below alters the properties object by overriding the
        //hadoop properties with the values from properties and recomputing
        //the properties
        recomputeProperties(jobConf, properties);

        // If we are running in local mode we dont read the hadoop conf file
        if ( this.pigContext.getExecType() != ExecType.LOCAL ) {
            configuration = ConfigurationUtil.toConfiguration(properties);
            properties = ConfigurationUtil.toProperties(configuration);
        } else {
            properties.setProperty(JOB_TRACKER_LOCATION, LOCAL );
            properties.setProperty(FILE_SYSTEM_LOCATION, "file:///");
            
            Configuration testConf = new Configuration();
            ClassLoader cl = testConf.getClassLoader();
            URL mapred_site = cl.getResource( MAPRED_SITE );
            URL hdfs_site = cl.getResource( HDFS_SITE );
            
            if( mapred_site != null || hdfs_site != null ) {
                log.warn( "Passing Hadoop Site Configurations in classpath " +
                		"is not recommended for Local Mode" );
            }

            // This is one case. Here we check if mapred.system.dir  
            // directory is present. This check causes use to print a nice error
            String newMapredSystemDir = testConf.get( MAPRED_SYS_DIR, "" );
            Configuration defaultConf = new Configuration(false);
            defaultConf.addResource("core-default.xml");
            defaultConf.addResource("mapred-default.xml");
            if( defaultConf.get(MAPRED_SYS_DIR, "").compareTo(newMapredSystemDir) != 0 ) {
                File systemDir = new File(newMapredSystemDir);
                if( ! systemDir.exists() ) {
                    throw new ExecException( MAPRED_SYS_DIR + ": " + newMapredSystemDir 
                            + " mentioned in the configuration does not exist");
                }
            }
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

        try {
            // Set job-specific configuration knobs
            jobClient = new JobClient(new JobConf(configuration));
        }
        catch (IOException e) {
            int errCode = 6009;
            String msg = "Failed to create job client:" + e.getMessage();
            throw new ExecException(msg, errCode, PigException.BUG, e);
        }
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

    public PhysicalPlan compile(LogicalPlan plan,
                                Properties properties) throws ExecException {
        if (plan == null) {
            int errCode = 2041;
            String msg = "No Plan to compile";
            throw new ExecException(msg, errCode, PigException.BUG);
        }

        try {
            LogToPhyTranslationVisitor translator = 
                new LogToPhyTranslationVisitor(plan);
            translator.setPigContext(pigContext);
            translator.visit();
            return translator.getPhysicalPlan();
        } catch (VisitorException ve) {
            int errCode = 2042;
            String msg = "Internal error. Unable to translate logical plan to physical plan.";
            throw new ExecException(msg, errCode, PigException.BUG, ve);
        }
    }

    public List<ExecJob> execute(PhysicalPlan plan,
                                 String jobName) throws ExecException {
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

            for (FileSpec spec: launcher.getSucceededFiles()) {
                String alias = leafMap.containsKey(spec.toString()) ? leafMap.get(spec.toString()).getAlias() : null;
                jobs.add(new HJob(ExecJob.JOB_STATUS.COMPLETED, pigContext, spec, alias, stats));
            }

            for (FileSpec spec: launcher.getFailedFiles()) {
                String alias = leafMap.containsKey(spec.toString()) ? leafMap.get(spec.toString()).getAlias() : null;
                HJob j = new HJob(ExecJob.JOB_STATUS.FAILED, pigContext, spec, alias, stats);
                j.setException(launcher.getError(spec));
                jobs.add(j);
            }

            return jobs;
        } catch (Exception e) {
            // There are a lot of exceptions thrown by the launcher.  If this
            // is an ExecException, just let it through.  Else wrap it.
            if (e instanceof ExecException) throw (ExecException)e;
            else {
                int errCode = 2043;
                String msg = "Unexpected error during execution.";
                throw new ExecException(msg, errCode, PigException.BUG, e);
            }
        } finally {
            launcher.reset();
        }

    }

    public List<ExecJob> submit(PhysicalPlan plan,
                          String jobName) throws ExecException {
        throw new UnsupportedOperationException();
    }

    public void explain(PhysicalPlan plan, PrintStream stream, String format, boolean verbose) {
        try {
            ExecTools.checkLeafIsStore(plan, pigContext);

            MapReduceLauncher launcher = new MapReduceLauncher();
            launcher.explain(plan, pigContext, stream, format, verbose);

        } catch (Exception ve) {
            throw new RuntimeException(ve);
        }
    }

    public Collection<ExecJob> runningJobs(Properties properties) throws ExecException {
        throw new UnsupportedOperationException();
    }
    
    public Collection<String> activeScopes() throws ExecException {
        throw new UnsupportedOperationException();
    }
    
    public void reclaimScope(String scope) throws ExecException {
        throw new UnsupportedOperationException();
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
}
