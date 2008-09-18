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
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.FileOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketImplFactory;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.backend.executionengine.ExecLogicalPlan;
import org.apache.pig.backend.executionengine.ExecPhysicalOperator;
import org.apache.pig.backend.executionengine.ExecPhysicalPlan;
import org.apache.pig.backend.executionengine.ExecutionEngine;
import org.apache.pig.backend.executionengine.ExecJob.JOB_STATUS;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.datastorage.HDataStorage;
import org.apache.pig.backend.hadoop.executionengine.mapreduceExec.MapReduceLauncher;
import org.apache.pig.builtin.BinStorage;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.OperatorKey;
import org.apache.pig.shock.SSHSocketImplFactory;


public class HExecutionEngine implements ExecutionEngine {
    
    private static final String HOD_SERVER = "hod.server";
    public static final String JOB_TRACKER_LOCATION = "mapred.job.tracker";
    private static final String FILE_SYSTEM_LOCATION = "fs.default.name";
    
    private final Log log = LogFactory.getLog(getClass());
    private static final String LOCAL = "local";
    
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
    
    public void init(Properties properties) throws ExecException {
        //First set the ssh socket factory
        setSSHFactory();
        
        String hodServer = properties.getProperty(HOD_SERVER);
        String cluster = null;
        String nameNode = null;
        Configuration configuration = null;
    
        if (hodServer != null && hodServer.length() > 0) {
            String hdfsAndMapred[] = doHod(hodServer, properties);
            properties.setProperty(FILE_SYSTEM_LOCATION, hdfsAndMapred[0]);
            properties.setProperty(JOB_TRACKER_LOCATION, hdfsAndMapred[1]);
        }
        else {
            
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
            configuration = ConfigurationUtil.toConfiguration(properties);            
            properties = ConfigurationUtil.toProperties(configuration);
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
        }
     
        log.info("Connecting to hadoop file system at: "  + (nameNode==null? LOCAL: nameNode) )  ;
        ds = new HDataStorage(properties);
                
        // The above HDataStorage constructor sets DEFAULT_REPLICATION_FACTOR_KEY in properties.
        // So we need to reconstruct the configuration object for the non HOD case
        // In the HOD case, this is the first time the configuration object will be created
        configuration = ConfigurationUtil.toConfiguration(properties);
        
            
        if(cluster != null && !cluster.equalsIgnoreCase(LOCAL)){
                log.info("Connecting to map-reduce job tracker at: " + properties.get(JOB_TRACKER_LOCATION));
        }

        try {
            // Set job-specific configuration knobs
            jobClient = new JobClient(new JobConf(configuration));
        }
        catch (IOException e) {
            throw new ExecException("Failed to create job client", e);
        }
    }

    public void close() throws ExecException {
        closeHod(pigContext.getProperties().getProperty("hod.server"));
    }
        
    public Properties getConfiguration() throws ExecException {
        return this.pigContext.getProperties();
    }
        
    public void updateConfiguration(Properties newConfiguration) 
            throws ExecException {
        init(newConfiguration);
    }
        
    public Map<String, Object> getStatistics() throws ExecException {
        throw new UnsupportedOperationException();
    }

    public ExecPhysicalPlan compile(ExecLogicalPlan plan,
                                               Properties properties)
            throws ExecException {
        return compile(new ExecLogicalPlan[] { plan },
                       properties);
    }

    public ExecPhysicalPlan compile(ExecLogicalPlan[] plans,
                                               Properties properties)
            throws ExecException {
        if (plans == null) {
            throw new ExecException("No Plans to compile");
        }

        OperatorKey physicalKey = null;
        for (int i = 0; i < plans.length; ++i) {
            ExecLogicalPlan curPlan = null;

            curPlan = plans[ i ];
     
            OperatorKey logicalKey = curPlan.getRoot();
            
            physicalKey = logicalToPhysicalKeys.get(logicalKey);
            
            if (physicalKey == null) {
                try {
                physicalKey = new MapreducePlanCompiler(pigContext).
                                        compile(curPlan.getRoot(),
                                                curPlan.getOpTable(),
                                                this);
                }
                catch (IOException e) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("Failed to compile plan (");
                    sb.append(i);
                    sb.append(") ");
                    sb.append(logicalKey);
                    throw new ExecException(sb.toString(), e);
                }
                
                logicalToPhysicalKeys.put(logicalKey, physicalKey);
            }            
        }
        
        return new MapRedPhysicalPlan(physicalKey, physicalOpTable);
    }

    public ExecJob execute(ExecPhysicalPlan plan) 
            throws ExecException {

        POMapreduce pom = (POMapreduce) physicalOpTable.get(plan.getRoot());

        MapReduceLauncher.initQueryStatus(pom.numMRJobs());  // initialize status, for bookkeeping purposes.
        MapReduceLauncher.setConf(ConfigurationUtil.toConfiguration(pigContext.getProperties()));
        MapReduceLauncher.setExecEngine(this);
        
        // if the final operator is a MapReduce with no output file, then send to a temp
        // file.
        if (pom.outputFileSpec==null) {
            try {
                pom.outputFileSpec = new FileSpec(FileLocalizer.getTemporaryPath(null, pigContext).toString(),
                                                  BinStorage.class.getName());
            }
            catch (IOException e) {
                throw new ExecException("Failed to obtain temp file for " + plan.getRoot().toString(), e);
            }
        }

        try {
            pom.open();
            
            Tuple t;
            while ((t = (Tuple) pom.getNext()) != null) {
                ;
            }
            
            pom.close();
            
            this.materializedResults.put(plan.getRoot(),
                                         new MapRedResult(pom.outputFileSpec,
                                                           pom.reduceParallelism));
        }
        catch (IOException e) {
            throw new ExecException(e);
        }
        
        return new HJob(JOB_STATUS.COMPLETED, pigContext, pom.outputFileSpec);

    }

    public ExecJob submit(ExecPhysicalPlan plan) throws ExecException {
        throw new UnsupportedOperationException();
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

    //To prevent doing hod if the pig server is constructed multiple times
    private static String hodMapRed;
    private static String hodHDFS;
    private String hodConfDir = null; 
    private String remoteHodConfDir = null; 
    private Process hodProcess = null;

    class ShutdownThread extends Thread{
        public synchronized void run() {
            closeHod(pigContext.getProperties().getProperty("hod.server"));
        }
    }
    
    private String[] doHod(String server, Properties properties) throws ExecException {
        if (hodMapRed != null) {
            return new String[] {hodHDFS, hodMapRed};
        }
        
        try {
            // first, create temp director to store the configuration
            hodConfDir = createTempDir(server);
			
            //jz: fallback to systemproperty cause this not handled in Main
            StringBuilder hodParams = new StringBuilder(properties.getProperty(
                    "hod.param", System.getProperty("hod.param", "")));
            // get the number of nodes out of the command or use default
            int nodes = getNumNodes(hodParams);

            // command format: hod allocate - d <cluster_dir> -n <number_of_nodes> <other params>
                       String[] fixedCmdArray = new String[] { "hod", "allocate", "-d",
                                       hodConfDir, "-n", Integer.toString(nodes) };
               String[] extraParams = hodParams.toString().split(" ");
    
               String[] cmdarray = new String[fixedCmdArray.length + extraParams.length];
               System.arraycopy(fixedCmdArray, 0, cmdarray, 0, fixedCmdArray.length);
               System.arraycopy(extraParams, 0, cmdarray, fixedCmdArray.length, extraParams.length);

            log.info("Connecting to HOD...");
            log.debug("sending HOD command " + cmdToString(cmdarray));

            // setup shutdown hook to make sure we tear down hod connection
            Runtime.getRuntime().addShutdownHook(new ShutdownThread());

            hodProcess = runCommand(server, cmdarray);

            // print all the information provided by HOD
            try {
                BufferedReader br = new BufferedReader(new InputStreamReader(hodProcess.getErrorStream()));
                String msg;
                while ((msg = br.readLine()) != null)
                    log.info(msg);
                br.close();
            } catch(IOException ioe) {}

            // for remote connection we need to bring the file locally  
            if (!server.equals(LOCAL))
                hodConfDir = copyHadoopConfLocally(server);

            String hdfs = null;
            String mapred = null;
            String hadoopConf = hodConfDir + "/hadoop-site.xml";

            log.info ("Hadoop configuration file: " + hadoopConf);

            JobConf jobConf = new JobConf(hadoopConf);
            jobConf.addResource("pig-cluster-hadoop-site.xml");

			// We need to load the properties from the hadoop configuration
			// file we just found in the hod dir.  We want these to override
			// any existing properties we have.
        	if (jobConf != null) {
            	Iterator<Map.Entry<String, String>> iter = jobConf.iterator();
            	while (iter.hasNext()) {
                	Map.Entry<String, String> entry = iter.next();
                	properties.put(entry.getKey(), entry.getValue());
            	}
        	}

            hdfs = properties.getProperty(FILE_SYSTEM_LOCATION);
            if (hdfs == null)
                throw new ExecException("Missing fs.default.name from hadoop configuration");
            log.info("HDFS: " + hdfs);

            mapred = properties.getProperty(JOB_TRACKER_LOCATION);
            if (mapred == null)
                throw new ExecException("Missing mapred.job.tracker from hadoop configuration");
            log.info("JobTracker: " + mapred);

            // this is not longer needed as hadoop-site.xml given to us by HOD
            // contains data in the correct format
            // hdfs = fixUpDomain(hdfs, properties);
            // mapred = fixUpDomain(mapred, properties);
            hodHDFS = hdfs;
            hodMapRed = mapred;

            return new String[] {hdfs, mapred};
        } 
        catch (Exception e) {
            ExecException ee = new ExecException("Could not connect to HOD");
            ee.initCause(e);
            throw ee;
        }
    }

    private synchronized void closeHod(String server){
            if (hodProcess == null)
                return;

            // hod deallocate format: hod deallocate -d <conf dir>
            String[] cmdarray = new String[4];
			cmdarray[0] = "hod";
            cmdarray[1] = "deallocate";
            cmdarray[2] = "-d";
            if (remoteHodConfDir != null)
                cmdarray[3] = remoteHodConfDir;
            else
                cmdarray[3] = hodConfDir;

            log.info("Disconnecting from HOD...");
            log.debug("Disconnect command: " + cmdToString(cmdarray));

            try {
                Process p = runCommand(server, cmdarray);
           } catch (Exception e) {
                log.warn("Failed to disconnect from HOD; error: " + e.getMessage());
           } finally {
               if (remoteHodConfDir != null)
                   deleteDir(server, remoteHodConfDir);
               deleteDir(LOCAL, hodConfDir);
           }

           hodProcess = null;
    }

    private String copyHadoopConfLocally(String server) throws ExecException {
        String localDir = createTempDir(LOCAL);
        String remoteFile = new String(hodConfDir + "/hadoop-site.xml");
        String localFile = new String(localDir + "/hadoop-site.xml");

        remoteHodConfDir = hodConfDir;

        String[] cmdarray = new String[2];
        cmdarray[0] = "cat";
        cmdarray[1] = remoteFile;

        Process p = runCommand(server, cmdarray);

        BufferedWriter bw;
        try {
            bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(localFile)));
        } catch (Exception e){
            throw new ExecException("Failed to create local hadoop file " + localFile, e);
        }

        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line;
            while ((line = br.readLine()) != null){
                bw.write(line, 0, line.length());
                bw.newLine();
            }
            br.close();
            bw.close();
        } catch (Exception e){
            throw new ExecException("Failed to copy data to local hadoop file " + localFile, e);
        }

        return localDir;
    }

    private String cmdToString(String[] cmdarray) {
        StringBuilder cmd = new StringBuilder();

        for (int i = 0; i < cmdarray.length; i++) {
            cmd.append(cmdarray[i]);
            cmd.append(' ');
        }

        return cmd.toString();
    }
    private Process runCommand(String server, String[] cmdarray) throws ExecException {
        Process p;
        try {
            if (server.equals(LOCAL)) {
                p = Runtime.getRuntime().exec(cmdarray);
            } 
            else {
                SSHSocketImplFactory fac = SSHSocketImplFactory.getFactory(server);
                p = fac.ssh(cmdToString(cmdarray));
            }

            //this should return as soon as connection is shutdown
            int rc = p.waitFor();
            if (rc != 0) {
                String errMsg = new String();
                try {
                    BufferedReader br = new BufferedReader(new InputStreamReader(p.getErrorStream()));
                    errMsg = br.readLine();
                    br.close();
                } catch (IOException ioe) {}
                StringBuilder msg = new StringBuilder("Failed to run command ");
                msg.append(cmdToString(cmdarray));
                msg.append(" on server ");
                msg.append(server);
                msg.append("; return code: ");
                msg.append(rc);
                msg.append("; error: ");
                msg.append(errMsg);
                throw new ExecException(msg.toString());
            }
        } catch (Exception e){
            throw new ExecException(e);
        }

        return p;
    }

    private void deleteDir(String server, String dir) {
        if (server.equals(LOCAL)){
            File path = new File(dir);
            deleteLocalDir(path);
        }
        else { 
            // send rm command over ssh
            String[] cmdarray = new String[3];
			cmdarray[0] = "rm";
            cmdarray[1] = "-rf";
            cmdarray[2] = dir;

            try{
                Process p = runCommand(server, cmdarray);
            }catch(Exception e){
                    log.warn("Failed to remove HOD configuration directory - " + dir);
            }
        }
    }

    private void deleteLocalDir(File path){
        File[] files = path.listFiles();
        int i;
        for (i = 0; i < files.length; i++){
            if (files[i].isHidden())
                continue;
            if (files[i].isFile())
                files[i].delete();
            else if (files[i].isDirectory())
                deleteLocalDir(files[i]);
        }

        path.delete();
    }

    private String fixUpDomain(String hostPort,Properties properties) throws UnknownHostException {
        URI uri = null;
        try {
            uri = new URI(hostPort);
        } catch (URISyntaxException use) {
            throw new RuntimeException("Illegal hostPort: " + hostPort);
        }
        
        String hostname = uri.getHost();
        int port = uri.getPort();
        
        // Parse manually if hostPort wasn't non-opaque URI
        // e.g. hostPort is "myhost:myport"
        if (hostname == null || port == -1) {
            String parts[] = hostPort.split(":");
            hostname = parts[0];
            port = Integer.valueOf(parts[1]);
        }
        
        if (hostname.indexOf('.') == -1) {
          //jz: fallback to systemproperty cause this not handled in Main 
            String domain = properties.getProperty("cluster.domain",System.getProperty("cluster.domain"));
            if (domain == null) 
                throw new RuntimeException("Missing cluster.domain property!");
            hostname = hostname + "." + domain;
        }
        InetAddress.getByName(hostname);
        return hostname + ":" + Integer.toString(port);
    }

    // create temp dir to store hod output; removed on exit
    // format: <tempdir>/PigHod.<host name>.<user name>.<nanosecondts>
    private String createTempDir(String server) throws ExecException {
        StringBuilder tempDirPrefix  = new StringBuilder ();
        
        if (server.equals(LOCAL))
            tempDirPrefix.append(System.getProperty("java.io.tmpdir"));
        else
            // for remote access we assume /tmp as temp dir
            tempDirPrefix.append("/tmp");

        tempDirPrefix.append("/PigHod.");
        try {
            tempDirPrefix.append(InetAddress.getLocalHost().getHostName());
            tempDirPrefix.append(".");
        } catch (UnknownHostException e) {}
            
        tempDirPrefix.append(System.getProperty("user.name"));
        tempDirPrefix.append(".");
        String path;
        do {
            path = tempDirPrefix.toString() + System.nanoTime();
        } while (!createDir(server, path));

        return path;
    }

    private boolean createDir(String server, String dir) throws ExecException{
        if (server.equals(LOCAL)){ 
            // create local directory
            File tempDir = new File(dir);
            boolean success = tempDir.mkdir();
            if (!success)
                log.warn("Failed to create HOD configuration directory - " + dir + ". Retrying ...");

            return success;
        }
        else {
            String[] cmdarray = new String[2];
			cmdarray[0] = "mkdir ";
            cmdarray[1] = dir;

            try{
                Process p = runCommand(server, cmdarray);
            }
            catch(ExecException e){
                    log.warn("Failed to create HOD configuration directory - " + dir + "Retrying...");
                    return false;
            }

            return true;
        }
    }

    // returns number of nodes based on -m option in hodParams if present;
    // otherwise, default is used; -m is removed from the params
    int getNumNodes(StringBuilder hodParams) {
        String val = hodParams.toString();
        int startPos = val.indexOf("-m ");
        if (startPos == -1)
            startPos = val.indexOf("-m\t");
        if (startPos != -1) {
            int curPos = startPos + 3;
            int len = val.length();
            while (curPos < len && Character.isWhitespace(val.charAt(curPos))) curPos ++;
            int numStartPos = curPos;
            while (curPos < len && Character.isDigit(val.charAt(curPos))) curPos ++;
            int nodes = Integer.parseInt(val.substring(numStartPos, curPos));
            hodParams.delete(startPos, curPos);
            return nodes;
        } else {
            return Integer.getInteger("hod.nodes", 15);
        }
    }
}




