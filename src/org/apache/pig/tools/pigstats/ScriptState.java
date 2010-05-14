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
package org.apache.pig.tools.pigstats;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.VersionInfo;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POCollectedGroup;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PODistinct;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFRJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POMergeCogroup;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POMergeJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStream;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.JarManager;

/**
 * ScriptStates encapsulates settings for a Pig script that runs on a hadoop 
 * cluster. These settings are added to all MR jobs spawned by the script and 
 * in turn are persisted in the hadoop job xml. With the properties already in 
 * the job xml, users who want to know the relations between the script and MR
 * jobs can derive them from the job xmls.  
 */
public class ScriptState {
    
    /**
     * Keys of Pig settings added in MR job
     */
    private enum PIG_PROPERTY {
        SCRIPT_ID           ("pig.script.id"),
        SCRIPT              ("pig.script"),
        LAUNCHER_HOST       ("pig.launcher.host"),
        COMMAND_LINE        ("pig.command.line"),
        HADOOP_VERSION      ("pig.hadoop.version"),
        VERSION             ("pig.version"),
        INPUT_DIRS          ("pig.input.dirs"),
        MAP_OUTPUT_DIRS     ("pig.map.output.dirs"),
        REDUCE_OUTPUT_DIRS  ("pig.reduce.output.dirs"),
        FEATURE             ("pig.feature");
       
        private String displayStr;
        
        private PIG_PROPERTY(String s) {
            displayStr = s;
        }
        
        @Override
        public String toString() { return displayStr; }
    };
    
    /**
     * Features used in a Pig script
     */
    private enum PIG_FEATURE {
        MERGE_JION,
        REPLICATED_JOIN,
        SKEWED_JION,
        COLLECTED_GROUP,
        MERGE_COGROUP,
        ORDER_BY,
        DISTINCT,
        STREAMING,
        MAP_ONLY;
    };
    
    /**
     * Pig property that allows user to turn off the inclusion of settings
     * in the jobs 
     */
    public static final String INSERT_ENABLED = "pig.script.info.enabled";
    
    private static final Log LOG = LogFactory.getLog(ScriptState.class);

    private static ThreadLocal<ScriptState> tss = new ThreadLocal<ScriptState>();
       
    private String id;
    
    private String script;
    private String commandLine;
    private String feature;
    
    private String host;
    private String pigVersion;
    private String hodoopVersion;
           
    public static ScriptState start(String commandLine) {
        ScriptState ss = new ScriptState(UUID.randomUUID().toString());
        ss.setCommandLine(commandLine);
        tss.set(ss);
        return ss;
    }
    
    private ScriptState(String id) {
        this.id = id;
        this.script = ""; 
    }

    public static ScriptState get() {
        if (tss.get() == null) {
            ScriptState.start("");
        }
        return tss.get();
    }
           
    public void addSettingsToConf(MapReduceOper mro, Configuration conf) {
        LOG.info("Pig script settings is added to the job");
        conf.set(PIG_PROPERTY.HADOOP_VERSION.toString(), getHadoopVersion());
        conf.set(PIG_PROPERTY.VERSION.toString(), getPigVersion());
        conf.set(PIG_PROPERTY.SCRIPT_ID.toString(), id);
        conf.set(PIG_PROPERTY.SCRIPT.toString(), getScript());
        conf.set(PIG_PROPERTY.LAUNCHER_HOST.toString(), getHostName());
        conf.set(PIG_PROPERTY.COMMAND_LINE.toString(), getCommandLine());
        
        try {
            LinkedList<POStore> stores = PlanHelper.getStores(mro.mapPlan);
            ArrayList<String> outputDirs = new ArrayList<String>();
            for (POStore st: stores) {  
                outputDirs.add(st.getSFile().getFileName()); 
            }                 
            conf.set(PIG_PROPERTY.MAP_OUTPUT_DIRS.toString(), LoadFunc.join(outputDirs, ","));
        } catch (VisitorException e) {
            LOG.warn("unable to get the map stores", e);
        }
        if (!mro.reducePlan.isEmpty()) {
            try {
                LinkedList<POStore> stores = PlanHelper.getStores(mro.reducePlan);
                ArrayList<String> outputDirs = new ArrayList<String>();
                for (POStore st: stores) {  
                    outputDirs.add(st.getSFile().getFileName()); 
                }                      
                conf.set(PIG_PROPERTY.REDUCE_OUTPUT_DIRS.toString(), LoadFunc.join(outputDirs, ","));
            } catch (VisitorException e) {
                LOG.warn("unable to get the reduce stores", e);
            }
        }        
        try {
            List<POLoad> lds = PlanHelper.getLoads(mro.mapPlan);
            ArrayList<String> inputDirs = new ArrayList<String>();
            if (lds != null && lds.size() > 0){
                for (POLoad ld : lds) {
                    inputDirs.add(ld.getLFile().getFileName());
                }               
                conf.set(PIG_PROPERTY.INPUT_DIRS.toString(), LoadFunc.join(inputDirs, ","));       
            }
        } catch (VisitorException e) {
            LOG.warn("unable to get the map loads", e);
        }

        setPigFeature(mro, conf);
    }
 
    public void setScript(File file) {            
        try {
            setScript(new BufferedReader(new FileReader(file)));
        } catch (FileNotFoundException e) {
            LOG.warn("unable to find the file", e);
        }
    }

    public void setScript(String script) {            
        this.script = script;
    }

    private String getScript() {
        return (script == null) ? "" : script;
    }
    
    private String getHadoopVersion() {
        if (hodoopVersion == null) {
            hodoopVersion = VersionInfo.getVersion();
        }
        return (hodoopVersion == null) ? "" : hodoopVersion;
    }
    
    private String getPigVersion() {
        if (pigVersion == null) {
            String findContainingJar = JarManager.findContainingJar(ScriptState.class);
            try { 
                JarFile jar = new JarFile(findContainingJar); 
                final Manifest manifest = jar.getManifest(); 
                final Map <String,Attributes> attrs = manifest.getEntries(); 
                Attributes attr = attrs.get("org/apache/pig");
                pigVersion = attr.getValue("Implementation-Version");
            } catch (Exception e) { 
                LOG.warn("unable to read pigs manifest file", e); 
            } 
        }
        return (pigVersion == null) ? "" : pigVersion;
    }
    
    private String getHostName() { 
        if (host == null) {
            try {
                InetAddress addr = InetAddress.getLocalHost();
                host = addr.getHostName(); 
            } catch (UnknownHostException e) {
                LOG.warn("unable to get host name", e); 
            }         
        }
        return (host == null) ? "" : host;
    }
    
    private String getCommandLine() {
        return (commandLine == null) ? "" : commandLine;
    }
    
    private void setCommandLine(String commandLine) {
        this.commandLine = commandLine;
    }
    
    private void setScript(BufferedReader reader) {
        StringBuilder sb = new StringBuilder();
        try {
            String line = reader.readLine();
            while (line != null) {
                line = line.trim();
                if (line.length() > 0 && !line.startsWith("--")) {
                    sb.append(line);
                }                
                line = reader.readLine();
            }            
        } catch (IOException e) {
            LOG.warn("unable to parse the script", e);
        }
        this.script = sb.toString();
    }
    
    private void setPigFeature(MapReduceOper mro, Configuration conf) {
        feature = "";
        if (mro.isSkewedJoin()) {
            feature = PIG_FEATURE.SKEWED_JION.toString();
        } else if (mro.isGlobalSort()) {
            feature = PIG_FEATURE.ORDER_BY.toString();
        } else {
            try {
                new FeatureVisitor(mro.mapPlan).visit();
                if (mro.reducePlan.isEmpty()) { 
                    feature = feature.isEmpty() ? 
                            PIG_FEATURE.MAP_ONLY.toString() : feature;                    
                } else {
                    new FeatureVisitor(mro.reducePlan).visit();
                }
            } catch (VisitorException e) {
                LOG.warn("Feature visitor failed", e);
            }
        }
        conf.set(PIG_PROPERTY.FEATURE.toString(), feature);
    }
    
    private class FeatureVisitor extends PhyPlanVisitor {
        
        public FeatureVisitor(PhysicalPlan plan) {
            super(plan, new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(
                    plan));
        }
        
        @Override
        public void visitFRJoin(POFRJoin join) throws VisitorException {
            feature = PIG_FEATURE.REPLICATED_JOIN.toString();
        }
        
        @Override
        public void visitMergeJoin(POMergeJoin join) throws VisitorException {
            feature = PIG_FEATURE.MERGE_JION.toString();
        }
        
        @Override
        public void visitMergeCoGroup(POMergeCogroup mergeCoGrp)
                throws VisitorException {
            feature = PIG_FEATURE.MERGE_COGROUP.toString();
        }
        
        @Override
        public void visitCollectedGroup(POCollectedGroup mg)
                throws VisitorException {           
            feature = PIG_FEATURE.COLLECTED_GROUP.toString();
        }
        
        @Override
        public void visitDistinct(PODistinct distinct) throws VisitorException {
            feature = PIG_FEATURE.DISTINCT.toString();
        }
        
        @Override
        public void visitStream(POStream stream) throws VisitorException {
            feature = PIG_FEATURE.STREAMING.toString();
        }
    }    
}
