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
package org.apache.pig.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.pig.Main;
import org.apache.pig.ExecType;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.datastorage.DataStorageException;
import org.apache.pig.backend.datastorage.ElementDescriptor;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecutionEngine;
import org.apache.pig.backend.hadoop.datastorage.HDataStorage;
import org.apache.pig.backend.hadoop.executionengine.HExecutionEngine;
//import org.apache.pig.backend.hadoop.executionengine.mapreduceExec.MapReduceLauncher;
//import org.apache.pig.backend.hadoop.executionengine.mapreduceExec.PigMapReduce;
import org.apache.pig.backend.local.executionengine.LocalExecutionEngine;
import org.apache.pig.impl.logicalLayer.LogicalPlanBuilder;
import org.apache.pig.impl.mapReduceLayer.MapReduceLauncher;
import org.apache.pig.impl.util.JarManager;
import org.apache.pig.impl.util.WrappedIOException;

public class PigContext implements Serializable, FunctionInstantiator {
    private static final long serialVersionUID = 1L;
    
    private transient final Log log = LogFactory.getLog(getClass());
    
    private static final String JOB_NAME_PREFIX= "PigLatin";
    
    /* NOTE: we only serialize some of the stuff 
     * 
     *(to make it smaller given that it's not all needed on the Hadoop side, 
     * and also because some is not serializable e.g. the Configuration)
     */
    
    //one of: local, mapreduce, pigbody
    private ExecType execType;;    

    //  configuration for connecting to hadoop
    transient private Properties conf = new Properties();
    
    //  extra jar files that are needed to run a job
    transient public List<URL> extraJars = new LinkedList<URL>();              
    
    //  The jars that should not be merged in. (Some functions may come from pig.jar and we don't want the whole jar file.)
    transient public Vector<String> skipJars = new Vector<String>(2);    
    
    //main file system that jobs and shell commands access
    transient private DataStorage dfs;                         
    
    //  local file system, where jar files, etc. reside
    transient private DataStorage lfs;                         
    
    // handle to the back-end
    transient private ExecutionEngine executionEngine;
   
    private String jobName = JOB_NAME_PREFIX;    // can be overwritten by users
  
    /**
     * a table mapping function names to function specs.
     */
    private Map<String, String> definedFunctions = new HashMap<String, String>();
    
    private static ArrayList<String> packageImportList = new ArrayList<String>();

    public boolean                       debug       = true;
    
    public PigContext() {
        this(ExecType.MAPREDUCE);
    }
        
    public PigContext(ExecType execType){
        this.execType = execType;

        initProperties();
        String pigJar = JarManager.findContainingJar(Main.class);
        String hadoopJar = JarManager.findContainingJar(FileSystem.class);
        if (pigJar != null) {
            skipJars.add(pigJar);
            if (!pigJar.equals(hadoopJar))
                skipJars.add(hadoopJar);
        }
        
        executionEngine = null;
    }

    static{
        packageImportList.add("");
        packageImportList.add("org.apache.pig.builtin.");
        packageImportList.add("com.yahoo.pig.yst.sds.ULT.");
        packageImportList.add("org.apache.pig.impl.builtin.");        
    }

    private void initProperties() {
        Properties fileProperties = new Properties();
            
        try{        
            // first read the properties in the jar file
            InputStream pis = MapReduceLauncher.class.getClassLoader().getResourceAsStream("properties");
            if (pis != null) {
                fileProperties.load(pis);
            }
            
            //then read the properties in the home directory
            try{
                pis = new FileInputStream(System.getProperty("user.home") + "/.pigrc");
            }catch(IOException e){}
            if (pis != null) {
                fileProperties.load(pis);
            }
        }catch (IOException e){
            log.error(e);
            throw new RuntimeException(e);
        }
        
        //Now set these as system properties only if they are not already defined.
        for (Object o: fileProperties.keySet()){
            String propertyName = (String)o;
            log.debug("Found system property " + propertyName + " in .pigrc"); 
            if (System.getProperty(propertyName) == null){
                System.setProperty(propertyName, fileProperties.getProperty(propertyName));
                log.debug("Setting system property " + propertyName);
            }
        }
    }    
    
    public void connect() throws ExecException {
        try {
            switch (execType) {

            case LOCAL:
            {
                lfs = new HDataStorage(URI.create("file:///"),
                                       new Configuration());
                
                dfs = lfs;
                executionEngine = new LocalExecutionEngine(this);
            }
            break;

            case MAPREDUCE:
            {
                executionEngine = new HExecutionEngine (this);

                executionEngine.init();
                
                dfs = executionEngine.getDataStorage();
                
                lfs = new HDataStorage(URI.create("file:///"),
                        new Configuration());                
            }
            break;
            
            default:
            {
                throw new ExecException("Unkown execType: " + execType);
            }
            }
        }
        catch (IOException e) {
            ;
        }
    }

    public void setJobName(String name){
    jobName = JOB_NAME_PREFIX + ":" + name;
    }

    public String getJobName(){
    return jobName;
    }

    public void setJobtrackerLocation(String newLocation) {
        Properties trackerLocation = new Properties();
        trackerLocation.setProperty("mapred.job.tracker", newLocation);
        
        try {
            executionEngine.updateConfiguration(trackerLocation);
        }
        catch (ExecException e) {
            log.error("Failed to set tracker at: " + newLocation);
        }
    }
    
    public void addJar(String path) throws MalformedURLException {
        if (path != null) {
            URL resource = (new File(path)).toURI().toURL();
            addJar(resource);
        }
    }
    
    public void addJar(URL resource) throws MalformedURLException{
        if (resource != null) {
            extraJars.add(resource);
            LogicalPlanBuilder.classloader = createCl(null);
        }
    }

    public void rename(String oldName, String newName) throws IOException {
        if (oldName.equals(newName)) {
            return;
        }
        
        System.out.println("Renaming " + oldName + " to " + newName);

        ElementDescriptor dst = null;
        ElementDescriptor src = null;            

        try {
            dst = dfs.asElement(newName);
            src = dfs.asElement(oldName);            
        }
        catch (DataStorageException e) {
            throw WrappedIOException.wrap("Unable to rename " + oldName + " to " + newName, e);
        }

        if (dst.exists()) {
            dst.delete();
        }
        
        src.rename(dst);

    }

    public void copy(String src, String dst, boolean localDst) throws IOException {
        DataStorage dstStorage = dfs;
        
        if (localDst) {
            dstStorage = lfs;
        }
        
        ElementDescriptor srcElement = null;
        ElementDescriptor dstElement = null;

        try {
            srcElement = dfs.asElement(src);
            dstElement = dstStorage.asElement(dst);
        }
        catch (DataStorageException e) {
            throw WrappedIOException.wrap("Unable to copy " + src + " to " + dst + (localDst ? "locally" : ""), e);
        }
        
        srcElement.copy(dstElement, conf,false);
    }
    
    public ExecutionEngine getExecutionEngine() {
        return executionEngine;
    }

    public DataStorage getDfs() {
        return dfs;
    }

    public DataStorage getLfs() {
        return lfs;
    }

    public Properties getConf() {
        return conf;
    }

    /**
     * Defines an alias for the given function spec. This
     * is useful for functions that require arguments to the 
     * constructor.
     * 
     * @param function - the new function alias to define.
     * @param functionSpec - the name of the function and any arguments.
     * It should have the form: classname('arg1', 'arg2', ...)
     */
    public void registerFunction(String function, String functionSpec) {
        if (functionSpec == null) {
            definedFunctions.remove(function);
        } else {
            definedFunctions.put(function, functionSpec);
        }
    }

    /**
     * Returns the type of execution currently in effect.
     * 
     * @return current execution type
     */
    public ExecType getExecType() {
        return execType;
    }
    
    
    
    

    /**
     * Creates a Classloader based on the passed jarFile and any extra jar files.
     * 
     * @param jarFile
     *            the jar file to be part of the newly created Classloader. This jar file plus any
     *            jars in the extraJars list will constitute the classpath.
     * @return the new Classloader.
     * @throws MalformedURLException
     */
    public ClassLoader createCl(String jarFile) throws MalformedURLException {
        int len = extraJars.size();
        int passedJar = jarFile == null ? 0 : 1;
        URL urls[] = new URL[len + passedJar];
        if (jarFile != null) {
            urls[0] = new URL("file:" + jarFile);
        }
        for (int i = 0; i < extraJars.size(); i++) {
            urls[i + passedJar] = extraJars.get(i);
        }
        //return new URLClassLoader(urls, PigMapReduce.class.getClassLoader());
        return new URLClassLoader(urls, PigContext.class.getClassLoader());
    }
    
    public static String getClassNameFromSpec(String funcSpec){
        int paren = funcSpec.indexOf('(');
        if (paren!=-1)
            return funcSpec.substring(0, paren);
        else
            return funcSpec;
    }

    private static String getArgStringFromSpec(String funcSpec){
        int paren = funcSpec.indexOf('(');
        if (paren!=-1)
            return funcSpec.substring(paren+1);
        else
            return "";
    }
    

    public static Class resolveClassName(String name) throws IOException{
        for(String prefix: packageImportList) {
            Class c;
        try {
                c = Class.forName(prefix+name,true, LogicalPlanBuilder.classloader);
                return c;
            } catch (ClassNotFoundException e) {
            } catch (LinkageError e) {}
        }

        // create ClassNotFoundException exception and attach to IOException
        // so that we don't need to buble interface changes throughout the code
        ClassNotFoundException e = new ClassNotFoundException("Could not resolve " + name + " using imports: " + packageImportList);
        throw WrappedIOException.wrap(e.getMessage(), e);
    }
    
    private static List<String> parseArguments(String argString){
        List<String> args = new ArrayList<String>();
        
        int startIndex = 0;
        int endIndex;
        while (startIndex < argString.length()) {
            while (startIndex < argString.length() && argString.charAt(startIndex++) != '\'')
                ;
            endIndex = startIndex;
            while (endIndex < argString.length() && argString.charAt(endIndex) != '\'') {
                if (argString.charAt(endIndex) == '\\')
                    endIndex++;
                endIndex++;
            }
               if (endIndex < argString.length()) {
                   args.add(argString.substring(startIndex, endIndex));
            }
            startIndex = endIndex + 1;
        }
        return args;
    }
    
    @SuppressWarnings("unchecked")
    private static Object instantiateFunc(String className, String argString)  {
        Object ret;
        List<String> args = parseArguments(argString);
        try{
            Class objClass = resolveClassName(className);
            if (args != null && args.size() > 0) {
                Class paramTypes[] = new Class[args.size()];
                for (int i = 0; i < paramTypes.length; i++) {
                    paramTypes[i] = String.class;
                }
                Constructor c = objClass.getConstructor(paramTypes);
                ret =  c.newInstance(args.toArray());
            } else {
                ret = objClass.newInstance();
            }
        }catch(Throwable e){
            throw new RuntimeException("could not instantiate '" + className
                    + "' with arguments '" + args + "'", e);
        }
        return ret;
    }
    
    public static Object instantiateFuncFromSpec(String funcSpec) {
        return instantiateFunc(getClassNameFromSpec(funcSpec), getArgStringFromSpec(funcSpec));
    }
    
    
    public Class getClassForAlias(String alias) throws IOException{
        String className, funcSpec = null;
        if (definedFunctions != null) {
            funcSpec = definedFunctions.get(alias);
        }
        if (funcSpec != null) {
            className = getClassNameFromSpec(funcSpec);
        }else{
            className = getClassNameFromSpec(alias);
        }
        return resolveClassName(className);
    }
  
    public Object instantiateFuncFromAlias(String alias) throws IOException {
        String funcSpec;
        if (definedFunctions != null && (funcSpec = definedFunctions.get(alias))!=null)
            return instantiateFuncFromSpec(funcSpec);
        else
            return instantiateFuncFromSpec(alias);
    }

    public void setExecType(ExecType execType) {
        this.execType = execType;
    }

    public String getFuncSpecFromAlias(String alias) {
        String funcSpec;
        if (definedFunctions != null && (funcSpec = definedFunctions.get(alias))!=null)
            return funcSpec;
        else
            return null;
    }
}
