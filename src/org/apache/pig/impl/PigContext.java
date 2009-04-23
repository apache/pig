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
import java.util.Collections;
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
import org.apache.pig.FuncSpec;
import org.apache.pig.Main;
import org.apache.pig.ExecType;
import org.apache.pig.PigException;
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
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceLauncher;
import org.apache.pig.backend.hadoop.streaming.HadoopExecutableManager;
import org.apache.pig.impl.logicalLayer.LogicalPlanBuilder;
import org.apache.pig.impl.streaming.ExecutableManager;
import org.apache.pig.impl.streaming.StreamingCommand;
import org.apache.pig.impl.util.JarManager;
import org.apache.pig.impl.util.WrappedIOException;

public class PigContext implements Serializable, FunctionInstantiator {
    private static final long serialVersionUID = 1L;
    
    private transient final Log log = LogFactory.getLog(getClass());
    
    public static final String JOB_NAME = "jobName";
    public static final String JOB_NAME_PREFIX= "PigLatin";
    
    /* NOTE: we only serialize some of the stuff 
     * 
     *(to make it smaller given that it's not all needed on the Hadoop side, 
     * and also because some is not serializable e.g. the Configuration)
     */
    
    //one of: local, mapreduce, pigbody
    private ExecType execType;;    

    //  configuration for connecting to hadoop
    private Properties conf = new Properties();
    
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
  
    private Properties properties;
    
    /**
     * a table mapping function names to function specs.
     */
    private Map<String, FuncSpec> definedFunctions = new HashMap<String, FuncSpec>();
    
    /**
     * a table mapping names to streaming commands.
     */
    private Map<String, StreamingCommand> definedCommands = 
        new HashMap<String, StreamingCommand>();

    private static ArrayList<String> packageImportList = new ArrayList<String>();

    public boolean debug = true;

    // Says, wether we're processing an explain right now. Explain
    // might skip some check in the logical plan validation (file
    // existence checks, etc).
    public boolean inExplain = false;
    
    private String last_alias = null;

    // List of paths skipped for automatic shipping
    List<String> skippedShipPaths = new ArrayList<String>();
    
    public PigContext() {
        this(ExecType.MAPREDUCE, new Properties());
    }
        
    public PigContext(ExecType execType, Properties properties){
        this.execType = execType;
        this.properties = properties;   

        String pigJar = JarManager.findContainingJar(Main.class);
        String hadoopJar = JarManager.findContainingJar(FileSystem.class);
        if (pigJar != null) {
            skipJars.add(pigJar);
            if (!pigJar.equals(hadoopJar))
                skipJars.add(hadoopJar);
        }
        
        executionEngine = null;
        
        // Add the default paths to be skipped for auto-shipping of commands
        skippedShipPaths.add("/bin");
        skippedShipPaths.add("/usr/bin");
        skippedShipPaths.add("/usr/local/bin");
        skippedShipPaths.add("/sbin");
        skippedShipPaths.add("/usr/sbin");
        skippedShipPaths.add("/usr/local/sbin");
    }

    static{
        packageImportList.add("");
        packageImportList.add("org.apache.pig.builtin.");
        packageImportList.add("com.yahoo.pig.yst.sds.ULT.");
        packageImportList.add("org.apache.pig.impl.builtin.");        
    }
    
    public void connect() throws ExecException {

        switch (execType) {
            case LOCAL:
            {
                lfs = new HDataStorage(URI.create("file:///"),
                                       new Properties());
                
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
                                        new Properties());                
            }
            break;
            
            default:
            {
                int errCode = 2040;
                String msg = "Unkown exec type: " + execType;
                throw new ExecException(msg, errCode, PigException.BUG);
            }
        }

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
            byte errSrc = getErrorSource();            
            int errCode = 0;
            switch(errSrc) {
            case PigException.REMOTE_ENVIRONMENT:
                errCode = 6005;
                break;
            case PigException.USER_ENVIRONMENT:
                errCode = 4005;
                break;
            default:
                errCode = 2038;
                    break;
            }
            String msg = "Unable to rename " + oldName + " to " + newName;            
            throw new ExecException(msg, errCode, errSrc, e);
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
            byte errSrc = getErrorSource();            
            int errCode = 0;
            switch(errSrc) {
            case PigException.REMOTE_ENVIRONMENT:
                errCode = 6006;
                break;
            case PigException.USER_ENVIRONMENT:
                errCode = 4006;
                break;
            default:
                errCode = 2039;
                    break;
            }
            String msg = "Unable to copy " + src + " to " + dst;            
            throw new ExecException(msg, errCode, errSrc, e);
        }
        
        srcElement.copy(dstElement, this.properties, false);
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

    public DataStorage getFs() {
        if(execType == ExecType.LOCAL) {
            return lfs;
        } else {
            return dfs;
        }
    }
    
    /**
     * Provides configuration information.
     * 
     * @return - information about the configuration used to connect to
     *         execution engine
     */
    public Properties getProperties() {
        return this.properties;
    }
    
    /**
     * @deprecated use {@link #getProperties()} instead
     */
    public Properties getConf() {
        return getProperties();
    }

    public String getLastAlias() {
      return this.last_alias;
    }

    public void setLastAlias(String value) {
      this.last_alias = value;
    }

    /**
     * Defines an alias for the given function spec. This
     * is useful for functions that require arguments to the 
     * constructor.
     * 
     * @param function - the new function alias to define.
     * @param functionSpec - the FuncSpec object representing the name of 
     * the function class and any arguments to constructor.
     * 
     */
    public void registerFunction(String function, FuncSpec functionSpec) {
        if (functionSpec == null) {
            definedFunctions.remove(function);
        } else {
            definedFunctions.put(function, functionSpec);
        }
    }

    /**
     * Defines an alias for the given streaming command.
     * 
     * This is useful for complicated streaming command specs.
     * 
     * @param alias - the new command alias to define.
     * @param command - the command 
     */
    public void registerStreamCmd(String alias, StreamingCommand command) {
        if (command == null) {
            definedCommands.remove(alias);
        } else {
            definedCommands.put(alias, command);
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
    
    
    public static Class resolveClassName(String name) throws IOException{

        for(String prefix: packageImportList) {
            Class c;
            try {
                c = Class.forName(prefix+name,true, LogicalPlanBuilder.classloader);
                return c;
            } 
            catch (ClassNotFoundException e) {
                // do nothing
            } 
            catch (UnsupportedClassVersionError e) {
                int errCode = 1069;
                String msg = "Problem resolving class version numbers for class " + name;
                throw new ExecException(msg, errCode, PigException.INPUT, e) ;
            }
            
        }

        // create ClassNotFoundException exception and attach to IOException
        // so that we don't need to buble interface changes throughout the code
        int errCode = 1070;
        String msg = "Could not resolve " + name + " using imports: " + packageImportList;
        throw new ExecException(msg, errCode, PigException.INPUT);
    }
    
    
    @SuppressWarnings("unchecked")
    public static Object instantiateFuncFromSpec(FuncSpec funcSpec)  {
        Object ret;
        String className =funcSpec.getClassName(); 
        String[] args = funcSpec.getCtorArgs();
        Class objClass = null ;

        try {
            objClass = resolveClassName(className);
        }
        catch(IOException ioe) {
            throw new RuntimeException("Cannot instantiate:" + className, ioe) ;
        }

        try {
            // Do normal instantiation
            if (args != null && args.length > 0) {
                Class paramTypes[] = new Class[args.length];
                for (int i = 0; i < paramTypes.length; i++) {
                    paramTypes[i] = String.class;
                }
                Constructor c = objClass.getConstructor(paramTypes);
                ret =  c.newInstance((Object[])args);
            } else {
                ret = objClass.newInstance();
            }
        }
        catch(NoSuchMethodException nme) {
            // Second chance. Try with var arg constructor
            try {
                Constructor c = objClass.getConstructor(String[].class);
                Object[] wrappedArgs = new Object[1] ;
                wrappedArgs[0] = args ;
                ret =  c.newInstance(wrappedArgs);
            }
            catch(Throwable e){
                // bad luck
                StringBuilder sb = new StringBuilder();
                sb.append("could not instantiate '");
                sb.append(className);
                sb.append("' with arguments '");
                sb.append(args);
                sb.append("'");
                throw new RuntimeException(sb.toString(), e);
            }
        }
        catch(Throwable e){
            // bad luck
            StringBuilder sb = new StringBuilder();
            sb.append("could not instantiate '");
            sb.append(className);
            sb.append("' with arguments '");
            sb.append(args);
            sb.append("'");
            throw new RuntimeException(sb.toString(), e);
        }
        return ret;
    }
    
    public static Object instantiateFuncFromSpec(String funcSpec)  {
        return instantiateFuncFromSpec(new FuncSpec(funcSpec));
    }
    
    
    public Class getClassForAlias(String alias) throws IOException{
        String className = null;
        FuncSpec funcSpec = null;
        if (definedFunctions != null) {
            funcSpec = definedFunctions.get(alias);
        }
        if (funcSpec != null) {
            className = funcSpec.getClassName();
        }else{
            className = FuncSpec.getClassNameFromSpec(alias);
        }
        return resolveClassName(className);
    }
  
    public Object instantiateFuncFromAlias(String alias) throws IOException {
        FuncSpec funcSpec;
        if (definedFunctions != null && (funcSpec = definedFunctions.get(alias))!=null)
            return instantiateFuncFromSpec(funcSpec);
        else
            return instantiateFuncFromSpec(alias);
    }

    /**
     * Get the {@link StreamingCommand} for the given alias.
     * 
     * @param alias the alias for the <code>StreamingCommand</code>
     * @return <code>StreamingCommand</code> for the alias
     */
    public StreamingCommand getCommandForAlias(String alias) {
        return definedCommands.get(alias);
    }
    
    public void setExecType(ExecType execType) {
        this.execType = execType;
    }
    
    /**
     * Create a new {@link ExecutableManager} depending on the ExecType.
     * 
     * @return a new {@link ExecutableManager} depending on the ExecType
     * @throws ExecException
     */
    public ExecutableManager createExecutableManager() throws ExecException {
        ExecutableManager executableManager = null;

        switch (execType) {
            case LOCAL:
            {
                executableManager = new ExecutableManager();
            }
            break;
            case MAPREDUCE: 
            {
                executableManager = new HadoopExecutableManager();
            }
            break;
            default:
            {
                int errCode = 2040;
                String msg = "Unkown exec type: " + execType;
                throw new ExecException(msg, errCode, PigException.BUG);
            }
        }
        
        return executableManager;
    }

    public FuncSpec getFuncSpecFromAlias(String alias) {
        FuncSpec funcSpec;
        if (definedFunctions != null && (funcSpec = definedFunctions.get(alias))!=null)
            return funcSpec;
        else
            return null;
    }

    /**
     * Add a path to be skipped while automatically shipping binaries for 
     * streaming.
     *  
     * @param path path to be skipped
     */
    public void addPathToSkip(String path) {
        skippedShipPaths.add(path);
    }
    
    /**
     * Get paths which are to skipped while automatically shipping binaries for
     * streaming.
     * 
     * @return paths which are to skipped while automatically shipping binaries 
     *         for streaming
     */
    public List<String> getPathsToSkip() {
        return skippedShipPaths;
    }
    
    /**
     * Check the execution mode and return the appropriate error source
     * 
     * @return error source
     */
    public byte getErrorSource() {
        if(execType == ExecType.LOCAL) {
            return PigException.USER_ENVIRONMENT;
        } else if (execType == ExecType.MAPREDUCE) {
            return PigException.REMOTE_ENVIRONMENT;
        } else {
            return PigException.BUG;
        }        
    }
}
