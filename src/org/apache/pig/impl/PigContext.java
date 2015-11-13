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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.Vector;

import org.antlr.runtime.tree.Tree;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Level;
import org.apache.pig.ExecType;
import org.apache.pig.ExecTypeProvider;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigException;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.datastorage.DataStorageException;
import org.apache.pig.backend.datastorage.ElementDescriptor;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecutionEngine;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.datastorage.HDataStorage;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRConfiguration;
import org.apache.pig.impl.streaming.ExecutableManager;
import org.apache.pig.impl.streaming.StreamingCommand;
import org.apache.pig.tools.parameters.ParameterSubstitutionPreprocessor;
import org.apache.pig.tools.parameters.ParseException;
import org.apache.pig.tools.parameters.PreprocessorContext;

public class PigContext implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final Log log = LogFactory.getLog(PigContext.class);

    public static final String JOB_NAME = "jobName";
    public static final String JOB_NAME_PREFIX= "PigLatin";
    public static final String JOB_PRIORITY = "jobPriority";
    public static final String PIG_CMD_ARGS_REMAINDERS = "pig.cmd.args.remainders";


    /* NOTE: we only serialize some of the stuff
     *
     *(to make it smaller given that it's not all needed on the Hadoop side,
     * and also because some is not serializable e.g. the Configuration)
     */

    //one of: local, mapreduce, or a custom exec type for a different execution engine
    private ExecType execType;

    //main file system that jobs and shell commands access
    transient private DataStorage dfs;

    //  local file system, where jar files, etc. reside
    transient private DataStorage lfs;

    // handle to the back-end
    transient private ExecutionEngine executionEngine;

    private Properties properties;

    /*
     * Resources for the job (jars, scripting udf files, cached macro abstract syntax trees)
     */

    // Jar files that are global to the whole Pig script, includes
    // 1. registered jars
    // 2. Jars defined in -Dpig.additional.jars
    transient public List<URL> extraJars = new LinkedList<URL>();

    // original paths each extra jar came from
    // used to avoid redundant imports
    transient private Map<URL, String> extraJarOriginalPaths = new HashMap<URL, String>();

    // jars needed for scripting udfs - jython.jar etc
    transient public List<String> scriptJars = new ArrayList<String>(2);

    // jars that are predeployed to the cluster and thus should not be merged in at all (even subsets).
    transient public Vector<String> predeployedJars = new Vector<String>(2);

    // script files that are needed to run a job
    @Deprecated
    public List<String> scriptFiles = new ArrayList<String>();
    private Map<String,File> aliasedScriptFiles = new LinkedHashMap<String,File>();

    // record of scripting udf file path --> which namespace it was registered to
    // used to avoid redundant imports
    transient public Map<String, String> scriptingUDFs;

    // cache of macro file path --> abstract syntax tree
    // used to avoid re-parsing the same macros over and over
    transient public Map<String, Tree> macros;

    /**
     * a table mapping function names to function specs.
     */
    private Map<String, FuncSpec> definedFunctions = new HashMap<String, FuncSpec>();

    /**
     * a table mapping names to streaming commands.
     */
    private Map<String, StreamingCommand> definedCommands =
        new HashMap<String, StreamingCommand>();

    private static ThreadLocal<ArrayList<String>> packageImportList =
        new ThreadLocal<ArrayList<String>>();

    private static ThreadLocal<Map<String,Class<?>>> classCache =
        new ThreadLocal<Map<String,Class<?>>>();

    private Properties log4jProperties = new Properties();

    private Level defaultLogLevel = Level.INFO;

    public int defaultParallel = -1;

    // Says, whether we're processing an explain right now. Explain
    // might skip some check in the logical plan validation (file
    // existence checks, etc).
    public boolean inExplain = false;

    // Where we are processing a dump schema right now
    public boolean inDumpSchema = false;

    // whether we're processing an ILLUSTRATE right now.
    public boolean inIllustrator = false;

    private String last_alias = null;

    // List of paths skipped for automatic shipping
    List<String> skippedShipPaths = new ArrayList<String>();

    //@StaticDataCleanup
    public static void staticDataCleanup() {
        packageImportList.set(null);
    }

    /**
     * extends URLClassLoader to allow adding to classpath as new jars
     * are registered.
     */
    private static class ContextClassLoader extends URLClassLoader {

        public ContextClassLoader(ClassLoader classLoader) {
            this(new URL[0], classLoader);
        }

        public ContextClassLoader(URL[] urls, ClassLoader classLoader) {
            super(urls, classLoader);
        }

        @Override
        public void addURL(URL url) {
            super.addURL(url);
        }
    };

    static private ContextClassLoader classloader = new ContextClassLoader(PigContext.class.getClassLoader());

    /*
     * Parameter-related fields
     * params: list of strings "key=value" from the command line
     * paramFiles: list of paths to parameter files
     * preprocessorContext: manages parsing params and paramFiles into an actual map
     */

    private List<String> params;
    private List<String> paramFiles;
    transient private PreprocessorContext preprocessorContext = new PreprocessorContext(50);

    public List<String> getParams() {
        return params;
    }
    public void setParams(List<String> params) {
        this.params = params;
    }

    public List<String> getParamFiles() {
        return paramFiles;
    }
    public void setParamFiles(List<String> paramFiles) {
        this.paramFiles = paramFiles;
    }

    public PreprocessorContext getPreprocessorContext() {
        return preprocessorContext;
    }

    public Map<String, String> getParamVal() throws IOException {
        Map<String, String> paramVal = preprocessorContext.getParamVal();
        if (paramVal == null) {
            try {
                preprocessorContext.loadParamVal(params, paramFiles);
            } catch (ParseException e) {
                throw new IOException(e.getMessage());
            }
            return preprocessorContext.getParamVal();
        } else {
            return paramVal;
        }
    }

    public PigContext() {
        this(ExecType.MAPREDUCE, new Properties());
    }

    public PigContext(Configuration conf) throws PigException {
        this(ConfigurationUtil.toProperties(conf));
    }

    public PigContext(Properties properties) throws PigException {
        this(ExecTypeProvider.selectExecType(properties), properties);
    }

    public PigContext(ExecType execType, Configuration conf) {
        this(execType, ConfigurationUtil.toProperties(conf));
    }

    public PigContext(ExecType execType, Properties properties){
        this.execType = execType;
        this.properties = properties;

        this.properties.setProperty("exectype", this.execType.name());

        this.executionEngine = execType.getExecutionEngine(this);

        // Add the default paths to be skipped for auto-shipping of commands
        skippedShipPaths.add("/bin");
        skippedShipPaths.add("/usr/bin");
        skippedShipPaths.add("/usr/local/bin");
        skippedShipPaths.add("/sbin");
        skippedShipPaths.add("/usr/sbin");
        skippedShipPaths.add("/usr/local/sbin");

        macros = new HashMap<String, Tree>();
        scriptingUDFs = new HashMap<String, String>();

        init();
    }

    /**
     * This method is created with the aim of unifying the Grunt and PigServer
     * approaches, so all common initializations can go in here.
     */
    private void init() {
        if (properties.get("udf.import.list")!=null)
            PigContext.initializeImportList((String)properties.get("udf.import.list"));
    }

    public static void initializeImportList(String importListCommandLineProperties)
    {
        StringTokenizer tokenizer = new StringTokenizer(importListCommandLineProperties, ":");
        int pos = 1; // Leave "" as the first import
        ArrayList<String> importList = getPackageImportList();
        while (tokenizer.hasMoreTokens())
        {
            String importItem = tokenizer.nextToken();
            if (!importItem.endsWith("."))
                importItem += ".";
            importList.add(pos, importItem);
            pos++;
        }
    }

    public void connect() throws ExecException {
        executionEngine.init();
        dfs = executionEngine.getDataStorage();
        lfs = new HDataStorage(URI.create("file:///"), properties);
    }

    public void setJobtrackerLocation(String newLocation) {
        executionEngine.setProperty(MRConfiguration.JOB_TRACKER, newLocation);
    }

    /**
     * calls: addScriptFile(path, new File(path)), ensuring that a given path is
     * added to the jar at most once.
     * @param path
     */
    public void addScriptFile(String path) {
        addScriptFile(path, path);
    }

    /**
     * this method adds script files that must be added to the shipped jar
     * named differently from their local fs path.
     * @param name  name in the jar
     * @param path  path on the local fs
     */
    public void addScriptFile(String name, String path) {
        if (path != null) {
            aliasedScriptFiles.put(name.replaceFirst("^/", "").replaceAll(":", ""), new File(path));
        }
    }

    public void addScriptJar(String path) {
        if (path != null && !scriptJars.contains(path)) {
            scriptJars.add(path);
        }
    }

    public void addJar(String path) throws MalformedURLException {
        if (path != null) {
            URL resource = (new File(path)).toURI().toURL();
            addJar(resource, path);
        }
    }

    public void addJar(URL resource, String originalPath) throws MalformedURLException{
        if (resource != null && !extraJars.contains(resource)) {
            extraJars.add(resource);
            extraJarOriginalPaths.put(resource, originalPath);
            classloader.addURL(resource);
            Thread.currentThread().setContextClassLoader(PigContext.classloader);
        }
    }

    public boolean hasJar(String path) {
        for (URL url : extraJars) {
            if (extraJarOriginalPaths.get(url).equals(path)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Adds the specified path to the predeployed jars list. These jars will
     * never be included in generated job jar.
     * <p>
     * This can be called for jars that are pre-installed on the Hadoop
     * cluster to reduce the size of the job jar.
     */
    public void markJarAsPredeployed(String path) {
        if (path != null && !predeployedJars.contains(path)) {
            predeployedJars.add(path);
        }
    }

    public String doParamSubstitution(InputStream in,
                                      List<String> params,
                                      List<String> paramFiles)
                                      throws IOException {

        return doParamSubstitution(new BufferedReader(new InputStreamReader(in)),
                                   params, paramFiles);
    }

    public String doParamSubstitution(BufferedReader reader,
                                      List<String> params,
                                      List<String> paramFiles)
                                      throws IOException {
        this.params = params;
        this.paramFiles = paramFiles;
        return doParamSubstitution(reader);
    }

    public String doParamSubstitution(BufferedReader reader) throws IOException {
        try {
            preprocessorContext.setPigContext(this);
            preprocessorContext.loadParamVal(params, paramFiles);
            ParameterSubstitutionPreprocessor psp
                = new ParameterSubstitutionPreprocessor(preprocessorContext);
            StringWriter writer = new StringWriter();
            psp.genSubstitutedFile(reader, writer);
            return writer.toString();
        } catch (ParseException e) {
            log.error(e.getLocalizedMessage());
            throw new IOException(e);
        }
    }

    public BufferedReader doParamSubstitutionOutputToFile(BufferedReader reader,
                                                  String outputFilePath,
                                                  List<String> params,
                                                  List<String> paramFiles)
                                                  throws IOException {
        this.params = params;
        this.paramFiles = paramFiles;
        return doParamSubstitutionOutputToFile(reader, outputFilePath);
    }

    public BufferedReader doParamSubstitutionOutputToFile(BufferedReader reader, String outputFilePath)
                          throws IOException {
        try {
            preprocessorContext.loadParamVal(params, paramFiles);
            ParameterSubstitutionPreprocessor psp
                    = new ParameterSubstitutionPreprocessor(preprocessorContext);
            BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilePath));
            psp.genSubstitutedFile(reader, writer);
            return new BufferedReader(new FileReader(outputFilePath));
        } catch (ParseException e) {
            log.error(e.getLocalizedMessage());
            throw new IOException(e);
        } catch (FileNotFoundException e) {
            throw new IOException("Could not find file to substitute parameters for: " + outputFilePath);
        }
    }

    /**
     * script files as name/file pairs to be added to the job jar
     * @return name/file pairs
     */
    public Map<String,File> getScriptFiles() {
        return aliasedScriptFiles;
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
        return dfs;
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
    @Deprecated
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
        return new ContextClassLoader(urls, PigContext.class.getClassLoader());
    }

    private static Map<String,Class<?>> getClassCache() {
        Map<String,Class<?>> c = classCache.get();
        if (c == null) {
            c = new HashMap<String,Class<?>>();
            classCache.set(c);
        }

        return c;
    }

    @SuppressWarnings("rawtypes")
    public static Class resolveClassName(String name) throws IOException{
        Map<String,Class<?>> cache = getClassCache();

        Class c = cache.get(name);
        if (c != null) {
            return c;
        }

        for(String prefix: getPackageImportList()) {
            try {
                c = Class.forName(prefix+name,true, PigContext.classloader);
                cache.put(name, c);

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
        String msg = "Could not resolve " + name + " using imports: " + packageImportList.get();
        throw new ExecException(msg, errCode, PigException.INPUT);
    }

    /**
     * A common Pig pattern for initializing objects via system properties is to support passing
     * something like this on the command line:
     * <code>-Dpig.notification.listener=MyClass</code>
     * <code>-Dpig.notification.listener.arg=myConstructorStringArg</code>
     *
     * This method will properly initialize the class with the args, if they exist.
     * @param conf
     * @param classParamKey the property used to identify the class
     * @param argParamKey the property used to identify the class args
     * @param clazz The class that is expected
     * @return <T> T
     */
    public static <T> T instantiateObjectFromParams(Configuration conf,
                                                    String classParamKey,
                                                    String argParamKey,
                                                    Class<T> clazz) throws ExecException {
      String className = conf.get(classParamKey);

      if (className != null) {
          FuncSpec fs;
          if (conf.get(argParamKey) != null) {
              fs = new FuncSpec(className, conf.get(argParamKey));
          } else {
              fs = new FuncSpec(className);
          }
          try {
            return clazz.cast(PigContext.instantiateFuncFromSpec(fs));
          }
          catch (ClassCastException e) {
              throw new ExecException("The class defined by " + classParamKey +
                      " in conf is not of type " + clazz.getName(), e);
          }
      } else {
          return null;
      }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static Object instantiateFuncFromSpec(FuncSpec funcSpec)  {
        Object ret;
        String className =funcSpec.getClassName();
        String[] args = funcSpec.getCtorArgs();
        Class objClass = null ;

        try {
            objClass = resolveClassName(className);
        }
        catch(IOException ioe) {
            throw new RuntimeException("Cannot instantiate: " + className, ioe) ;
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
                sb.append(Arrays.toString(args));
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
            sb.append(Arrays.toString(args));
            sb.append("'");
            throw new RuntimeException(sb.toString(), e);
        }
        return ret;
    }

    public static Object instantiateFuncFromSpec(String funcSpec)  {
        return instantiateFuncFromSpec(new FuncSpec(funcSpec));
    }

    @SuppressWarnings("rawtypes")
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
        if (executionEngine != null) {
            return executionEngine.getExecutableManager();
        }
        return null;
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
        return PigException.REMOTE_ENVIRONMENT;
    }

    public static ArrayList<String> getPackageImportList() {
        if (packageImportList.get() == null) {
            ArrayList<String> importlist = new ArrayList<String>();
            importlist.add("");
            importlist.add("java.lang.");
            importlist.add("org.apache.pig.builtin.");
            importlist.add("org.apache.pig.impl.builtin.");
            packageImportList.set(importlist);
        }
        return packageImportList.get();
    }

    public static void setPackageImportList(ArrayList<String> list) {
        packageImportList.set(list);
    }

    public void setLog4jProperties(Properties p)
    {
        log4jProperties = p;
    }
    public Properties getLog4jProperties()
    {
        return log4jProperties;
    }
    public Level getDefaultLogLevel()
    {
        return defaultLogLevel;
    }
    public void setDefaultLogLevel(Level l)
    {
        defaultLogLevel = l;
    }
    public static ClassLoader getClassLoader() {
        return classloader;
    }
    public static void setClassLoader(ClassLoader cl) {
        if (cl instanceof ContextClassLoader) {
            classloader = (ContextClassLoader) cl;
        } else {
            classloader = new ContextClassLoader(cl);
        }
    }
}
