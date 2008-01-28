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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.File;
import java.io.InputStream;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketImplFactory;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapred.JobSubmissionProtocol;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapred.JobClient;

import org.apache.log4j.Logger;
import org.apache.pig.Main;
import org.apache.pig.PigServer.ExecType;
import org.apache.pig.impl.logicalLayer.LogicalPlanBuilder;
import org.apache.pig.impl.mapreduceExec.MapReduceLauncher;
import org.apache.pig.impl.mapreduceExec.PigMapReduce;
import org.apache.pig.impl.util.JarManager;
import org.apache.pig.impl.util.PigLogger;
import org.apache.pig.shock.SSHSocketImplFactory;


public class PigContext implements Serializable, FunctionInstantiator {
	private static final long serialVersionUID = 1L;
	private static final String JOB_NAME_PREFIX= "PigLatin";
    
    /* NOTE: we only serialize some of the stuff 
     * 
     *(to make it smaller given that it's not all needed on the Hadoop side, 
     * and also because some is not serializable e.g. the Configuration)
     */
    
	//one of: local, mapreduce, pigbody
    private ExecType execType;;    

    //  configuration for connecting to hadoop
    transient private JobConf conf = null;        
    
    //  extra jar files that are needed to run a job
    transient public List<URL> extraJars = new LinkedList<URL>();              
    
    //  The jars that should not be merged in. (Some functions may come from pig.jar and we don't want the whole jar file.)
    transient public Vector<String> skipJars = new Vector<String>(2);    
    
    //main file system that jobs and shell commands access
    transient private FileSystem dfs;                         
    
    //  local file system, where jar files, etc. reside
    transient private FileSystem lfs;                         
    
    //  connection to hadoop jobtracker (stays as null if doing local execution)
    transient private JobSubmissionProtocol jobTracker;                  
    transient private JobClient jobClient;                  
   
    private String jobName = JOB_NAME_PREFIX;	// can be overwritten by users
  
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
    }

    static{
        packageImportList.add("");
        packageImportList.add("org.apache.pig.builtin.");
        packageImportList.add("com.yahoo.pig.yst.sds.ULT.");
        packageImportList.add("org.apache.pig.impl.builtin.");        
    }

	private void initProperties() {
        Logger log = PigLogger.getLogger();

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
	    	e.printStackTrace();
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
	
    public void connect(){
        Logger log = PigLogger.getLogger();
    	try{
		if (execType != ExecType.LOCAL){
		    	//First set the ssh socket factory
		    	setSSHFactory();
		    	
		    	String hodServer = System.getProperty("hod.server");
	    	
		    	if (execType == ExecType.MAPREDUCE && hodServer != null && hodServer.length() > 0) {
		    		String hdfsAndMapred[] = doHod(hodServer);
		    		setFilesystemLocation(hdfsAndMapred[0]);
		    		setJobtrackerLocation(hdfsAndMapred[1]);
		     	}
			else
			{
				conf = new JobConf();
        			String cluster = System.getProperty("cluster");
        			if (cluster!=null && cluster.length() > 0){
	        			if(cluster.indexOf(':') < 0) {
	            				cluster = cluster + ":50020";
	        			}
	        			setJobtrackerLocation(cluster);
        			}
    
        			String nameNode = System.getProperty("namenode");
        			if (nameNode!=null && nameNode.length() > 0){
        				if(nameNode.indexOf(':') < 0) {
        					nameNode = nameNode + ":8020";
        				}
        				setFilesystemLocation(nameNode);
        			}
			}
        	
		     
	            lfs = FileSystem.getNamed("local", conf);
	       
	            log.info("Connecting to hadoop file system at: " + conf.get("fs.default.name"));
	            dfs = FileSystem.get(conf);
	        
	            log.info("Connecting to map-reduce job tracker at: " + conf.get("mapred.job.tracker"));
	            jobTracker = (JobSubmissionProtocol) RPC.getProxy(JobSubmissionProtocol.class,
	            				JobSubmissionProtocol.versionID, JobTracker.getAddress(conf), conf);
		    jobClient = new JobClient(conf);
	        
	   }else{
		conf = new JobConf();
	    	lfs = FileSystem.getNamed("local", conf);
	        dfs = lfs;  // for local execution, the "dfs" is the local file system
	   }
    	}catch (IOException e){
    		throw new RuntimeException(e);
    	}
    }

    public void setJobName(String name){
	jobName = JOB_NAME_PREFIX + ":" + name;
    }

    public String getJobName(){
	return jobName;
    }

    //To prevent doing hod if the pig server is constructed multiple times
    private static String hodMapRed;
    private static String hodHDFS;
    enum ParsingState {
    	NOTHING, HDFSUI, MAPREDUI, HDFS, MAPRED, HADOOPCONF
    };
    
    private String[] doHod(String server) {
        Logger log = PigLogger.getLogger();
    	if (hodMapRed != null) {
    		return new String[] {hodHDFS, hodMapRed};
    	}
        
        try {
            Process p = null;
			// Make the kryptonite released version the default if nothing
			// else is specified.
            StringBuilder cmd = new StringBuilder();
			cmd.append(System.getProperty("hod.expect.root"));
			cmd.append('/');
			cmd.append("libexec/pig/");
			cmd.append(System.getProperty("hod.expect.uselatest"));
			cmd.append('/');
            cmd.append(System.getProperty("hod.command"));
            //String cmd = System.getProperty("hod.command", "/home/breed/startHOD.expect");
			String cluster = System.getProperty("yinst.cluster");
            // TODO This is a Yahoo specific holdover, need to remove
            // this.
			if (cluster != null && cluster.length() > 0 &&
                    !cluster.startsWith("kryptonite")) {
				cmd.append(" --config=");
				cmd.append(System.getProperty("hod.config.dir"));
				cmd.append('/');
				cmd.append(cluster);
			}
            cmd.append(" " + System.getProperty("hod.param", ""));
	    if (server.equals("local")) {
		p = Runtime.getRuntime().exec(cmd.toString());
            } else {
            	SSHSocketImplFactory fac = SSHSocketImplFactory.getFactory(server);
            	p = fac.ssh(cmd.toString());
            }
            InputStream is = p.getInputStream();
            log.info("Connecting to HOD...");
			log.debug("sending HOD command " + cmd.toString());
            StringBuffer sb = new StringBuffer();
            int c;
            String hdfsUI = null;
            String mapredUI = null;
            String hdfs = null;
            String mapred = null;
	    String hadoopConf = null;
            ParsingState current = ParsingState.NOTHING;
            while((c = is.read()) != -1 && mapred == null) {
                if (c == '\n' || c == '\r') {
                	switch(current) {
                	case HDFSUI:
                		hdfsUI = sb.toString().trim();
                		log.info("HDFS Web UI: " + hdfsUI);
                		break;
                	case HDFS:
                		hdfs = sb.toString().trim();
                		log.info("HDFS: " + hdfs);
                		break;
                	case MAPREDUI:
                		mapredUI = sb.toString().trim();
                		log.info("JobTracker Web UI: " + mapredUI);
                		break;
                	case MAPRED:
                		mapred = sb.toString().trim();
                		log.info("JobTracker: " + mapred);
                		break;
			case HADOOPCONF:
				hadoopConf = sb.toString().trim();
                		log.info("HadoopConf: " + hadoopConf);
                		break;
                	}
                	current = ParsingState.NOTHING;
                	sb = new StringBuffer();
                }
                sb.append((char)c);
                if (sb.indexOf("hdfsUI:") != -1) {
                    current = ParsingState.HDFSUI;
                    sb = new StringBuffer();
                } else if (sb.indexOf("hdfs:") != -1) {
                	current = ParsingState.HDFS;
                    sb = new StringBuffer();
                } else if (sb.indexOf("mapredUI:") != -1) {
                	current = ParsingState.MAPREDUI;
                    sb = new StringBuffer();
                } else if (sb.indexOf("mapred:") != -1) {
                	current = ParsingState.MAPRED;
                    sb = new StringBuffer();
                } else if (sb.indexOf("hadoopConf:") != -1) {
		    current = ParsingState.HADOOPCONF;
                    sb = new StringBuffer();
		}
	
            }
            hdfsUI = fixUpDomain(hdfsUI);
            hdfs = fixUpDomain(hdfs);
            mapredUI = fixUpDomain(mapredUI);
            mapred = fixUpDomain(mapred);
            hodHDFS = hdfs;
            hodMapRed = mapred;

	    if (hadoopConf != null)
	    {
		conf = new JobConf(hadoopConf);
		// make sure that files on class path are used
		conf.addResource("pig-cluster-hadoop-site.xml");
		System.out.println("Job Conf = " + conf);
		System.out.println("dfs.block.size= " + conf.get("dfs.block.size"));
		System.out.println("ipc.client.timeout= " + conf.get("ipc.client.timeout"));
		System.out.println("mapred.child.java.opts= " + conf.get("mapred.child.java.opts"));
	    }
	    else
		throw new IOException("Missing Hadoop configuration file");
            return new String[] {hdfs, mapred};
        } catch (Exception e) {
            log.fatal("Could not connect to HOD", e);
            System.exit(4);
        }
        throw new RuntimeException("Could not scrape needed information.");
    }
    
    /**
     * Transform from "hostname:port" to "ip:port".
     * @param hostPort "hostname:port"
     * @throws IllegalArgumentException if hostPort doesn't match pattern 'host:port'
     * @return "ip:port"
     */
    private String fixUpDomain(String hostPort) throws UnknownHostException {
      if (hostPort == null) {
        return null;
      }
        String parts[] = hostPort.split(":");
        if (parts.length < 2) {
          throw new IllegalArgumentException("Invalid 'host:port' parameter was: " + hostPort);
        }
        if (parts[0].indexOf('.') == -1) {
          // FIXME don't hardcode "inktomisearch" strings
            parts[0] = parts[0] + ".inktomisearch.com";
        }
        InetAddress.getByName(parts[0]);
        return parts[0] + ":" + parts[1];
    }

    private void setSSHFactory(){
		String g = System.getProperty("ssh.gateway");
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
        if (oldName.equals(newName)) return;
        System.out.println("Renaming " + oldName + " to " + newName);
        Path dst = new Path(newName);
        if (dfs.exists(dst))
            dfs.delete(dst);
        dfs.rename(new Path(oldName), dst);
    }

    public void copy(String src, String dst, boolean localDst) throws IOException {
        FileUtil.copy(dfs, new Path(src), (localDst ? lfs : dfs), new Path(dst), false, conf);
    }
    

    public FileSystem getDfs() {
        return dfs;
    }

    public FileSystem getLfs() {
        return lfs;
    }

    public JobSubmissionProtocol getJobTracker() {
        return jobTracker;
    }
    public JobClient getJobClient() {
        return jobClient;
    }

    public JobConf getConf() {
        return conf;
    }

    public void setJobtrackerLocation(String newLocation) {
        conf.set("mapred.job.tracker", newLocation);
    }

    public void setFilesystemLocation(String newLocation) {
        conf.set("fs.default.name", newLocation);
    }
    
    /**
     * Defines an alias for the given function spec. This
     * is useful for functions that require arguments to the 
     * constructor.
     * 
     * @param aliases - the new function alias to define.
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
     * @return
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
        return new URLClassLoader(urls, PigMapReduce.class.getClassLoader());
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
		IOException newE = new IOException(e.getMessage());
                newE.initCause(e);
                throw newE;
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
	private static Object instantiateFunc(String className, String argString) throws IOException {
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
    		IOException newE = new IOException(e.getMessage());
                newE.initCause(e);
                throw newE;
    	}
		return ret;
	}
    
    public static Object instantiateFuncFromSpec(String funcSpec) throws IOException{
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
			className = alias;
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
		
}
