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
package org.apache.pig;

import java.io.*;
import java.lang.reflect.Method;
import java.util.*;
import java.util.jar.*;
import java.text.ParseException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.PropertyConfigurator;
import org.apache.pig.PigServer.ExecType;
import org.apache.pig.backend.hadoop.executionengine.HExecutionEngine;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.LogicalPlanBuilder;
import org.apache.pig.impl.util.JarManager;
import org.apache.pig.impl.util.PropertiesUtil;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.tools.cmdline.CmdLineParser;
import org.apache.pig.tools.grunt.Grunt;
import org.apache.pig.tools.timer.PerformanceTimerFactory;
import org.apache.pig.tools.parameters.ParameterSubstitutionPreprocessor;

public class Main
{

    private final static Log log = LogFactory.getLog(Main.class);
    
    private static final String LOG4J_CONF = "log4jconf";
    private static final String BRIEF = "brief";
    private static final String DEBUG = "debug";
    private static final String JAR = "jar";
    private static final String VERBOSE = "verbose";
    
    private enum ExecMode {STRING, FILE, SHELL, UNKNOWN};
                
/**
 * The Main-Class for the Pig Jar that will provide a shell and setup a classpath appropriate
 * for executing Jar files.
 * 
 * @param args
 *            -jar can be used to add additional jar files (colon separated). - will start a
 *            shell. -e will execute the rest of the command line as if it was input to the
 *            shell.
 * @throws IOException
 */
public static void main(String args[])
{
    int rc = 1;
    Properties properties = new Properties();
    PropertiesUtil.loadPropertiesFromFile(properties);

    try {
        BufferedReader pin = null;
        boolean debug = false;
        boolean dryrun = false;
        ArrayList<String> params = new ArrayList<String>();
        ArrayList<String> paramFiles = new ArrayList<String>();

        CmdLineParser opts = new CmdLineParser(args);
        opts.registerOpt('4', "log4jconf", CmdLineParser.ValueExpected.REQUIRED);
        opts.registerOpt('b', "brief", CmdLineParser.ValueExpected.NOT_ACCEPTED);
        opts.registerOpt('c', "cluster", CmdLineParser.ValueExpected.REQUIRED);
        opts.registerOpt('d', "debug", CmdLineParser.ValueExpected.REQUIRED);
        opts.registerOpt('e', "execute", CmdLineParser.ValueExpected.NOT_ACCEPTED);
        opts.registerOpt('f', "file", CmdLineParser.ValueExpected.REQUIRED);
        opts.registerOpt('h', "help", CmdLineParser.ValueExpected.NOT_ACCEPTED);
        opts.registerOpt('o', "hod", CmdLineParser.ValueExpected.NOT_ACCEPTED);
        opts.registerOpt('j', "jar", CmdLineParser.ValueExpected.REQUIRED);
        opts.registerOpt('v', "verbose", CmdLineParser.ValueExpected.NOT_ACCEPTED);
        opts.registerOpt('x', "exectype", CmdLineParser.ValueExpected.REQUIRED);
        opts.registerOpt('i', "version", CmdLineParser.ValueExpected.OPTIONAL);
        opts.registerOpt('p', "param", CmdLineParser.ValueExpected.OPTIONAL);
        opts.registerOpt('m', "param_file", CmdLineParser.ValueExpected.OPTIONAL);
        opts.registerOpt('r', "dryrun", CmdLineParser.ValueExpected.NOT_ACCEPTED);

        ExecMode mode = ExecMode.UNKNOWN;
        String file = null;
        ExecType execType = ExecType.MAPREDUCE ;
        String execTypeString = properties.getProperty("exectype");
        if(execTypeString!=null && execTypeString.length()>0){
            execType = PigServer.parseExecType(execTypeString);
        }
        String cluster = "local";
        String clusterConfigured = properties.getProperty("cluster");
        if(clusterConfigured != null && clusterConfigured.length() > 0){
            cluster = clusterConfigured;
        }

        char opt;
        while ((opt = opts.getNextOpt()) != CmdLineParser.EndOfOpts) {
            switch (opt) {
            case '4':
                String log4jconf = opts.getValStr();
                if(log4jconf != null){
                    properties.setProperty(LOG4J_CONF, log4jconf);
                }
                break;

            case 'b':
                properties.setProperty(BRIEF, "true");
                break;

            case 'c': 
                // Needed away to specify the cluster to run the MR job on
                // Bug 831708 - fixed
                String clusterParameter = opts.getValStr();
                if (clusterParameter != null && clusterParameter.length() > 0) {
                    cluster = clusterParameter;
                }
                break;

            case 'd':
                String logLevel = opts.getValStr();
                if (logLevel != null) {
                    properties.setProperty(DEBUG, logLevel);
                }
                debug = true;
                break;
                
            case 'e': 
                mode = ExecMode.STRING;
                break;

            case 'f':
                mode = ExecMode.FILE;
                file = opts.getValStr();
                break;

            case 'h':
                usage();
                return;

            case 'j': 
                String jarsString = opts.getValStr();
                if(jarsString != null){
                    properties.setProperty(JAR, jarsString);
                }
                break;

            case 'm':
                paramFiles.add(opts.getValStr());
                break;
                            
            case 'o': 
                // TODO sgroschupf using system properties is always a very bad idea
                String gateway = System.getProperty("ssh.gateway");
                if (gateway == null || gateway.length() == 0) {
                    properties.setProperty("hod.server", "local");
                } else {
                    properties.setProperty("hod.server", System.getProperty("ssh.gateway"));
                }
                break;

            case 'p': 
                String val = opts.getValStr();
                params.add(opts.getValStr());
                break;
                            
            case 'r': 
                // currently only used for parameter substitition
                // will be extended in the future
                dryrun = true;
                break;
                            
            case 'v':
                properties.setProperty(VERBOSE, ""+true);
                break;

            case 'x':
                try {
                    execType = PigServer.parseExecType(opts.getValStr());
                    } catch (IOException e) {
                        throw new RuntimeException("ERROR: Unrecognized exectype.", e);
                    }
                break;
            case 'i':
            	System.out.println(getVersionString());
            	return;
            default: {
                Character cc = new Character(opt);
                throw new AssertionError("Unhandled option " + cc.toString());
                     }
            }
        }
        // configure logging
        configureLog4J(properties);
        // create the context with the parameter
        PigContext pigContext = new PigContext(execType, properties);

        LogicalPlanBuilder.classloader = pigContext.createCl(null);

        // construct the parameter subsitution preprocessor
        Grunt grunt = null;
        BufferedReader in;
        String substFile = null;
        switch (mode) {
        case FILE:
            // Run, using the provided file as a pig file
            in = new BufferedReader(new FileReader(file));

            // run parameter substition preoprocessor first
            substFile = file + ".substituted";
            pin = runParamPreprocessor(in, params, paramFiles, substFile, debug || dryrun);
            if (dryrun){
                log.info("Dry run completed. Substitued pig script is at " + substFile);
                return;
            }

            if (!debug)
                new File(substFile).deleteOnExit();
            
            grunt = new Grunt(pin, pigContext);
            grunt.exec();
            return;

        case STRING: {
            // Gather up all the remaining arguments into a string and pass them into
            // grunt.
            StringBuilder sb = new StringBuilder();
            String remainders[] = opts.getRemainingArgs();
            for (int i = 0; i < remainders.length; i++) {
                if (i != 0) sb.append(' ');
                sb.append(remainders[i]);
            }
            in = new BufferedReader(new StringReader(sb.toString()));
            grunt = new Grunt(in, pigContext);
            grunt.exec();
            rc = 0;
            return;
            }

        default:
            break;
        }

        // If we're here, we don't know yet what they want.  They may have just
        // given us a jar to execute, they might have given us a pig script to
        // execute, or they might have given us a dash (or nothing) which means to
        // run grunt interactive.
        String remainders[] = opts.getRemainingArgs();
        if (remainders == null) {
            // Interactive
            mode = ExecMode.SHELL;
            in = new BufferedReader(new InputStreamReader(System.in));
            grunt = new Grunt(in, pigContext);
            grunt.run();
            rc = 0;
            return;
        } else {
            // They have a pig script they want us to run.
            if (remainders.length > 1) {
                   throw new RuntimeException("You can only run one pig script "
                    + "at a time from the command line.");
            }
            mode = ExecMode.FILE;
            in = new BufferedReader(new FileReader(remainders[0]));

            // run parameter substition preoprocessor first
            substFile = remainders[0] + ".substituted";
            pin = runParamPreprocessor(in, params, paramFiles, substFile, debug || dryrun);
            if (dryrun){
                log.info("Dry run completed. Substitued pig script is at " + substFile);
                return;
            }

            if (!debug)
                new File(substFile).deleteOnExit();

            grunt = new Grunt(pin, pigContext);
            grunt.exec();
            rc = 0;
            return;
        }

        // Per Utkarsh and Chris invocation of jar file via pig depricated.
    } catch (ParseException e) {
        usage();
        rc = 1;
    } catch (NumberFormatException e) {
        usage();
        rc = 1;
    } catch (Throwable e) {
        //log.error(e);
        // this is a hack to see full error till we resolve commons logging config
        e.printStackTrace();
    } finally {
        // clear temp files
        FileLocalizer.deleteTempFiles();
        PerformanceTimerFactory.getPerfTimerFactory().dumpTimers();
        System.exit(rc);
    }
}

//TODO jz: log4j.properties should be used instead
private static void configureLog4J(Properties properties) {
    // TODO Add a file appender for the logs
    // TODO Need to create a property in the properties file for it.
    // sgroschupf, 25Feb2008: this method will be obsolete with PIG-115.
     
    String log4jconf = properties.getProperty(LOG4J_CONF);
    String trueString = "true";
    boolean brief = trueString.equalsIgnoreCase(properties.getProperty(BRIEF));
    boolean verbose = trueString.equalsIgnoreCase(properties.getProperty(VERBOSE));
    Level logLevel = Level.INFO;

    String logLevelString = properties.getProperty(DEBUG);
    if (logLevelString != null){
        logLevel = Level.toLevel(logLevelString, Level.INFO);
    }
    
    if (log4jconf != null) {
         PropertyConfigurator.configure(log4jconf);
     } else if (!brief ) {
         // non-brief logging - timestamps
         Properties props = new Properties();
         props.setProperty("log4j.rootLogger", "INFO, PIGCONSOLE");
         props.setProperty("log4j.appender.PIGCONSOLE",
                           "org.apache.log4j.ConsoleAppender");
         props.setProperty("log4j.appender.PIGCONSOLE.layout",
                           "org.apache.log4j.PatternLayout");
         props.setProperty("log4j.appender.PIGCONSOLE.layout.ConversionPattern",
                           "%d [%t] %-5p %c - %m%n");
         PropertyConfigurator.configure(props);
         // Set the log level/threshold
         Logger.getRootLogger().setLevel(verbose ? Level.ALL : logLevel);
     } else {
         // brief logging - no timestamps
         Properties props = new Properties();
         props.setProperty("log4j.rootLogger", "INFO, PIGCONSOLE");
         props.setProperty("log4j.appender.PIGCONSOLE",
                           "org.apache.log4j.ConsoleAppender");
         props.setProperty("log4j.appender.PIGCONSOLE.layout",
                           "org.apache.log4j.PatternLayout");
         props.setProperty("log4j.appender.PIGCONSOLE.layout.ConversionPattern",
                           "%m%n");
         PropertyConfigurator.configure(props);
         // Set the log level/threshold
         Logger.getRootLogger().setLevel(verbose ? Level.ALL : logLevel);
     }
}
 
// retruns the stream of final pig script to be passed to Grunt
private static BufferedReader runParamPreprocessor(BufferedReader origPigScript, ArrayList<String> params,
                                            ArrayList<String> paramFiles, String scriptFile, boolean createFile) 
                                throws org.apache.pig.tools.parameters.ParseException, IOException{
    ParameterSubstitutionPreprocessor psp = new ParameterSubstitutionPreprocessor(50);
    String[] type1 = new String[1];
    String[] type2 = new String[1];

    if (createFile){
        BufferedWriter fw = new BufferedWriter(new FileWriter(scriptFile));
        psp.genSubstitutedFile (origPigScript, fw, params.size() > 0 ? params.toArray(type1) : null, 
                                paramFiles.size() > 0 ? paramFiles.toArray(type2) : null);
        return new BufferedReader(new FileReader (scriptFile));

    } else {
        StringWriter writer = new StringWriter();
        psp.genSubstitutedFile (origPigScript, writer,  params.size() > 0 ? params.toArray(type1) : null, 
                                paramFiles.size() > 0 ? paramFiles.toArray(type2) : null);
        return new BufferedReader(new StringReader(writer.toString()));
    }
}
    
   
private static String getVersionString() {
	String findContainingJar = JarManager.findContainingJar(Main.class);
	  try { 
		  StringBuffer buffer = new  StringBuffer();
          JarFile jar = new JarFile(findContainingJar); 
          final Manifest manifest = jar.getManifest(); 
          final Map <String,Attributes> attrs = manifest.getEntries(); 
          Attributes attr = attrs.get("org/apache/pig");
          String version = (String) attr.getValue("Implementation-Version");
          String svnRevision = (String) attr.getValue("Svn-Revision");
          String buildTime = (String) attr.getValue("Build-TimeStamp");
          // we use a version string similar to svn 
          //svn, version 1.4.4 (r25188)
          // compiled Sep 23 2007, 22:32:34
          return "Apache Pig version " + version + " (r" + svnRevision + ") \ncompiled "+buildTime;
      } catch (Exception e) { 
          throw new RuntimeException("unable to read pigs manifest file", e); 
      } 
}

public static void usage()
{
	System.out.println("\n"+getVersionString()+"\n");
    System.out.println("USAGE: Pig [options] [-] : Run interactively in grunt shell.");
    System.out.println("       Pig [options] -e[xecute] cmd [cmd ...] : Run cmd(s).");
    System.out.println("       Pig [options] [-f[ile]] file : Run cmds found in file.");
    System.out.println("  options include:");
    System.out.println("    -4, -log4jconf log4j configuration file, overrides log conf");
    System.out.println("    -b, -brief brief logging (no timestamps)");
    System.out.println("    -c, -cluster clustername, kryptonite is default");
    System.out.println("    -d, -debug debug level, INFO is default");
    System.out.println("    -h, -help display this message");
    System.out.println("    -j, -jar jarfile load jarfile"); 
    System.out.println("    -o, -hod read hod server from system property ssh.gateway");
    System.out.println("    -v, -verbose print all log messages to screen (default to print only INFO and above to screen)");
    System.out.println("    -x, -exectype local|mapreduce, mapreduce is default");
    System.out.println("    -i, -version display version information");
}
}
