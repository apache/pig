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
import org.apache.pig.ExecType;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.LogicalPlanBuilder;
import org.apache.pig.tools.cmdline.CmdLineParser;
import org.apache.pig.tools.grunt.Grunt;
import org.apache.pig.tools.timer.PerformanceTimerFactory;


public class Main
{

    private final static Log log = LogFactory.getLog(Main.class);
    
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
    PigContext pigContext = new PigContext();

    try {
        BufferedReader in = null;
        ExecMode mode = ExecMode.UNKNOWN;
        int port = 0;
        String file = null;
        Level logLevel = Level.INFO;
        boolean brief = false;
        String log4jconf = null;
        boolean verbose = false;

        CmdLineParser opts = new CmdLineParser(args);
        // Don't use -l, --latest, -c, --cluster, -cp, -classpath, -D as these
        // are masked by the startup perl script.
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

        char opt;
        while ((opt = opts.getNextOpt()) != CmdLineParser.EndOfOpts) {
            switch (opt) {
            case '4':
                log4jconf = opts.getValStr();
                break;

            case 'b':
                brief = true;
                break;

            case 'c': {
                // Needed away to specify the cluster to run the MR job on
                // Bug 831708 - fixed
                   String cluster = opts.getValStr();
                   System.out.println("Changing MR cluster to " + cluster);
                   if(cluster.indexOf(':') < 0) {
                       cluster = cluster + ":50020";
                   }
                   pigContext.setJobtrackerLocation(cluster);
                break;
                      }

            case 'd':
                logLevel = Level.toLevel(opts.getValStr(), Level.INFO);
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

            case 'j': {
                   String splits[] = opts.getValStr().split(":", -1);
                   for (int i = 0; i < splits.length; i++) {
                       if (splits[i].length() > 0) {
                        pigContext.addJar(splits[i]);
                       }
                   }
                break;
                      }

            case 'o': {
                   String gateway = System.getProperty("ssh.gateway");
                   if (gateway == null || gateway.length() == 0) {
                       System.setProperty("hod.server", "local");
                   } else {
                       System.setProperty("hod.server", System.getProperty("ssh.gateway"));
                   }
                break;
                      }

            case 'v':
                verbose = true;
                break;

            case 'x':
                ExecType exectype;
                   try {
                       exectype = PigServer.parseExecType(opts.getValStr());
                   } catch (IOException e) {
                       throw new RuntimeException("ERROR: Unrecognized exectype.", e);
                   }
                   pigContext.setExecType(exectype);
                break;

            default: {
                Character cc = new Character(opt);
                throw new AssertionError("Unhandled option " + cc.toString());
                     }
            }
        }

        LogicalPlanBuilder.classloader = pigContext.createCl(null);

        if (log4jconf != null) {
            PropertyConfigurator.configure(log4jconf);
        } else if (!brief) {
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

        // TODO Add a file appender for the logs
        // TODO Need to create a property in the properties file for it.

        // Don't forget to undo all this for the port option.

        // I might know what I want to do next, then again I might not.
        Grunt grunt = null;
        switch (mode) {
        case FILE:
            // Run, using the provided file as a pig file
            in = new BufferedReader(new FileReader(file));
            grunt = new Grunt(in, pigContext);
            grunt.exec();
            return;

        case STRING: {
            // Gather up all the remaining arguments into a string and pass them into
            // grunt.
            StringBuffer sb = new StringBuffer();
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
            grunt = new Grunt(in, pigContext);
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
        log.error(e);
    } finally {
        PerformanceTimerFactory.getPerfTimerFactory().dumpTimers();
        System.exit(rc);
    }
}
    
public static void usage()
{
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
}
}
