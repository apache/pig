
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

package org.apache.hadoop.owl.common;


import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.varia.NullAppender;

public class LogHandler {

    private static String logLayout = "%d{yyyy-MMM-dd HH:mm:ss,SSS} [%t] %x %-5p %c %m%n";

    private static final int RELOAD_PREFS_INTERVAL = 300000; // 300k milliseconds 
    // 5 minute reload interval for picking up new logging preferences
    private static long timeToReload = 0;

    private static boolean logAppenderConfigured = false;

    /**
     * Returns an instance of a logger (<code>org.apache.commons.logging.Log</code>) 
     */
    public static Log getLogger(String caller){
        if (!logAppenderConfigured){
            configureDefaultAppender();
        }
        if (System.currentTimeMillis() > timeToReload){
            readConfigAndResetSelf();
        }
        return LogFactory.getLog(caller);
    }

    public static void resetLogConfiguration(){
        BasicConfigurator.resetConfiguration();
    }

    protected static String getLogLayout(){
        try {
            String ll = OwlConfig.getLogLayout();
            logLayout = ll;
        } catch (OwlException e) {
            // do nothing - basically, wasn't set in Prefs, or wasn't readable right now.
        }
        return logLayout; // return last stable known value.
    }

    public static void configureBasicFileLogAppender(String logfile) throws OwlException{
        try {
            BasicConfigurator.configure(new FileAppender(
                    new PatternLayout(getLogLayout()),
                    logfile
            ));
        } catch (IOException e) {
            throw new OwlException(ErrorType.ERROR_UNABLE_TO_WRITE_TO_LOGFILE, "File : [" + logfile + "]", e);
        }
        logAppenderConfigured = true;
    }

    public static void configureBasicConsoleLogAppender(){
        BasicConfigurator.configure(new ConsoleAppender(
                new PatternLayout(getLogLayout())
        ));
        logAppenderConfigured = true;
    }

    public static void configureNullLogAppender(){
        BasicConfigurator.configure(new NullAppender());
        logAppenderConfigured = true;
    }


    public static void setLogLevel(String caller, Level level) {
        Logger logger = Logger.getLogger(caller);
        logger.setLevel(level);
    }


    public static void setLogLevel(String caller, String level){
        setLogLevel(caller,Level.toLevel(level)); 
        // note: if toLevel cannot parse string level, it defaults to DEBUG
    }

    public static void readConfigAndResetSelf(){
        // try to read info from OwlConfig.getLogLevels, then set levels individually.
        try {
            String levels = OwlConfig.getLogLevels();
            for (String assignment : levels.split(",")){
                String[] kvp = assignment.split("=");
                if (kvp.length == 2){
                    setLogLevel(kvp[0],Level.toLevel(kvp[1]));
                }
            }
        } catch (OwlException e){
            // do nothing - we couldn't read the prefs, or the format was wrong, ignore and move on.
        }
        timeToReload = System.currentTimeMillis()+RELOAD_PREFS_INTERVAL;
    }

    public static void configureDefaultAppender() {
        String logFile;

        // if we can't read logfile or fail setting up an appender to the logfile, we default to using the Console for logs
        try {
            logFile = OwlConfig.getLogFile();
            configureBasicFileLogAppender(logFile);
        } catch (OwlException e) {
            configureBasicConsoleLogAppender();
            return;
        }
        logAppenderConfigured = true;
        readConfigAndResetSelf();
    }

    public static void disableDefaultAppenderInvocation(){
        logAppenderConfigured = true;
    }

}
