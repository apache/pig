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
package org.apache.pig.tools.grunt;

import java.io.BufferedReader;
import java.util.ArrayList;

import jline.ConsoleReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.PigServer;
import org.apache.pig.impl.PigContext;
import org.apache.pig.tools.grunt.GruntParser;
import org.apache.pig.tools.grunt.PigCompletor;
import org.apache.pig.tools.grunt.PigCompletorAliases;
import org.apache.pig.tools.pigstats.PigStatsUtil;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.util.LogUtils;

public class Grunt 
{
    private final Log log = LogFactory.getLog(getClass());
    
    BufferedReader in;
    PigServer pig;
    GruntParser parser;    

    public Grunt(BufferedReader in, PigContext pigContext) throws ExecException {
        this.in = in;
        this.pig = new PigServer(pigContext);

        if (in != null) {
            parser = new GruntParser(in, pig);
        }
    }

    public void setConsoleReader(ConsoleReader c)
    {
        c.addCompletor(new PigCompletorAliases(pig));
        c.addCompletor(new PigCompletor());
        parser.setConsoleReader(c);
    }

    public void run() {        
        boolean verbose = "true".equalsIgnoreCase(pig.getPigContext().getProperties().getProperty("verbose"));
        while(true) {
            try {
                PigStatsUtil.getEmptyPigStats();
                parser.setInteractive(true);
                parser.parseStopOnError();
                break;                            
            } catch(Throwable t) {
                LogUtils.writeLog(t, pig.getPigContext().getProperties().getProperty("pig.logfile"), 
                        log, verbose, "Pig Stack Trace");
                parser.ReInit(in);
            }
        }
    }

    public int[] exec() throws Throwable {
        boolean verbose = "true".equalsIgnoreCase(pig.getPigContext().getProperties().getProperty("verbose"));
        try {
            PigStatsUtil.getEmptyPigStats();
            parser.setInteractive(false);
            return parser.parseStopOnError();
        } catch (Throwable t) {
            LogUtils.writeLog(t, pig.getPigContext().getProperties().getProperty("pig.logfile"), 
                    log, verbose, "Pig Stack Trace");
            throw (t);
        }
    }
    
    public void checkScript(String scriptFile) throws Throwable {
        boolean verbose = "true".equalsIgnoreCase(pig.getPigContext().getProperties().getProperty("verbose"));
        try {
            parser.setInteractive(false);
            parser.setValidateEachStatement(true);
            boolean dontPrintOutput = true;
            parser.processExplain(null, scriptFile, false, "text", null, 
                    new ArrayList<String>(), new ArrayList<String>(),
                    dontPrintOutput);
        } catch (Throwable t) {
            LogUtils.writeLog(t, pig.getPigContext().getProperties().getProperty("pig.logfile"), 
                    log, verbose, "Pig Stack Trace");
            throw (t);
        }
    }

}
