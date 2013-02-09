/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * This is the main driver for parameter substitution 
 * http://wiki.apache.org/pig/ParameterSubstitution
 */
package org.apache.pig.tools.parameters;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Appender;
import org.apache.log4j.Logger;

import java.util.Hashtable;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.StringReader;
import java.io.Writer;

public class ParameterSubstitutionPreprocessor {

    private PreprocessorContext pc;
    private final Log log = LogFactory.getLog(getClass());
    private ParamLoader paramParser;
    private PigFileParser pigParser;

    /**
     * @param limit - max number of parameters to expect. Smaller values would
     * would not cause incorrect behavior but would impact performance
     */
    public ParameterSubstitutionPreprocessor(int limit) {
        pc = new PreprocessorContext(50);
        StringReader sr = null;

        paramParser  = new ParamLoader(sr);
        paramParser.setContext(pc);
        pigParser = new PigFileParser(sr);
        pigParser.setContext(pc);
    }

    /**
     * This is the main API that takes script template and produces pig script 
     * @param pigInput - input stream that contains pig file
     * @param pigOutput - stream where transformed file is written
     * @param args - command line arguments in the order they appear on the command line; format: key=val
     * @param argFiles - list of configuration files in the order they appear on the command line
     */
    public void genSubstitutedFile (BufferedReader pigInput, Writer pigOutput, String[] args, String[] argFiles) throws ParseException {

        // load parameters from the files followed by parameters
        // from command line both in the order they appear on the command line
        // this enforces precedence rules 
        if (argFiles!=null) {
            for (int i=0;i<argFiles.length;i++) {
                if (argFiles[i].length() > 0)
                    loadParamsFromFile(argFiles[i]);
            }
        }
        if (args!=null) {
            for (int i=0;i<args.length;i++) {
                if (args[i].length() > 0)
                    loadParamsFromCmdline(args[i]);
            }
        }

        // In case there is no EOL before EOF, add EOL for each line
        String line = null;
        StringBuilder blder = new StringBuilder();       
        try {
            while ((line = pigInput.readLine()) != null) { 
                blder.append(line).append("\n");
            }
        } catch (IOException e) {
            throw new ParseException(e.getMessage());
        }
                
        pigInput = new BufferedReader(new StringReader(blder.toString()));
        
        // perform the substitution
        parsePigFile(pigInput , pigOutput);
    }

    private void parsePigFile(BufferedReader in, Writer out) throws ParseException {
        pigParser.setOutputWriter(out);
        pigParser.ReInit(in);
        try {
            pigParser.Parse();
            //close input and output streams
            in.close();
            out.flush();
            out.close();
        } catch (IOException e) {
            RuntimeException rte = new RuntimeException(e.getMessage() , e);
            throw rte;
        }
    }

    /* 
     * populates the param-val hashtable with parameters read from a file
     * @param filename - name of the config file
     */
    private void loadParamsFromFile(String filename) throws ParseException {

        try {
            BufferedReader in = new BufferedReader(new FileReader(filename));
            String line;
 
            paramParser.ReInit(in);
            while (paramParser.Parse()) {}
            in.close();
        }catch(org.apache.pig.tools.parameters.ParseException e){
        	log.info("The file: \""+filename+"\" contains parameter that cannot be parsed by Pig in line. Please double check it");
        	log.info("Parser give the follow error message:");
        	log.info(e.getMessage());
        	throw e;
        } catch (IOException e) {
            RuntimeException rte = new RuntimeException(e.getMessage() , e);
            throw rte;
        }

    }

    /*
     * adds key-val pairs from cmd line to the param-val hashtable 
     * @param param contains key-val of the form key='value'
     */
    private void loadParamsFromCmdline(String line) throws ParseException {
        try {
            // new lines are needed by the parser
            paramParser.ReInit(new StringReader(line));
            paramParser.Parse();
        } catch(org.apache.pig.tools.parameters.ParseException e){
        	log.info("The parameter: \""+line+"\" cannot be parsed by Pig. Please double check it");
        	log.info("Parser give the follow error message:");
        	log.info(e.getMessage());
        	throw e;
        }catch (IOException e) {
            RuntimeException rte = new RuntimeException(e.getMessage() , e);
            throw rte;
        }

    }


}
