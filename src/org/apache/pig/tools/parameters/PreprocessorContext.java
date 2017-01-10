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

/**
 * This is helper class for parameter substitution
 */

package org.apache.pig.tools.parameters;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.Shell;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.validator.BlackAndWhitelistFilter;
import org.apache.pig.validator.PigCommandFilter;
import org.python.google.common.base.Preconditions;

public class PreprocessorContext {

    private int tableinitsize = 10;
    private Deque<Map<String,String>> param_val_stack;

    private PigContext pigContext;

    public Map<String, String> getParamVal() {
        Map <String, String> ret = new Hashtable <String, String>(tableinitsize);

        //stack (deque) iterates LIFO
        for (Map <String, String> map : param_val_stack ) {
            for (Map.Entry<String, String> entry : map.entrySet()) {
                if( ! ret.containsKey(entry.getKey()) ) {
                    ret.put(entry.getKey(), entry.getValue());
                }
            }
        }
        return ret;
    }

    private final Log log = LogFactory.getLog(getClass());

    /**
     * @param limit - max number of parameters. Passing
     *                smaller number only impacts performance
     */
    public PreprocessorContext(int limit) {
        tableinitsize = limit;
        param_val_stack = new ArrayDeque<Map<String,String>> ();
        param_val_stack.push(new Hashtable<String, String> (tableinitsize));
    }

    public void setPigContext(PigContext context) {
        this.pigContext = context;
    }

    /**
     * This method generates parameter value by running specified command
     *
     * @param key - parameter name
     * @param val - string containing command to be executed
     */
    public  void processShellCmd(String key, String val)  throws ParameterSubstitutionException, FrontendException {
        processShellCmd(key, val, true);
    }

    /**
     * This method generates value for the specified key by
     * performing substitution if needed within the value first.
     *
     * @param key - parameter name
     * @param val - value supplied for the key
     */
    public  void processOrdLine(String key, String val)  throws ParameterSubstitutionException {
        processOrdLine(key, val, true);
    }

    public void paramScopePush() {
        param_val_stack.push( new Hashtable<String, String> (tableinitsize) );
    }

    public void paramScopePop() {
        param_val_stack.pop();
    }

    public boolean paramval_containsKey(String key) {
        for (Map <String, String> map : param_val_stack ) {
            if( map.containsKey(key) ) {
                return true;
            }
        }
        return false;
    }

    public String paramval_get(String key) {
        for (Map <String, String> map : param_val_stack ) {
            if( map.containsKey(key) ) {
                return map.get(key);
            }
        }
        return null;
    }

    public void paramval_put(String key, String value) {
        param_val_stack.peek().put(key, value);
    }

    /**
     * This method generates parameter value by running specified command
     *
     * @param key - parameter name
     * @param val - string containing command to be executed
     */
    public  void processShellCmd(String key, String val, Boolean overwrite)  throws ParameterSubstitutionException, FrontendException {
        if (pigContext != null) {
            BlackAndWhitelistFilter filter = new BlackAndWhitelistFilter(pigContext);
            filter.validate(PigCommandFilter.Command.SH);
        }

        if (paramval_containsKey(key) && !overwrite) {
            return;
        }

        val = val.substring(1, val.length()-1); //to remove the backticks
        String sub_val = substitute(val);
        sub_val = executeShellCommand(sub_val);

        if (paramval_containsKey(key) && !paramval_get(key).equals(sub_val) ) {
            //(boolean overwrite is always true here)
            log.warn("Warning : Multiple values found for " + key + " command `" + val + "`. "
                     + "Previous value " + paramval_get(key) + ", now using value " + sub_val);
        }

        paramval_put(key, sub_val);
    }

    public void validate(String preprocessorCmd) throws FrontendException {
        if (pigContext == null) {
            return;
        }

        final BlackAndWhitelistFilter filter = new BlackAndWhitelistFilter(pigContext);
        final String declareToken = "%declare";
        final String defaultToken = "%default";

        if (preprocessorCmd.toLowerCase().equals(declareToken)) {
            filter.validate(PigCommandFilter.Command.DECLARE);
        } else if (preprocessorCmd.toLowerCase().equals(defaultToken)) {
            filter.validate(PigCommandFilter.Command.DEFAULT);
        } else {
            throw new IllegalArgumentException("Pig Internal Error. Invalid preprocessor command specified : "
                            + preprocessorCmd);
        }
    }
    
    /**
     * This method generates value for the specified key by
     * performing substitution if needed within the value first.
     *
     * @param key - parameter name
     * @param val - value supplied for the key
     * @param overwrite - specifies whether the value should be replaced if it already exists
     */
    public  void processOrdLine(String key, String val, Boolean overwrite)  throws ParameterSubstitutionException {

        String sub_val = substitute(val, key);
        if (paramval_containsKey(key)) {
            if (paramval_get(key).equals(sub_val) || !overwrite) {
                return;
            } else {
                log.warn("Warning : Multiple values found for " + key
                         + ". Previous value " + paramval_get(key)
                         + ", now using value " + sub_val);
            }
        }

        paramval_put(key, sub_val);
    }


    /*
     * executes the 'cmd' in shell and returns result
     */
    private String executeShellCommand (String cmd)
    {
        Process p;
        String streamData="";
        String streamError="";
        try {
            log.info("Executing command : " + cmd);
            // we can't use exec directly since it does not handle
            // case like foo -c "bar bar" correctly. It splits on white spaces even in presents of quotes
            StringBuffer sb  = new StringBuffer("");
            String[] cmdArgs;
            if (Shell.WINDOWS) {
                cmd = cmd.replaceAll("/", "\\\\");
                sb.append(cmd);
                cmdArgs = new String[]{"cmd", "/c", sb.toString() };
            } else {
                sb.append("exec ");
                sb.append(cmd);
                cmdArgs = new String[]{"bash", "-c", sb.toString() };
            }

            p = Runtime.getRuntime().exec(cmdArgs);

        } catch (IOException e) {
            RuntimeException rte = new RuntimeException("IO Exception while executing shell command : "+e.getMessage() , e);
            throw rte;
        }

        BufferedReader br = null;
        try{
            InputStreamReader isr = new InputStreamReader(p.getInputStream());
            br = new BufferedReader(isr);
            String line=null;
            StringBuilder sb = new StringBuilder();
            while ( (line = br.readLine()) != null){
                sb.append(line);
                sb.append("\n");
            }
            streamData = sb.toString();
        } catch (IOException e){
            RuntimeException rte = new RuntimeException("IO Exception while executing shell command : "+e.getMessage() , e);
            throw rte;
        } finally {
            if (br != null) try {br.close();} catch(Exception e) {}
        }

        try {
            InputStreamReader isr = new InputStreamReader(p.getErrorStream());
            br = new BufferedReader(isr);
            String line=null;
            StringBuilder sb = new StringBuilder();
            while ( (line = br.readLine()) != null ) {
                sb.append(line);
                sb.append("\n");
            }
            streamError = sb.toString();
            log.debug("Error stream while executing shell command : " + streamError);
        } catch (Exception e) {
            RuntimeException rte = new RuntimeException("IO Exception while executing shell command : "+e.getMessage() , e);
            throw rte;
        } finally {
            if (br != null) try {br.close();} catch(Exception e) {}
        }

        int exitVal;
        try {
            exitVal = p.waitFor();
        } catch (InterruptedException e) {
            RuntimeException rte = new RuntimeException("Interrupted Thread Exception while waiting for command to get over"+e.getMessage() , e);
            throw rte;
        }

        if (exitVal != 0) {
            RuntimeException rte = new RuntimeException("Error executing shell command: " + cmd + ". Command exit with exit code of " + exitVal );
            throw rte;
        }

        return streamData.trim();
    }

    public void loadParamVal(List<String> params, List<String> paramFiles)
                throws IOException, ParseException {
        StringReader dummyReader = null; // ParamLoader does not have an empty contructor
        ParamLoader paramLoader = new ParamLoader(dummyReader);
        paramLoader.setContext(this);

        if (paramFiles != null) {
            for (String path : paramFiles) {
                BufferedReader in = new BufferedReader(new FileReader(path));
                paramLoader.ReInit(in);
                while (paramLoader.Parse()) {}
                in.close();
            }
        }
        
        if (params != null) {
            for (String param : params) {
                paramLoader.ReInit(new StringReader(param));
                paramLoader.Parse();
            }
        }
    }

    private Pattern bracketIdPattern = Pattern.compile("\\$\\{([_]*[a-zA-Z][a-zA-Z_0-9]*)\\}");
    private Pattern id_pattern = Pattern.compile("\\$([_]*[a-zA-Z][a-zA-Z_0-9]*)");

    public String substitute(String line) throws ParameterSubstitutionException {
        return substitute(line, null);
    }

    public  String substitute(String line, String parentKey) throws ParameterSubstitutionException {
        int index = line.indexOf('$');
        if (index == -1)
            return line;

        String replaced_line = line;

        Matcher bracketKeyMatcher = bracketIdPattern.matcher(line);

        String key="";
        String val="";

        while (bracketKeyMatcher.find()) {
            if ( (bracketKeyMatcher.start() == 0) || (line.charAt( bracketKeyMatcher.start() - 1)) != '\\' ) {
                key = bracketKeyMatcher.group(1);
                if (!(paramval_containsKey(key))) {
                    String message;
                    if (parentKey == null) {
                        message = "Undefined parameter : " + key;
                    } else {
                        message = "Undefined parameter : " + key + " found when trying to find the value of " + parentKey + "."; 
                    }
                    throw new ParameterSubstitutionException(message);
                }
                val = paramval_get(key);
                if (val.contains("$")) {
                    val = val.replaceAll("(?<!\\\\)\\$", "\\\\\\$");
                }
                replaced_line = replaced_line.replaceFirst("\\$\\{"+key+"\\}", val);
            }
        }

        Matcher keyMatcher = id_pattern.matcher( replaced_line );

        key="";
        val="";

        while (keyMatcher.find()) {
            // make sure that we don't perform parameter substitution
            // for escaped vars of the form \$<id>
            if ( (keyMatcher.start() == 0) || (line.charAt( keyMatcher.start() - 1)) != '\\' ) {
                key = keyMatcher.group(1);
                if (!(paramval_containsKey(key))) {
                    String message;
                    if (parentKey == null) {
                        message = "Undefined parameter : " + key;
                    } else {
                        message = "Undefined parameter : " + key + " found when trying to find the value of " + parentKey + "."; 
                    }
                    throw new ParameterSubstitutionException(message);
                }
                val = paramval_get(key);
                if (val.contains("$")) {
                    val = val.replaceAll("(?<!\\\\)\\$", "\\\\\\$");
                }
                replaced_line = replaced_line.replaceFirst("\\$"+key, val);
            }
        }

        // unescape $<id>
        replaced_line = replaced_line.replaceAll("\\\\\\$","\\$");
        return replaced_line;
    }

}


