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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Class to represent a UDF specification.
 * Encapsulates the class name and the arguments to the constructor.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FuncSpec implements Serializable, Cloneable {

    private static final long serialVersionUID = 2L;
    String className = null;
    String[] ctorArgs = null;
    Schema inputArgsSchema = null;
   
    /**
     * @param className the name of the class for the udf
     * @param ctorArg the argument to pass the constructor for the above class.
     * Constructors can only take strings.
     */
    public FuncSpec(String className, String ctorArg) {
        this.className = className;
        this.ctorArgs = new String[1];
        this.ctorArgs[0] = ctorArg;
    }

    /**
     * @param className the name of the class for the udf
     * @param ctorArgs the arguments to pass to the constructor for the above class.
     * Constructors can only take strings.
     */
    public FuncSpec(String className, String[] ctorArgs) {
        this.className = className;
        this.ctorArgs = ctorArgs;
    }
    
    /**
     * @param className the name of the class for the udf
     * @param ctorArgs the arguments to pass to the constructor for the above class.
     * Constructors can only take strings.
     * @param inputArgsSchema schema for input args taken by this Function
     */
    public FuncSpec(String className, String[] ctorArgs, Schema inputArgsSchema) {
        this(className, ctorArgs);
        this.inputArgsSchema = inputArgsSchema;
    }
    
    /**
     * @param funcSpec the name of the function and any arguments.
     * It should have the form: classname('arg1', 'arg2', ...)
     */
    public FuncSpec(String funcSpec) {
        this.className = getClassNameFromSpec(funcSpec);
        List<String> args = parseArguments(getArgStringFromSpec(funcSpec));
        if(args.size() > 0) {
            this.ctorArgs = new String[args.size()];
            int i = 0;
            for (Iterator<String> iterator = args.iterator(); iterator.hasNext();) {
                this.ctorArgs[i++] = iterator.next();
                
            }
        }
            
    }
    
    /**
     * 
     * @param funcSpec funcSpec the name of the function and any arguments.
     * It should have the form: classname('arg1', 'arg2', ...)
     * @param inputArgsSchema schema for input args taken by this Function
     */
    public FuncSpec(String funcSpec, Schema inputArgsSchema) {
        this(funcSpec);
        this.inputArgsSchema = inputArgsSchema;
    }
    
    /**
     * Parse the class name out of a function specification string.
     * @return name of the class.
     */
    public static String getClassNameFromSpec(String funcSpec){
        int paren = funcSpec.indexOf('(');
        if (paren!=-1)
            return funcSpec.substring(0, paren);
        else
            return funcSpec;
    }
 
    /**
     * Get the argument values passed to the func spec.
     * @return argument values.  Format will be arg1, arg2, ... )
     */
    public static String getArgStringFromSpec(String funcSpec){
        int paren = funcSpec.indexOf('(');
        if (paren!=-1)
            return funcSpec.substring(paren+1);
        else
            return "";
    }
    
    /**
     * Parse the argument values out of a function specification string.
     * @param argString should be of the form "'arg1', 'arg2', ..."
     * @return List of the different argument strings
     */
    public static List<String> parseArguments(String argString){
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
    
    /**
     * @return the className
     */
    public String getClassName() {
        return className;
    }
    /**
     * @param className the className to set
     */
    public void setClassName(String className) {
        this.className = className;
    }
    /**
     * @return the ctorArgs
     */
    public String[] getCtorArgs() {
        return ctorArgs;
    }
    /**
     * @param ctorArgs the ctorArgs to set
     */
    public void setCtorArgs(String[] ctorArgs) {
        this.ctorArgs = ctorArgs;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        StringBuilder sb =  new StringBuilder();
        sb.append(className);
        
        
        if(ctorArgs != null) {
            sb.append("(");
            for (int i = 0; i < ctorArgs.length; i++) {
                sb.append("'");
                sb.append(ctorArgs[i]);
                sb.append("'");
                if(i != ctorArgs.length - 1) {
                    sb.append(",");
                }
            }
            sb.append(")");
            
        }
        
        
        return sb.toString();
    }

    /**
     * @return the inputArgsSchema
     */
    public Schema getInputArgsSchema() {
        return inputArgsSchema;
    }

    /**
     * @param inputArgsSchema the inputArgsSchema to set
     */
    public void setInputArgsSchema(Schema inputArgsSchema) {
        this.inputArgsSchema = inputArgsSchema;
    }
    
    @Override
    public boolean equals(Object other) {
        if (other != null && other instanceof FuncSpec) {
            FuncSpec ofs = (FuncSpec)other;
            if (!className.equals(ofs.className)) return false;
            if (ctorArgs == null && ofs.ctorArgs != null ||
                    ctorArgs != null && ofs.ctorArgs == null) {
                return false;
            }
           
            if (ctorArgs != null && ofs.ctorArgs != null) {
                if (ctorArgs.length != ofs.ctorArgs.length) return false;
                for (int i = 0; i < ctorArgs.length; i++) {
                    if (!ctorArgs[i].equals(ofs.ctorArgs[i])) return false;
                }
            }
            return Schema.equals(inputArgsSchema, ofs.inputArgsSchema, false, true);
        } else {
            return false;
        }
    }
    
    @Override
    public int hashCode() {
        return ctorArgs == null ?  getClassName().hashCode() :
                                   getClassName().hashCode() + ctorArgs.length;
    }
 
    @Override
    public FuncSpec clone() throws CloneNotSupportedException {
        String[] args = null;
        if (ctorArgs != null) {
            args = new String[ctorArgs.length];
            for (int i = 0; i < ctorArgs.length; i++) {
                // Can use the same strings, they're immutable
                args[i] = ctorArgs[i];
            }
        }
        Schema s = null;
        if (inputArgsSchema != null) s = inputArgsSchema.clone();
        return new FuncSpec(className, args, s);
    }
    
}
