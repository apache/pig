
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

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PigLogger;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PigProgressable;


/**
 * The class is used to implement functions to be applied to
 * a dataset. The function is applied to each Tuple in the set.
 * The programmer should not make assumptions about state maintained
 * between invocations of the invoke() method since the Pig runtime
 * will schedule and localize invocations based on information provided
 * at runtime.  The programmer also should not make assumptions about when or
 * how many times the class will be instantiated, since it may be instantiated
 * multiple times in both the front and back end.
 */
public abstract class EvalFunc<T>  {
    // UDFs must use this to report progress
    // if the exec is taking more that 300 ms
    protected PigProgressable reporter;
    protected Log log = LogFactory.getLog(getClass());
    protected PigLogger pigLogger;

    private static int nextSchemaId; // for assigning unique ids to UDF columns
    protected String getSchemaName(String name, Schema input) {
        String alias = name + "_";
        if (input.getAliases().size() > 0){
            alias += input.getAliases().iterator().next() + "_";
        }

        alias += ++nextSchemaId;
        return alias;
    }
    
    protected Type returnType;
    
    public EvalFunc(){
        
        //Figure out what the return type is by following the object hierarchy upto the EvalFunc
        
        Class<?> superClass = getClass();
        Type superType = getClass();
        
        while (!superClass.isAssignableFrom(EvalFunc.class)){
            superType = superClass.getGenericSuperclass();
            superClass = superClass.getSuperclass();
        }
        String errMsg = getClass() + "extends the raw type EvalFunc. It should extend the parameterized type EvalFunc<T> instead.";
        
        if (!(superType instanceof ParameterizedType))
            throw new RuntimeException(errMsg);
        
        Type[] parameters  = ((ParameterizedType)superType).getActualTypeArguments();
        
        if (parameters.length != 1)
                throw new RuntimeException(errMsg);
        
        returnType = parameters[0];
        
        
        
        //Type check the initial, intermediate, and final functions
        if (this instanceof Algebraic){
            Algebraic a = (Algebraic)this;
            
            errMsg = "function of " + getClass().getName() + " is not of the expected type.";
            if (getReturnTypeFromSpec(new FuncSpec(a.getInitial())) != Tuple.class)
                throw new RuntimeException("Initial " + errMsg);
            if (getReturnTypeFromSpec(new FuncSpec(a.getIntermed())) != Tuple.class)
                    throw new RuntimeException("Intermediate " + errMsg);
            if (getReturnTypeFromSpec(new FuncSpec(a.getFinal())) != returnType)
                    throw new RuntimeException("Final " + errMsg);
        }
        
    }
    

    private Type getReturnTypeFromSpec(FuncSpec funcSpec){
        try{
            return ((EvalFunc<?>)PigContext.instantiateFuncFromSpec(funcSpec)).getReturnType();
        }catch (ClassCastException e){
            throw new RuntimeException(funcSpec + " does not specify an eval func", e);
        }
    }
    
    public Type getReturnType(){
        return returnType;
    }
        
    // report that progress is being made (otherwise hadoop times out after 600 seconds working on one outer tuple)
    public final void progress() {
        if (reporter != null) reporter.progress();
        else warn("No reporter object provided to UDF.", PigWarning.PROGRESS_REPORTER_NOT_PROVIDED);
    }
    
    public final void warn(String msg, Enum warningEnum) {
    	if(pigLogger != null) pigLogger.warn(this, msg, warningEnum);
    	else log.warn("No logger object provided to UDF: " + this.getClass().getName() + ". " + msg);
    }

    /**
     * Placeholder for cleanup to be performed at the end. User defined functions can override.
     *
     */
    public void finish(){}
    
    
    
    /**
     * This callback method must be implemented by all subclasses. This
     * is the method that will be invoked on every Tuple of a given dataset.
     * Since the dataset may be divided up in a variety of ways the programmer
     * should not make assumptions about state that is maintained between
     * invocations of this method.
     * 
     * @param input the Tuple to be processed.
     * @return result, of type T.
     * @throws IOException
     */
    abstract public T exec(Tuple input) throws IOException;
    
    /**
     * @param input Schema of the input
     * @return Schema of the output
     */
    public Schema outputSchema(Schema input) {
        return null;
    }
    
    /**
     * This function should be overriden to return true for functions that return their values
     * asynchronously.  Currently pig never attempts to execute a function
     * asynchronously.
     * @return true if the function can be executed asynchronously.
     */
    public boolean isAsynchronous(){
        return false;
    }


    public PigProgressable getReporter() {
        return reporter;
    }


    public final void setReporter(PigProgressable reporter) {
        this.reporter = reporter;
    }
    
    /**
     * @return A List containing FuncSpec objects representing the Function class
     * which can handle the inputs corresponding to the schema in the objects
     */
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException{
        return null;
    }
    
    public PigLogger getPigLogger() {
        return pigLogger;
    }

    public final void setPigLogger(PigLogger pigLogger) {
        this.pigLogger = pigLogger;
    }
    
    public Log getLogger() {
    	return log;
    }
}
