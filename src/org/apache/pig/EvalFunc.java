
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

import org.apache.pig.data.Datum;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.TupleSchema;


/**
 * The class is used to implement functions to be applied to
 * a dataset. The function is applied to each Tuple in the set.
 * The programmer should not make assumptions about state maintained
 * between invocations of the invoke() method since the Pig runtime
 * will schedule and localize invocations based on information provided
 * at runtime.
 * 
 * The programmer should not directly extend this class but instead
 * extend one of the subclasses where we have bound the parameter T 
 * to a specific Datum
 * 
 * @author database-systems@yahoo.research
 *
 */
public abstract class EvalFunc<T extends Datum>  {
    
    protected Type returnType;
    
    public EvalFunc(){
        
        //Figure out what the return type is by following the object hierarchy upto the EvalFunc
        
        Class<?> superClass = getClass();
        Type superType = getClass();
        
        while (!superClass.isAssignableFrom(EvalFunc.class)){
            superType = superClass.getGenericSuperclass();
            superClass = superClass.getSuperclass();
        }
        StringBuilder errMsg = new StringBuilder();
        errMsg.append(getClass());
        errMsg.append("extends the raw type EvalFunc. It should extend the parameterized type EvalFunc<T> instead.");
        
        if (!(superType instanceof ParameterizedType))
            throw new RuntimeException(errMsg.toString());
        
        Type[] parameters  = ((ParameterizedType)superType).getActualTypeArguments();
        
        if (parameters.length != 1)
                throw new RuntimeException(errMsg.toString());
        
        returnType = parameters[0];
        
        if (returnType == Datum.class){
            throw new RuntimeException("Eval function must return a specific type of Datum");
        }
        
        
        //Type check the initial, intermediate, and final functions
        if (this instanceof Algebraic){
            Algebraic a = (Algebraic)this;
            
            errMsg = new StringBuilder();
            errMsg.append("function of ");
            errMsg.append(getClass().getName());
            errMsg.append(" is not of the expected type.");
            if (getReturnTypeFromSpec(a.getInitial()) != Tuple.class)
                throw new RuntimeException("Initial " + errMsg.toString());
            if (getReturnTypeFromSpec(a.getIntermed()) != Tuple.class)
                    throw new RuntimeException("Intermediate " + errMsg.toString());
            if (getReturnTypeFromSpec(a.getFinal()) != returnType)
                    throw new RuntimeException("Final " + errMsg.toString());
        }
        
    }
    

    private Type getReturnTypeFromSpec(String funcSpec){
        return ((EvalFunc) PigContext.instantiateFuncFromSpec(funcSpec))
                .getReturnType();
    }
    
    public Type getReturnType(){
        return returnType;
    }
        
    // report that progress is being made (otherwise hadoop times out after 600 seconds working on one outer tuple)
    protected void progress() { 
        //This part appears to be unused and is causing problems due to changing hadoop signature
        /*
        if (PigMapReduce.reporter != null) {
            try {
                PigMapReduce.reporter.progress();
            } catch (IOException ignored) {
            }
        }
        */
        
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
     * @throws IOException
     */
    abstract public void exec(Tuple input, T output) throws IOException;
    
    /**
     * @param input Schema of the input
     * @return Schema of the output
     */
    public Schema outputSchema(Schema input) {
        return new TupleSchema();
    }
    
    /**
     * This function should be overriden to return true for functions that return their values
     * asynchronously. 
     * @return
     */
    public boolean isAsynchronous(){
        return false;
    }
}
