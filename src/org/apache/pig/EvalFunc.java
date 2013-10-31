
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PigLogger;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PigProgressable;
import org.apache.pig.builtin.OutputSchema;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.parser.ParserException;

import java.io.IOException;
import java.lang.reflect.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * The class is used to implement functions to be applied to
 * fields in a dataset. The function is applied to each Tuple in the set.
 * The programmer should not make assumptions about state maintained
 * between invocations of the exec() method since the Pig runtime
 * will schedule and localize invocations based on information provided
 * at runtime.  The programmer also should not make assumptions about when or
 * how many times the class will be instantiated, since it may be instantiated
 * multiple times in both the front and back end.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class EvalFunc<T>  {
    /**
     * Reporter to send heartbeats to Hadoop.  If exec will take more than a
     * a few seconds {@link PigProgressable#progress} should be called
     * occasionally to avoid timeouts.  Default Hadoop timeout is 600 seconds.
     */
    protected PigProgressable reporter;

    /**
     * Logging object.  Log calls made on the front end will be sent to
     * pig's log on the client.  Log calls made on the backend will be
     * sent to stdout and can be seen in the Hadoop logs.
     */
    protected Log log = LogFactory.getLog(getClass());

    /**
     * Logger for aggregating warnings.  Any warnings to be sent to the user
     * should be logged to this via {@link PigLogger#warn}.
     */
    protected PigLogger pigLogger;

    private static int nextSchemaId; // for assigning unique ids to UDF columns
    protected String getSchemaName(String name, Schema input) {
        String alias = name + "_";
        if (input!=null && input.getAliases().size() > 0){
            alias += input.getAliases().iterator().next() + "_";
        }

        alias += ++nextSchemaId;
        return alias;
    }

    /**
     * Return type of this instance of EvalFunc.
     */
    protected Type returnType;

    /**
     * EvalFunc's schema type.
     * @see {@link EvalFunc#getSchemaType()}
     */
    public static enum SchemaType {
        NORMAL, //default field type
        VARARG //if the last field of the (udf) schema is of type vararg
    };
    
    public EvalFunc() {
        // Resolve concrete type for T of EvalFunc<T>
        // 1. Build map from type param to type for class hierarchy from current class to EvalFunc
        Map<TypeVariable<?>, Type> typesByTypeVariable = new HashMap<TypeVariable<?>, Type>();
        Class<?> cls = getClass();
        Type type = cls.getGenericSuperclass();
        cls = cls.getSuperclass();
        while (EvalFunc.class.isAssignableFrom(cls)) {
            TypeVariable<? extends Class<?>>[] typeParams = cls.getTypeParameters();
            if (type instanceof ParameterizedType) {
                ParameterizedType pType = (ParameterizedType) type;
                Type[] typeArgs = pType.getActualTypeArguments();
                for (int i = 0; i < typeParams.length; i++) {
                    typesByTypeVariable.put(typeParams[i], typeArgs[i]);
                }
            }
            type = cls.getGenericSuperclass();
            cls = cls.getSuperclass();
        }

        // 2. Use type param to type map to determine concrete type of for T of EvalFunc<T>
        Type targetType = EvalFunc.class.getTypeParameters()[0];
        while (targetType != null && targetType instanceof TypeVariable) {
            targetType = typesByTypeVariable.get(targetType);
        }
        if (targetType == null
                || targetType instanceof GenericArrayType
                || targetType instanceof WildcardType) {
            throw new RuntimeException(String.format(
                    "Failed to determine concrete type for type parameter T of EvalFunc<T> for derived class '%s'",
                    getClass().getName()));
        }
        returnType = targetType;

        // Type check the initial, intermediate, and final functions
        if (this instanceof Algebraic){
            Algebraic a = (Algebraic)this;

            String errMsg = "function of " + getClass().getName() + " is not of the expected type.";
            if (getReturnTypeFromSpec(new FuncSpec(a.getInitial())) != Tuple.class)
                throw new RuntimeException("Initial " + errMsg);
            if (getReturnTypeFromSpec(new FuncSpec(a.getIntermed())) != Tuple.class)
                    throw new RuntimeException("Intermediate " + errMsg);
            if (!getReturnTypeFromSpec(new FuncSpec(a.getFinal())).equals(returnType))
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

    /**
     * Get the Type that this EvalFunc returns.
     * @return Type
     */
    public Type getReturnType(){
        return returnType;
    }

    // report that progress is being made (otherwise hadoop times out after 600 seconds working on one outer tuple)
    /**
     * Utility method to allow UDF to report progress.  If exec will take more than a
     * a few seconds {@link PigProgressable#progress} should be called
     * occasionally to avoid timeouts.  Default Hadoop timeout is 600 seconds.
     */
    public final void progress() {
        if (reporter != null) reporter.progress();
        else warn("No reporter object provided to UDF.", PigWarning.PROGRESS_REPORTER_NOT_PROVIDED);
    }

    /**
     * Issue a warning.  Warning messages are aggregated and reported to
     * the user.
     * @param msg String message of the warning
     * @param warningEnum type of warning
     */
    public final void warn(String msg, Enum warningEnum) {
    	if(pigLogger != null) pigLogger.warn(this, msg, warningEnum);
    	else log.warn("No logger object provided to UDF: " + this.getClass().getName() + ". " + msg);
    }

    /**
     * Placeholder for cleanup to be performed at the end. User defined functions can override.
     * Default implementation is a no-op.
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
     * Report the schema of the output of this UDF.  Pig will make use of
     * this in error checking, optimization, and planning.  The schema
     * of input data to this UDF is provided.
     * <p>
     * The default implementation interprets the {@link OutputSchema} annotation,
     * if one is present. Otherwise, it returns <code>null</code> (no known output schema).
     *
     * @param input Schema of the input
     * @return Schema of the output
     */
    public Schema outputSchema(Schema input) {
        OutputSchema schema = this.getClass().getAnnotation(OutputSchema.class);
        try {
            return (schema == null) ? null : Utils.getSchemaFromString(schema.value());
        } catch (ParserException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * This function should be overriden to return true for functions that return their values
     * asynchronously.  Currently pig never attempts to execute a function
     * asynchronously.
     * @return true if the function can be executed asynchronously.
     */
    @Deprecated
    public boolean isAsynchronous(){
        return false;
    }


    public PigProgressable getReporter() {
        return reporter;
    }


    /**
     * Set the reporter.  Called by Pig to provide a reference of
     * the reporter to the UDF.
     * @param reporter Hadoop reporter
     */
    public final void setReporter(PigProgressable reporter) {
        this.reporter = reporter;
    }

    /**
     * Allow a UDF to specify type specific implementations of itself.  For example,
     * an implementation of arithmetic sum might have int and float implementations,
     * since integer arithmetic performs much better than floating point arithmetic.  Pig's
     * typechecker will call this method and using the returned list plus the schema
     * of the function's input data, decide which implementation of the UDF to use.
     * @return A List containing FuncSpec objects representing the EvalFunc class
     * which can handle the inputs corresponding to the schema in the objects.  Each
     * FuncSpec should be constructed with a schema that describes the input for that
     * implementation.  For example, the sum function above would return two elements in its
     * list:
     * <ol>
     * <li>FuncSpec(this.getClass().getName(), new Schema(new Schema.FieldSchema(null, DataType.DOUBLE)))
     * <li>FuncSpec(IntSum.getClass().getName(), new Schema(new Schema.FieldSchema(null, DataType.INTEGER)))
     * </ol>
     * This would indicate that the main implementation is used for doubles, and the special
     * implementation IntSum is used for ints.
     */
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException{
        return null;
    }

    /**
     * Allow a UDF to specify a list of files it would like placed in the distributed
     * cache.  These files will be put in the cache for every job the UDF is used in.
     * The default implementation returns null.
     * @return A list of files
     */
    public List<String> getCacheFiles() {
        return null;
    }

    public PigLogger getPigLogger() {
        return pigLogger;
    }

    /**
     * Set the PigLogger object.  Called by Pig to provide a reference
     * to the UDF.
     * @param pigLogger PigLogger object.
     */
    public final void setPigLogger(PigLogger pigLogger) {
        this.pigLogger = pigLogger;
    }

    public Log getLogger() {
        return log;
    }

    private Schema inputSchemaInternal=null;
    /**
     * This method will be called by Pig both in the front end and back end to
     * pass a unique signature to the {@link EvalFunc}. The signature can be used
     * to store into the {@link UDFContext} any information which the
     * {@link EvalFunc} needs to store between various method invocations in the
     * front end and back end.
     * @param signature a unique signature to identify this EvalFunc
     */
    public void setUDFContextSignature(String signature) {
    }

    /**
     * This method is for internal use. It is called by Pig core in both front-end
     * and back-end to setup the right input schema for EvalFunc
     */
    public void setInputSchema(Schema input){
        this.inputSchemaInternal=input;
    }

    /**
     * This method is intended to be called by the user in {@link EvalFunc} to get the input
     * schema of the EvalFunc
     */
    public Schema getInputSchema(){
        return this.inputSchemaInternal;
    }

    /**
     * Returns the {@link SchemaType} of the EvalFunc. User defined functions can override
     * this method to return {@link SchemaType#VARARG}. In this case the last FieldSchema
     * added to the Schema in {@link #getArgToFuncMapping()} will be considered as a vararg field.
     * 
     * @return the schema type of the UDF
     */
    public SchemaType getSchemaType() {
        return SchemaType.NORMAL;
    }

}
