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

package org.apache.pig.scripting.jruby;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.parser.ParserException;
import org.apache.pig.backend.executionengine.ExecException;

import org.jruby.Ruby;
import org.jruby.RubyArray;
import org.jruby.embed.ScriptingContainer;
import org.jruby.runtime.builtin.IRubyObject;

/**
 * This class serves at the bridge between Ruby methods that
 * are registered with and extend PigUdf, and their execution in
 * Pig. An instance of the containing class is created, and their
 * method name will be called against that instance. If they have
 * a schema function associated, then when outputSchema is called,
 * that function will be given the input Schema and the output will
 * be given to Pig.
 */
public class JrubyEvalFunc extends EvalFunc<Object> {

    private String methodName;
    private long numRequiredArgs;
    private long numOptionalArgs;

    private String fileName;
    private String functionName;

    private boolean isInitialized = false;

    private String schemaPiece;
    private Object funcReceiver;
    private Object funcInfoEncapsulator;

    private static ScriptingContainer rubyEngine = JrubyScriptEngine.rubyEngine;
    private static Ruby ruby = rubyEngine.getProvider().getRuntime();

    // Instantiating this class without arguments is meaningless
    private JrubyEvalFunc() {}

    /**
     * The constructor for this class registers the filename of the Ruby script
     * and the name of the method we care about. The difference between function name
     * and method name is that the former is the name that was used to register the function,
     * whereas the method name is the actual method that must be invoked. The two are often
     * the same, but not always. functionName is the key to the list of EvalFuncs that
     * are registered.
     */
    public JrubyEvalFunc(String filename, String functionName) throws IOException {
        this.fileName = filename;
        this.functionName = functionName;
    }

    /**
     * This method initializes the objects necessary to evaluate the Ruby class on the pig side. Using
     * the object that was saved to the functionName key by Ruby, this class gets an instance of the
     * class that will receive method calls, as well as information on the arity.
     */
    private void initialize() {
        funcInfoEncapsulator = JrubyScriptEngine.RubyFunctions.getFunctions("evalfunc", fileName).get(functionName);

        funcReceiver = rubyEngine.callMethod(funcInfoEncapsulator, "get_receiver");
        methodName = rubyEngine.callMethod(funcInfoEncapsulator, "method_name", String.class);
        numRequiredArgs = rubyEngine.callMethod(funcInfoEncapsulator, "required_args", Long.class);
        numOptionalArgs = rubyEngine.callMethod(funcInfoEncapsulator, "optional_args", Long.class); //TODO support varargs?

        isInitialized = true;
    }

    /**
     * The exec method passes the tuple argument to the Ruby function, and converts the result back to Pig.
     */
    @Override
    public Object exec(Tuple tuple) throws IOException {
        if (!isInitialized)
            initialize();

        try {
            IRubyObject rubyResult = null;
            if (tuple == null || (numRequiredArgs == 0 && numOptionalArgs == 0)) {
                rubyResult = rubyEngine.callMethod(funcReceiver, methodName, IRubyObject.class);
            } else {
                Object[] args = PigJrubyLibrary.pigToRuby(ruby, tuple).toArray();
                if (args.length >= numRequiredArgs && (numOptionalArgs == -1 || args.length <= numRequiredArgs + numOptionalArgs)) {
                    rubyResult = rubyEngine.callMethod(funcReceiver, methodName, args, IRubyObject.class);
                } else {
                    String s = "Method " + methodName + " requires " + numRequiredArgs + " arguments and ";
                    s += ( numOptionalArgs == -1 ? "unlimitated " : numOptionalArgs ) +" optional arguments. ";
                    s += "Instead, " + args.length + " arguments given.";
                    throw new RuntimeException(s);
                }
            }
            return PigJrubyLibrary.rubyToPig(rubyResult);
        } catch (Exception e) {
            throw new IOException("Error executing function",  e);
        }
    }

    /**
     * This method uses the schema method of the function encapsulation object to get the Schema information for
     * the Ruby method.
     */
    @Override
    public Schema outputSchema(Schema input) {
        if (!isInitialized)
            initialize();
        RubySchema rs = PigJrubyLibrary.pigToRuby(ruby, input);
        return PigJrubyLibrary.rubyToPig(rubyEngine.callMethod(funcInfoEncapsulator, "schema", new Object[]{rs, funcReceiver}, RubySchema.class));
    }
}
