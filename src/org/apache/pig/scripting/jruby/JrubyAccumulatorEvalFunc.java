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

import org.apache.pig.AccumulatorEvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.scripting.jruby.JrubyScriptEngine.RubyFunctions;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import org.jruby.Ruby;
import org.jruby.embed.ScriptingContainer;
import org.jruby.runtime.builtin.IRubyObject;

/**
 * This class provides a bridge between Ruby classes that extend AccumulatorPigUdf
 * and their execution in Pig. This class passes a Bag of data to the Ruby "exec"
 * function, and ultimate gets the value by calling "get" on the class instance
 * that receives methods.
 */
public class JrubyAccumulatorEvalFunc extends AccumulatorEvalFunc<Object> {
    private Object methodReceiver;
    private Object classObject;
    private boolean isInitialized = false;
    private String path;
    private String methodName;

    private static final ScriptingContainer rubyEngine = JrubyScriptEngine.rubyEngine;
    private static final Ruby ruby = rubyEngine.getProvider().getRuntime();

    // It is meaningless to instantiate this class without a path and a method
    private JrubyAccumulatorEvalFunc() {}

    /**
     * This constructor is used by JrubyScriptEngine to register a Ruby class as an Accumulator.
     * The path and methodName are used to find the ruby Class, which is then instantated and used.
     */
    public JrubyAccumulatorEvalFunc(String path, String methodName) {
        this.path = path;
        this.methodName = methodName;
    }

    /**
     * This function intializes the object that receives method calls, and the class object that
     * has schema information. While this is only 3 lines, it is split out so that the schema
     * function can initialize it if necessary. The class object is pulled from the ruby script
     * registered at the path per the RubyFunctions helper methods.
     */
    private void initialize() {
        classObject = RubyFunctions.getFunctions("accumulator", path).get(methodName);
        methodReceiver = rubyEngine.callMethod(classObject, "new");
        isInitialized = true;
    }

    /**
     * This uses the "exec" method required of AccumulatorPigUdf Ruby classes. It streams the data bags
     * it receives through the exec method defined on the registered class.
     */
    @Override
    public void accumulate(Tuple b) throws IOException {
        if (!isInitialized)
            initialize();
        RubyDataBag db = new RubyDataBag(ruby, ruby.getClass("DataBag"), (DataBag)b.get(0));
        rubyEngine.callMethod(methodReceiver, "exec", db, IRubyObject.class);
    }

    @Override
    public void cleanup() {
        isInitialized = false;
        methodReceiver = null;
    }

   /**
    * This method calls "get" on the AccumulatorPigUdf Ruby class that was specified.
    */
    @Override
    public Object getValue()  {
        IRubyObject rubyResult = rubyEngine.callMethod(methodReceiver, "get", IRubyObject.class);
        try {
            return PigJrubyLibrary.rubyToPig(rubyResult);
        } catch (ExecException e) {
            throw new RuntimeException("Unable to convert result from Ruby to Pig: " + rubyResult, e);
        }
    }

    /**
     * This provides the Schema of the output, and leverages the get_output_schema function on the class object
     * that is defined on the ruby side.
     */
    @Override
    public Schema outputSchema(Schema input) {
        if (!isInitialized)
            initialize();
        RubySchema rs = PigJrubyLibrary.pigToRuby(ruby, input);
        return PigJrubyLibrary.rubyToPig(rubyEngine.callMethod(classObject, "get_output_schema", rs, RubySchema.class));
    }
}
