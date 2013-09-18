/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pig.scripting.groovy;

import groovy.lang.Tuple;
import groovy.util.ResourceException;
import groovy.util.ScriptException;

import java.io.File;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigServer;
import org.apache.pig.builtin.OutputSchema;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.scripting.ScriptEngine;
import org.apache.pig.scripting.groovy.GroovyAlgebraicEvalFunc.BigDecimalGroovyAlgebraicEvalFunc;
import org.apache.pig.scripting.groovy.GroovyAlgebraicEvalFunc.BigIntegerGroovyAlgebraicEvalFunc;
import org.apache.pig.scripting.groovy.GroovyAlgebraicEvalFunc.BooleanGroovyAlgebraicEvalFunc;
import org.apache.pig.scripting.groovy.GroovyAlgebraicEvalFunc.ChararrayGroovyAlgebraicEvalFunc;
import org.apache.pig.scripting.groovy.GroovyAlgebraicEvalFunc.DataBagGroovyAlgebraicEvalFunc;
import org.apache.pig.scripting.groovy.GroovyAlgebraicEvalFunc.DataByteArrayGroovyAlgebraicEvalFunc;
import org.apache.pig.scripting.groovy.GroovyAlgebraicEvalFunc.DateTimeGroovyAlgebraicEvalFunc;
import org.apache.pig.scripting.groovy.GroovyAlgebraicEvalFunc.DoubleGroovyAlgebraicEvalFunc;
import org.apache.pig.scripting.groovy.GroovyAlgebraicEvalFunc.FloatGroovyAlgebraicEvalFunc;
import org.apache.pig.scripting.groovy.GroovyAlgebraicEvalFunc.IntegerGroovyAlgebraicEvalFunc;
import org.apache.pig.scripting.groovy.GroovyAlgebraicEvalFunc.LongGroovyAlgebraicEvalFunc;
import org.apache.pig.scripting.groovy.GroovyAlgebraicEvalFunc.MapGroovyAlgebraicEvalFunc;
import org.apache.pig.scripting.groovy.GroovyAlgebraicEvalFunc.TupleGroovyAlgebraicEvalFunc;
import org.apache.pig.tools.pigstats.PigStats;

public class GroovyScriptEngine extends ScriptEngine {

  private static final Log LOG = LogFactory.getLog(GroovyScriptEngine.class);

  private static groovy.util.GroovyScriptEngine gse;

  private static boolean isInitialized = false;

  static {
    try {
      gse = new groovy.util.GroovyScriptEngine("");
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  protected Map<String, List<PigStats>> main(PigContext context, String scriptFile) throws IOException {

    PigServer pigServer = new PigServer(context, false);

    //
    // Register dependencies
    //

    String groovyJar = getJarPath(groovy.util.GroovyScriptEngine.class);

    if (null != groovyJar) {
        pigServer.registerJar(groovyJar);
    }

    //
    // Register UDFs
    //

    registerFunctions(scriptFile, null, context);

    try {

      //
      // Load the script
      //

      Class c = gse.loadScriptByName(new File(scriptFile).toURI().toString());

      //
      // Extract the main method
      //

      Method main = c.getMethod("main", String[].class);

      if (null == main || !Modifier.isStatic(main.getModifiers()) || !Modifier.isPublic(main.getModifiers()) || !Void.TYPE.equals(main.getReturnType())) {
        throw new IOException("No method 'public static void main(String[] args)' was found.");
      }

      //
      // Invoke the main method
      //

      Object[] args = new Object[1];
      String[] argv = (String[])ObjectSerializer.deserialize(context.getProperties().getProperty(PigContext.PIG_CMD_ARGS_REMAINDERS));
      args[0] = argv;

      main.invoke(null, args);

    } catch (Exception e) {
      throw new IOException(e);
    }

    return getPigStatsMap();
  }

  @Override
  public void registerFunctions(String path, String namespace, PigContext pigContext) throws IOException {

    if (!isInitialized) {
      pigContext.scriptJars.add(getJarPath(groovy.util.GroovyScriptEngine.class));
      isInitialized = true;
    }

    try {
      //
      // Read file
      //

      Class c = gse.loadScriptByName(new File(path).toURI().toString());

      //
      // Keep track of initial/intermed/final methods of Albegraic UDFs
      //

      Map<String, Method[]> algebraicMethods = new HashMap<String, Method[]>();

      //
      // Keep track of accumulate/getValue/cleanup methods of Accumulator UDFs
      //

      Map<String, Method[]> accumulatorMethods = new HashMap<String, Method[]>();

      //
      // Loop over the methods
      //

      Method[] methods = c.getMethods();

      for (Method method : methods) {
        Annotation[] annotations = method.getAnnotations();

        boolean isAccumulator = false;

        if (annotations.length > 0) {
          Schema schema = null;
          String schemaFunction = null;

          for (Annotation annotation : annotations) {
            if (annotation.annotationType().equals(OutputSchema.class)) {
              schema = Utils.getSchemaFromString(((OutputSchema) annotation).value());
            } else if (annotation.annotationType().equals(OutputSchemaFunction.class)) {
              schemaFunction = ((OutputSchemaFunction) annotation).value();
            } else if (isAlgebraic(annotation)) {

              String algebraic = null;

              int idx = 0;

              if (annotation.annotationType().equals(AlgebraicInitial.class)) {
                //
                // Check that method accepts a single Tuple as parameter.
                //
                Class<?>[] params = method.getParameterTypes();
                if (1 != params.length || !Tuple.class.equals(params[0])) {
                  throw new IOException(path + ": methods annotated with @AlgebraicInitial MUST take a single groovy.lang.Tuple as parameter.");
                }
                if (!method.getReturnType().equals(Tuple.class) && !method.getReturnType().equals(Object[].class)) {
                  throw new IOException(path + ":" + method.getName()
                      + " Algebraic UDF Initial method MUST return type groovy.lang.Tuple or Object[].");
                }
                algebraic = ((AlgebraicInitial) annotation).value();
                idx = 0;
              } else if (annotation.annotationType().equals(AlgebraicIntermed.class)) {
                //
                // Check that method accepts a single Tuple as parameter.
                //
                Class<?>[] params = method.getParameterTypes();
                if (1 != params.length || !Tuple.class.equals(params[0])) {
                  throw new IOException(path + ": methods annotated with @AlgebraicIntermed MUST take a single groovy.lang.Tuple as parameter.");
                }
                if (!method.getReturnType().equals(Tuple.class) && !method.getReturnType().equals(Object[].class)) {
                  throw new IOException(path + ":" + method.getName()
                      + " Algebraic UDF Intermed method MUST return type groovy.lang.Tuple or Object[].");
                }
                algebraic = ((AlgebraicIntermed) annotation).value();
                idx = 1;
              } else {
                //
                // Check that method accepts a single Tuple as parameter.
                //
                Class<?>[] params = method.getParameterTypes();
                if (1 != params.length || !Tuple.class.equals(params[0])) {
                  throw new IOException(path + ": methods annotated with @AlgebraicFinal MUST take a single groovy.lang.Tuple as parameter.");
                }
                algebraic = ((AlgebraicFinal) annotation).value();
                idx = 2;
              }

              Method[] algmethods = algebraicMethods.get(algebraic);

              if (null == algmethods) {
                algmethods = new Method[3];
                algebraicMethods.put(algebraic, algmethods);
              }

              if (null != algmethods[idx]) {
                throw new IOException(path + ": Algebraic UDF '" + algebraic + "' already has an "
                    + annotation.annotationType().getSimpleName() + " method defined ('" + algmethods[idx] + "')");
              }

              algmethods[idx] = method;
            } else if (isAccumulator(annotation)) {
              String accumulator = null;

              int idx = 0;

              if (annotation.annotationType().equals(AccumulatorAccumulate.class)) {
                //
                // Check that method accepts a single Tuple as parameter.
                //
                Class<?>[] params = method.getParameterTypes();
                if (1 != params.length || !Tuple.class.equals(params[0])) {
                  throw new IOException(path + ": methods annotated with @AccumulatorAccumulate MUST take a single groovy.lang.Tuple as parameter.");
                }
                accumulator = ((AccumulatorAccumulate) annotation).value();
                idx = 0;
              } else if (annotation.annotationType().equals(AccumulatorGetValue.class)) {
                //
                // Check that method does not accept any parameters.
                //
                Class<?>[] params = method.getParameterTypes();
                if (0 != params.length) {
                  throw new IOException(path + ": methods annotated with @AccumulatorGetValue take no parameters.");
                }
                accumulator = ((AccumulatorGetValue) annotation).value();
                isAccumulator = true;
                idx = 1;
              } else if (annotation.annotationType().equals(AccumulatorCleanup.class)) {
                //
                // Check that method does not accept any parameters.
                //
                Class<?>[] params = method.getParameterTypes();
                if (0 != params.length) {
                  throw new IOException(path + ": methods annotated with @AccumulatorCleanup take no parameters and return void.");
                }
                accumulator = ((AccumulatorCleanup) annotation).value();
                idx = 2;
              }

              Method[] accumethods = accumulatorMethods.get(accumulator);

              if (null == accumethods) {
                accumethods = new Method[3];
                accumulatorMethods.put(accumulator, accumethods);
              }

              if (null != accumethods[idx]) {
                throw new IOException(path + ": Accumulator UDF '" + accumulator + "' already has an "
                    + annotation.annotationType().getSimpleName() + " method defined ('" + accumethods[idx] + "')");
              }

              accumethods[idx] = method;
            }
          }

          //
          // Only register functions which have an output schema declared
          //

          if (null == schema && null == schemaFunction) {
            LOG.info(path
                + ": Only methods annotated with @OutputSchema or @OutputSchemaFunction (but not with @AccumulatorGetValue are exposed to Pig, skipping method '"
                + method.getName() + "'");
            continue;
          }

          //
          // Only one of OutputSchema / OutputSchemaFunction can be defined
          //

          if (null != schema && null != schemaFunction) {
            LOG.info("Annotation @OutputSchemaFunction has precedence over @OutputSchema for method '" + method.getName() + "'");
          }

          //
          // Register methods annotated with 'OutputSchema' or
          // 'OutputSchemaFunction, unless they are accumulators' getValue
          // methods
          //

          if (!isAccumulator) {
            namespace = (namespace == null) ? "" : namespace;
            FuncSpec spec = new FuncSpec(GroovyEvalFuncObject.class.getCanonicalName() + "('" + path + "','" + namespace
                + "','" + method.getName() + "')");
            pigContext.registerFunction(("".equals(namespace) ? "" : (namespace + NAMESPACE_SEPARATOR)) + method.getName(),
                spec);
            LOG.info(path + ": Register Groovy UDF: " + ("".equals(namespace) ? "" : (namespace + NAMESPACE_SEPARATOR))
                + method.getName());
          }
        }
      }

      //
      // Now register algebraic methods
      //

      for (String algebraic : algebraicMethods.keySet()) {

        Method[] algmethods = algebraicMethods.get(algebraic);

        if (null == algmethods[0]) {
          throw new IOException(path + ": Algebratic UDF '" + algebraic + "' does not have an Initial method defined.");
        } else if (null == algmethods[1]) {
          throw new IOException(path + ": Algebratic UDF '" + algebraic + "' does not have an Intermed method defined.");
        } else if (null == algmethods[2]) {
          throw new IOException(path + ": Algebratic UDF '" + algebraic + "' does not have a Final method defined.");
        }

        //
        // Retrieve schema of 'Final' method
        //

        String className = null;

        Class<?> returnType = algmethods[2].getReturnType();

        if (returnType.equals(Tuple.class) || returnType.equals(Object[].class)
            || returnType.equals(org.apache.pig.data.Tuple.class)) {
          className = TupleGroovyAlgebraicEvalFunc.class.getName();
        } else if (returnType.equals(List.class) || returnType.equals(DataBag.class)) {
          className = DataBagGroovyAlgebraicEvalFunc.class.getName();
        } else if (returnType.equals(org.joda.time.DateTime.class)) {
          className = DateTimeGroovyAlgebraicEvalFunc.class.getName();
        } else if (returnType.equals(Boolean.class) || returnType.equals(boolean.class)) {
          className = BooleanGroovyAlgebraicEvalFunc.class.getName();
        } else if (returnType.equals(byte[].class) || returnType.equals(DataByteArray.class)) {
          className = DataByteArrayGroovyAlgebraicEvalFunc.class.getName();
        } else if (returnType.equals(String.class)) {
          className = ChararrayGroovyAlgebraicEvalFunc.class.getName();
        } else if (returnType.equals(Double.class) || returnType.equals(double.class) || returnType.equals(BigDecimal.class)) {
          className = DoubleGroovyAlgebraicEvalFunc.class.getName();
        } else if (returnType.equals(Float.class) || returnType.equals(float.class)) {
          className = FloatGroovyAlgebraicEvalFunc.class.getName();
        } else if (returnType.equals(Byte.class) || returnType.equals(byte.class) || returnType.equals(Short.class)
            || returnType.equals(short.class) || returnType.equals(Integer.class) || returnType.equals(int.class)) {
          className = IntegerGroovyAlgebraicEvalFunc.class.getName();
        } else if (returnType.equals(Long.class) || returnType.equals(long.class) || returnType.equals(BigInteger.class)) {
          className = LongGroovyAlgebraicEvalFunc.class.getName();
        } else if (returnType.equals(Map.class)) {
          className = MapGroovyAlgebraicEvalFunc.class.getName();
        } else if (returnType.equals(BigDecimal.class)) {
          className = BigDecimalGroovyAlgebraicEvalFunc.class.getName();
        } else if (returnType.equals(BigInteger.class)) {
          className = BigIntegerGroovyAlgebraicEvalFunc.class.getName();
        } else {
          throw new RuntimeException(path + ": Unknown return type for Algebraic UDF '" + algebraic + "'");
        }

        FuncSpec spec = new FuncSpec(className + "('" + path + "','" + namespace + "','" + algebraic + "','"
            + algmethods[0].getName() + "','" + algmethods[1].getName() + "','" + algmethods[2].getName() + "')");
        pigContext.registerFunction(("".equals(namespace) ? "" : (namespace + NAMESPACE_SEPARATOR)) + algebraic, spec);

        LOG.info("Register Groovy Algebraic UDF: " + ("".equals(namespace) ? "" : (namespace + NAMESPACE_SEPARATOR))
            + algebraic);
      }

      //
      // Now register Accumulator UDFs
      //

      for (String accumulator : accumulatorMethods.keySet()) {

        Method[] accumethods = accumulatorMethods.get(accumulator);

        if (null == accumethods[0]) {
          throw new IOException(path + ": Accumulator UDF '" + accumulator + "' does not have an Accumulate method defined.");
        } else if (null == accumethods[1]) {
          throw new IOException(path + ": Accumulator UDF '" + accumulator + "' does not have a GetValue method defined.");
        } else if (null == accumethods[2]) {
          throw new IOException(path + ": Accumulator UDF '" + accumulator + "' does not have a Cleanup method defined.");
        }

        FuncSpec spec = new FuncSpec(GroovyAccumulatorEvalFunc.class.getName() + "('" + path + "','" + namespace + "','"
            + accumulator + "','" + accumethods[0].getName() + "','" + accumethods[1].getName() + "','"
            + accumethods[2].getName() + "')");
        pigContext.registerFunction(("".equals(namespace) ? "" : (namespace + NAMESPACE_SEPARATOR)) + accumulator, spec);

        LOG.info("Register Groovy Accumulator UDF: " + ("".equals(namespace) ? "" : (namespace + NAMESPACE_SEPARATOR))
            + accumulator);
      }
    } catch (ResourceException re) {
      throw new IOException(re);
    } catch (ScriptException se) {
      throw new IOException(se);
    }

  }

  @Override
  protected Map<String, Object> getParamsFromVariables() throws IOException {
    return null;
  }

  @Override
  protected String getScriptingLang() {
    return "groovy";
  }

  protected static groovy.util.GroovyScriptEngine getEngine() {
    return gse;
  }

  private static boolean isAlgebraic(Annotation annotation) {
    return annotation.annotationType().equals(AlgebraicInitial.class)
           || annotation.annotationType().equals(AlgebraicIntermed.class)
           || annotation.annotationType().equals(AlgebraicFinal.class);
  }

  private static boolean isAccumulator(Annotation annotation) {
    return annotation.annotationType().equals(AccumulatorAccumulate.class)
           || annotation.annotationType().equals(AccumulatorGetValue.class)
           || annotation.annotationType().equals(AccumulatorCleanup.class);
  }
}
