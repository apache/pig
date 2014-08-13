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

import groovy.util.ResourceException;
import groovy.util.ScriptException;

import java.io.File;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.pig.EvalFunc;
import org.apache.pig.builtin.OutputSchema;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.parser.ParserException;
import org.apache.pig.scripting.ScriptEngine;

public class GroovyEvalFunc<T> extends EvalFunc<T> {

  private Schema schema = null;

  private GroovyEvalFunc schemaFunction = null;

  protected Method method = null;

  private static Map<String, Class> scriptClasses = new ConcurrentHashMap<String, Class>();

  private Object invocationTarget;

  public GroovyEvalFunc() {
  }

  public GroovyEvalFunc(String path, String namespace, String methodName) throws IOException {
    this(path, namespace, methodName, null);
  }

  public GroovyEvalFunc(String path, String namespace, String methodName, Object target) throws IOException {
    String fqmn = "".equals(namespace) ? methodName : namespace + ScriptEngine.NAMESPACE_SEPARATOR + methodName;

    Class c = scriptClasses.get(path);

    if (null == c) {
      try {
        c = GroovyScriptEngine.getEngine().loadScriptByName(new File(path).toURI().toString());
      } catch (ScriptException se) {
        throw new IOException(se);
      } catch (ResourceException re) {
        throw new IOException(re);
      }
    }

    scriptClasses.put(path, c);

    Method[] methods = c.getMethods();

    int matches = 0;

    for (Method m : methods) {
      if (m.getName().equals(methodName)) {
        this.method = m;
        matches++;
      }
    }

    if (null == this.method) {
      throw new IOException("Method " + methodName + " was not found in '" + path + "'");
    }

    if (matches > 1) {
      throw new IOException("There are " + matches + " methods with name '" + methodName + "', please make sure method names are unique within the Groovy class.");
    }

    //
    // Extract schema
    //

    Annotation[] annotations = this.method.getAnnotations();

    for (Annotation annotation : annotations) {
      if (annotation.annotationType().equals(OutputSchemaFunction.class)) {
        this.schemaFunction = new GroovyEvalFuncObject(path, namespace, ((OutputSchemaFunction) annotation).value());
        break;
      } else if (annotation.annotationType().equals(OutputSchema.class)) {
        this.schema = Utils.getSchemaFromString(((OutputSchema) annotation).value());
        break;
      }
    }

    //
    // For static method, invocation target is null, for non
    // static method, create/set invocation target unless passed
    // to the constructor
    //

    if (!Modifier.isStatic(this.method.getModifiers())) {
      if (null != target) {
        this.invocationTarget = target;
      } else {
        try {
          this.invocationTarget = c.newInstance();
        } catch (InstantiationException ie) {
          throw new IOException(ie);
        } catch (IllegalAccessException iae) {
          throw new IOException(iae);
        }
      }
    }
  }

  @Override
  public T exec(Tuple input) throws IOException {

    Object[] args = new Object[null != input ? input.size() : 0];

    for (int i = 0; i < args.length; i++) {
      args[i] = GroovyUtils.pigToGroovy(input.get(i));
    }

    try {
      if (this.method.getReturnType().equals(Void.TYPE)) {
        //
        // Invoke method but return null if method is 'void',
        // this is done so we can wrap 'accumulate' and 'cleanup' methods too.
        //
        this.method.invoke(this.invocationTarget, args);
        return null;
      } else {
        return (T) GroovyUtils.groovyToPig(this.method.invoke(this.invocationTarget, args));
      }
    } catch (InvocationTargetException ite) {
      throw new IOException(ite);
    } catch (IllegalAccessException iae) {
      throw new IOException(iae);
    }
  }

  @Override
  public Schema outputSchema(Schema input) {
    if (null != this.schemaFunction) {
      try {
        Tuple t = TupleFactory.getInstance().newTuple(1);
        // Strip enclosing '{}' from schema
        t.set(0, input.toString().replaceAll("^\\{", "").replaceAll("\\}$", ""));
        return Utils.getSchemaFromString((String) this.schemaFunction.exec(t));
      } catch (ParserException pe) {
        throw new RuntimeException(pe);
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    } else {
      return this.schema;
    }
  }

  public Object getInvocationTarget() {
    return this.invocationTarget;
  }
}
