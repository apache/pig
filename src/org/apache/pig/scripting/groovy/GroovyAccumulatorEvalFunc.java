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

import java.io.IOException;

import org.apache.pig.AccumulatorEvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class GroovyAccumulatorEvalFunc extends AccumulatorEvalFunc<Object> {

  private GroovyEvalFuncObject groovyAccumulate;
  private GroovyEvalFuncObject groovyGetValue;
  private GroovyEvalFuncObject groovyCleanup;

  public GroovyAccumulatorEvalFunc(String path, String namespace, String accumulatorMethod, String accumulateMethod,
      String getValueMethod, String cleanupMethod) throws IOException {
    //
    // Use the same invocation target for accumulate/getValue/cleanup
    //
    this.groovyAccumulate = new GroovyEvalFuncObject(path, namespace, accumulateMethod);
    this.groovyGetValue = new GroovyEvalFuncObject(path, namespace, getValueMethod, this.groovyAccumulate.getInvocationTarget());
    this.groovyCleanup = new GroovyEvalFuncObject(path, namespace, cleanupMethod, this.groovyAccumulate.getInvocationTarget());
  }

  @Override
  public void accumulate(Tuple b) throws IOException {
    this.groovyAccumulate.exec(b);
  }

  @Override
  public void cleanup() {
    try {
      this.groovyCleanup.exec(null);
    } catch (IOException ioe) {
    }
  }

  @Override
  public Object getValue() {
    try {
      return this.groovyGetValue.exec(null);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public Schema outputSchema(Schema input) {
    return this.groovyGetValue.outputSchema(input);
  }
}
