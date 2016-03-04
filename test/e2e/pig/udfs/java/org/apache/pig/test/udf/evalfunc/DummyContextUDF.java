/**
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

package org.apache.pig.test.udf.evalfunc;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reporter;
import org.apache.pig.PigWarning;

@Description(name = "dummycontextudf",
value = "_FUNC_(col) - UDF to report MR counter values")
public class DummyContextUDF extends GenericUDF {

  private MapredContext context;
  private Text result = new Text();

  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
  }

  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Reporter reporter = context.getReporter();
    int age = (Integer)arguments[0].get();
    if (age>50) {
        // Here use PigWarning.UDF_WARNING_1, so it is easier to verify in e2e test
        reporter.incrCounter(PigWarning.UDF_WARNING_1, 1);
    }
    result.set(arguments.toString());
    return result;
  }

  public String getDisplayString(String[] children) {
    return "dummy-func()";
  }

  @Override
    public void configure(MapredContext context) {
    this.context = context;
  }
}

