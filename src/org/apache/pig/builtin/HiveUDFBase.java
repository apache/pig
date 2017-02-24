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
package org.apache.pig.builtin;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.Collector;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.shims.Hadoop23Shims;
import org.apache.hadoop.hive.shims.HadoopShimsSecure;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRConfiguration;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.impl.util.hive.HiveUtils;
import org.apache.pig.tools.pigstats.PigStatusReporter;

import com.esotericsoftware.kryo.Serializer;

abstract class HiveUDFBase extends EvalFunc<Object> {

    static protected class ConstantObjectInspectInfo {
        ConstantObjectInspector[] constants;
        static ConstantObjectInspectInfo parse(String params) throws IOException {
            ConstantObjectInspectInfo info = new ConstantObjectInspectInfo();
            params = params.replaceAll("\"", "'");
            Object constant = Utils.parseConstant(params);
            if (DataType.findType(constant) == DataType.TUPLE) {
                Tuple t = (Tuple)constant;
                info.constants = new ConstantObjectInspector[t.size()];
                for (int i=0;i<t.size();i++) {
                    if (t.get(i) != null) {
                        info.constants[i] = HiveUtils.getConstantObjectInspector(t.get(i));
                    }
                }
            } else {
                info.constants = new ConstantObjectInspector[1];
                info.constants[0] = HiveUtils.getConstantObjectInspector(constant);
            }
            return info;
        }
        boolean isEmpty() {
            return (constants == null);
        }
        int size() {
            return constants.length;
        }
        ConstantObjectInspector get(int i) {
            return constants[i];
        }
        void injectConstantObjectInspector(StructObjectInspector inputObjectInspector) {
            if (!isEmpty()) {
                for (int i=0;i<size();i++) {
                    if (get(i)!=null) {
                        StructField origField = inputObjectInspector.getAllStructFieldRefs().get(i);
                        StructField newfield = new HiveUtils.Field(origField.getFieldName(), get(i), i);
                        ((List<HiveUtils.Field>)inputObjectInspector.getAllStructFieldRefs()).set(i, (HiveUtils.Field)newfield);
                    }
                }
            }
        }
    }

    static protected Class resolveFunc(String funcName) throws IOException {
        String className = funcName;
        Class udfClass;
        if (FunctionRegistry.getFunctionNames().contains(funcName)) {
            FunctionInfo func;
            try {
                func = FunctionRegistry.getFunctionInfo(funcName);
            } catch (SemanticException e) {
                throw new IOException(e);
            }
            udfClass = func.getFunctionClass();
        } else {
            udfClass = PigContext.resolveClassName(className);
            if (udfClass == null) {
                throw new IOException("Cannot find Hive UDF " + funcName);
            }
        }
        return udfClass;
    }

    /**
     * A constant of Reporter type that does nothing.
     */
    static protected class HiveReporter implements Reporter {
        PigStatusReporter rep;
        HiveReporter(PigStatusReporter rep) {
            this.rep = rep;
        }
        public void setStatus(String s) {
            rep.setStatus(s);
        }
        public void progress() {
            rep.progress();
        }
        public Counter getCounter(Enum<?> name) {
            try {
                Counters counters = new Counters();
                counters.incrCounter(name, rep.getCounter(name).getValue());
                return counters.findCounter(name);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        public Counter getCounter(String group, String name) {
            try {
                Counters counters = new Counters();
                counters.incrCounter(group, name, rep.getCounter(group, name).getValue());
                return counters.findCounter(group, name);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        public void incrCounter(Enum<?> key, long amount) {
            rep.incrCounter(key, amount);
        }
        public void incrCounter(String group, String counter, long amount) {
            rep.incrCounter(group, counter, amount);
        }
        public InputSplit getInputSplit() throws UnsupportedOperationException {
          throw new UnsupportedOperationException("NULL reporter has no input");
        }
        public float getProgress() {
          return 0;
        }
    };

    protected static MapredContext instantiateMapredContext() {
        Configuration conf = UDFContext.getUDFContext().getJobConf();
        boolean isMap = conf.getBoolean(MRConfiguration.TASK_IS_MAP, false);
        if (conf.get("exectype").startsWith("TEZ")) {
            isMap = true;
            HiveConf.setVar(conf, ConfVars.HIVE_EXECUTION_ENGINE, "tez");
        }
        MapredContext context = MapredContext.init(isMap, new JobConf(UDFContext.getUDFContext().getJobConf()));
        context.setReporter(new HiveReporter(PigStatusReporter.getInstance()));
        return context;
    }

    @Override
    public List<String> getShipFiles() {
        List<String> files = FuncUtils.getShipFiles(new Class[] {GenericUDF.class,
                PrimitiveObjectInspector.class, HiveConf.class, Serializer.class, ShimLoader.class,
                Hadoop23Shims.class, HadoopShimsSecure.class, Collector.class});
        return files;
    }

    static protected String getErrorMessage(Class c) {
        StringBuffer message = new StringBuffer("Please declare " + c.getName() + " as ");
        if (UDF.class.isAssignableFrom(c) || GenericUDF.class.isAssignableFrom(c)) {
            message.append(HiveUDF.class.getName());
        } else if (GenericUDTF.class.isAssignableFrom(c)) {
            message.append(HiveUDTF.class.getName());
        } else if (UDAF.class.isAssignableFrom(c) || GenericUDAFResolver.class.isAssignableFrom(c)) {
            message.append(HiveUDAF.class.getName());
        } else {
            message = new StringBuffer(c.getName() + " is not Hive UDF");
        }
        return message.toString();
    }
}
