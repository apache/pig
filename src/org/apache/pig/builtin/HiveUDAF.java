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
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver2;
import org.apache.hadoop.hive.ql.udf.generic.SimpleGenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.util.hive.HiveUtils;

/**
 * Use Hive UDAF or GenericUDAF.
 * Example:
 *      define avg HiveUDAF('avg');
 *      A = load 'mydata' as (name:chararray, num:double);
 *      B = group A by name;
 *      C = foreach B generate group, avg(A.num);
 */
public class HiveUDAF extends HiveUDFBase implements Algebraic {

    private boolean inited = false;
    private String funcName;
    private String params;
    private GenericUDAFResolver udaf;

    static class SchemaAndEvaluatorInfo {
        private TypeInfo inputTypeInfo;
        private TypeInfo outputTypeInfo;
        private TypeInfo intermediateOutputTypeInfo;
        private ObjectInspector[] inputObjectInspectorAsArray;
        private ObjectInspector[] intermediateInputObjectInspectorAsArray;
        private StructObjectInspector inputObjectInspector;
        private ObjectInspector intermediateInputObjectInspector;
        private ObjectInspector intermediateOutputObjectInspector;
        private ObjectInspector outputObjectInspector;
        private GenericUDAFEvaluator evaluator;

        private static TypeInfo getInputTypeInfo(Schema inputSchema) throws IOException {
            FieldSchema innerFieldSchema = inputSchema.getField(0).schema.getField(0);
            ResourceFieldSchema rfs = new ResourceFieldSchema(innerFieldSchema);

            TypeInfo inputTypeInfo = HiveUtils.getTypeInfo(rfs);
            return inputTypeInfo;
        }

        private static ObjectInspector[] getInputObjectInspectorAsArray(TypeInfo inputTypeInfo,
                ConstantObjectInspectInfo constantsInfo) throws IOException {

            StructObjectInspector inputObjectInspector = (StructObjectInspector)HiveUtils.createObjectInspector(inputTypeInfo);

            ObjectInspector[] arguments = new ObjectInspector[inputObjectInspector.getAllStructFieldRefs().size()];
            for (int i=0;i<inputObjectInspector.getAllStructFieldRefs().size();i++) {
                if (constantsInfo!=null && constantsInfo.get(i)!=null) {
                    arguments[i] = constantsInfo.get(i);
                } else {
                    arguments[i] = inputObjectInspector.getAllStructFieldRefs().get(i).getFieldObjectInspector();
                }
            }
            return arguments;
        }

        private static GenericUDAFEvaluator getEvaluator(TypeInfo inputTypeInfo, GenericUDAFResolver udaf,
                ConstantObjectInspectInfo constantsInfo) throws IOException {
            try {
                GenericUDAFEvaluator evaluator;
                ObjectInspector[] arguments = getInputObjectInspectorAsArray(inputTypeInfo, constantsInfo);
                
                if (udaf instanceof GenericUDAFResolver2) {
                    GenericUDAFParameterInfo paramInfo =
                            new SimpleGenericUDAFParameterInfo(
                                    arguments, false, false);
                    evaluator = ((GenericUDAFResolver2)udaf).getEvaluator(paramInfo);
                } else {
                    TypeInfo[] params = ((StructTypeInfo)inputTypeInfo)
                            .getAllStructFieldTypeInfos().toArray(new TypeInfo[0]);
                    evaluator = udaf.getEvaluator(params);
                }
                return evaluator;
            } catch (Exception e) {
                throw new IOException(e);
            }
        }

        private void init(Schema inputSchema, GenericUDAFResolver udaf, Mode m, ConstantObjectInspectInfo constantsInfo) throws IOException {
            try {
                inputTypeInfo = getInputTypeInfo(inputSchema);
                inputObjectInspector = (StructObjectInspector)HiveUtils.createObjectInspector(inputTypeInfo);
                if (constantsInfo!=null) {
                    constantsInfo.injectConstantObjectInspector(inputObjectInspector);
                }
                inputObjectInspectorAsArray = getInputObjectInspectorAsArray(inputTypeInfo, constantsInfo);
                evaluator = getEvaluator(inputTypeInfo, udaf, constantsInfo);

                if (m == Mode.COMPLETE) {
                    outputObjectInspector = evaluator.init(Mode.COMPLETE, inputObjectInspectorAsArray);
                    outputTypeInfo = TypeInfoUtils.getTypeInfoFromObjectInspector(outputObjectInspector);
                    return;
                }

                if (m == Mode.PARTIAL1 || m == Mode.FINAL) {
                    intermediateOutputObjectInspector = evaluator.init(Mode.PARTIAL1, inputObjectInspectorAsArray);
                    intermediateOutputTypeInfo = TypeInfoUtils.getTypeInfoFromObjectInspector(intermediateOutputObjectInspector);

                    if (m == Mode.FINAL) {
                        intermediateInputObjectInspector = HiveUtils.createObjectInspector(intermediateOutputTypeInfo);
                        intermediateInputObjectInspectorAsArray = new ObjectInspector[] {intermediateInputObjectInspector};
                        outputObjectInspector = evaluator.init(Mode.FINAL, intermediateInputObjectInspectorAsArray);
                        outputTypeInfo = TypeInfoUtils.getTypeInfoFromObjectInspector(outputObjectInspector);
                    }
                }
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }

    SchemaAndEvaluatorInfo schemaAndEvaluatorInfo = new SchemaAndEvaluatorInfo();

    ConstantObjectInspectInfo constantsInfo;

    public HiveUDAF(String funcName) throws IOException, InstantiationException, IllegalAccessException {
        this.funcName = funcName;
        this.udaf = instantiateUDAF(funcName);
    }

    public HiveUDAF(String funcName, String params) throws IOException, InstantiationException, IllegalAccessException {
        this(funcName);
        constantsInfo = ConstantObjectInspectInfo.parse(params);
        this.params = params;
    }

    private static GenericUDAFResolver instantiateUDAF(String funcName) throws IOException, InstantiationException, IllegalAccessException {
        GenericUDAFResolver udaf;
        Class hiveUDAFClass = resolveFunc(funcName);
        if (UDAF.class.isAssignableFrom(hiveUDAFClass)) {
            udaf =  new GenericUDAFBridge((UDAF)hiveUDAFClass.newInstance());
        } else if (GenericUDAFResolver.class.isAssignableFrom(hiveUDAFClass)){
            udaf = (GenericUDAFResolver)hiveUDAFClass.newInstance();
        } else {
            throw new IOException(getErrorMessage(hiveUDAFClass));
        }
        return udaf;
    }

    @Override
    public String getInitial() {
        if (params == null) {
            return Initial.class.getName() + "('" + funcName + "')";
        } else {
            return Initial.class.getName() + "('" + funcName + "," + params + "')";
        }
    }

    @Override
    public String getIntermed() {
        if (params == null) {
            return Intermediate.class.getName() + "('" + funcName + "')";
        } else {
            return Intermediate.class.getName() + "('" + funcName + "," + params + "')";
        }
    }

    @Override
    public String getFinal() {
        if (params == null) {
            return Final.class.getName() + "('" + funcName + "')";
        } else {
            return Final.class.getName() + "('" + funcName + "," + params + "')";
        }
    }

    static public class Initial extends EvalFunc<Tuple> {
        public Initial(String funcName) {
        }
        public Initial(String funcName, String params) {
        }
        @Override
        public Tuple exec(Tuple input) throws IOException {

            DataBag bg = (DataBag) input.get(0);
            Tuple tp = null;
            if(bg.iterator().hasNext()) {
                tp = bg.iterator().next();
            }

            return tp;
        }
    }

    static public class Intermediate extends EvalFunc<Tuple> {

        private boolean inited = false;
        private String funcName;
        ConstantObjectInspectInfo constantsInfo;
        private SchemaAndEvaluatorInfo schemaAndEvaluatorInfo = new SchemaAndEvaluatorInfo();
        private static TupleFactory tf = TupleFactory.getInstance();

        public Intermediate(String funcName) {
            this.funcName = funcName;
        }
        public Intermediate(String funcName, String params) throws IOException {
            this.funcName = funcName;
            constantsInfo = ConstantObjectInspectInfo.parse(params);
        }
        @Override
        public Tuple exec(Tuple input) throws IOException {
            try {
                if (!inited) {
                    schemaAndEvaluatorInfo.init(getInputSchema(), instantiateUDAF(funcName), Mode.PARTIAL1, constantsInfo);
                    inited = true;
                }
                DataBag b = (DataBag)input.get(0);
                AggregationBuffer agg = schemaAndEvaluatorInfo.evaluator.getNewAggregationBuffer();
                for (Iterator<Tuple> it = b.iterator(); it.hasNext();) {
                    Tuple t = it.next();
                    List inputs = schemaAndEvaluatorInfo.inputObjectInspector.getStructFieldsDataAsList(t);
                    schemaAndEvaluatorInfo.evaluator.iterate(agg, inputs.toArray());
                }
                Object returnValue = schemaAndEvaluatorInfo.evaluator.terminatePartial(agg);
                Tuple result = tf.newTuple();
                result.append(HiveUtils.convertHiveToPig(returnValue, schemaAndEvaluatorInfo.intermediateOutputObjectInspector, null));
                return result;
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }

    static public class Final extends EvalFunc<Object> {

        private boolean inited = false;
        private String funcName;
        ConstantObjectInspectInfo constantsInfo;
        private SchemaAndEvaluatorInfo schemaAndEvaluatorInfo = new SchemaAndEvaluatorInfo();

        public Final(String funcName) {
            this.funcName = funcName;
        }
        public Final(String funcName, String params) throws IOException {
            this.funcName = funcName;
            constantsInfo = ConstantObjectInspectInfo.parse(params);
        }
        @Override
        public Object exec(Tuple input) throws IOException {
            try {
                if (!inited) {
                    schemaAndEvaluatorInfo.init(getInputSchema(), instantiateUDAF(funcName), Mode.FINAL, constantsInfo);
                    schemaAndEvaluatorInfo.evaluator.configure(instantiateMapredContext());
                    inited = true;
                }
                DataBag b = (DataBag)input.get(0);
                AggregationBuffer agg = schemaAndEvaluatorInfo.evaluator.getNewAggregationBuffer();
                for (Iterator<Tuple> it = b.iterator(); it.hasNext();) {
                    Tuple t = it.next();
                    schemaAndEvaluatorInfo.evaluator.merge(agg, t.get(0));
                }

                Object returnValue = schemaAndEvaluatorInfo.evaluator.terminate(agg);
                Object result = HiveUtils.convertHiveToPig(returnValue, schemaAndEvaluatorInfo.outputObjectInspector, null);
                return result;
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }

    @Override
    public Object exec(Tuple input) throws IOException {
        try {
            if (!inited) {
                schemaAndEvaluatorInfo.init(getInputSchema(), instantiateUDAF(funcName), Mode.COMPLETE, constantsInfo);
                inited = true;
            }
            AggregationBuffer agg = schemaAndEvaluatorInfo.evaluator.getNewAggregationBuffer();
            DataBag bg = (DataBag) input.get(0);
            Tuple tp = null;
            for (Iterator<Tuple> it = bg.iterator(); it.hasNext();) {
                tp = it.next();
                List inputs = schemaAndEvaluatorInfo.inputObjectInspector.getStructFieldsDataAsList(tp);
                schemaAndEvaluatorInfo.evaluator.iterate(agg, inputs.toArray());
            }
            Object returnValue = schemaAndEvaluatorInfo.evaluator.terminate(agg);
            Object result = HiveUtils.convertHiveToPig(returnValue, schemaAndEvaluatorInfo.outputObjectInspector, null);
            return result;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public Schema outputSchema(Schema input) {
        try {
            if (!inited) {
                schemaAndEvaluatorInfo.init(getInputSchema(), instantiateUDAF(funcName), Mode.COMPLETE, constantsInfo);
                inited = true;
            }

            ResourceFieldSchema rfs = HiveUtils.getResourceFieldSchema(schemaAndEvaluatorInfo.outputTypeInfo);
            ResourceSchema outputSchema = new ResourceSchema();
            outputSchema.setFields(new ResourceFieldSchema[] {rfs});
            return Schema.getPigSchema(outputSchema);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
