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
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.hive.HiveUtils;

/**
 * Use Hive UDF or GenericUDF.
 * Example:
 *      define sin HiveUDF('sin');
 *      A = load 'mydata' as (num:double);
 *      B = foreach A generate sin(num);
 * HiveUDF takes an optional second parameter if the Hive UDF require constant parameters
 *      define in_file HiveUDF('in_file', '(null, "names.txt")');
 */
public class HiveUDF extends HiveUDFBase {

    private boolean inited = false;
    private GenericUDF evalUDF;

    static class SchemaInfo {
        private StructObjectInspector inputObjectInspector;
        private ObjectInspector outputObjectInspector;

        private void init(Schema inputSchema, GenericUDF evalUDF, ConstantObjectInspectInfo constantsInfo) throws IOException {
            ResourceSchema rs = new ResourceSchema(inputSchema);
            ResourceFieldSchema wrappedTupleFieldSchema = new ResourceFieldSchema();
            wrappedTupleFieldSchema.setType(DataType.TUPLE);
            wrappedTupleFieldSchema.setSchema(rs);

            TypeInfo ti = HiveUtils.getTypeInfo(wrappedTupleFieldSchema);
            inputObjectInspector = (StructObjectInspector)HiveUtils.createObjectInspector(ti);

            try {
                ObjectInspector[] arguments = new ObjectInspector[inputSchema.size()];
                for (int i=0;i<inputSchema.size();i++) {
                    if (constantsInfo!=null && !constantsInfo.isEmpty() && constantsInfo.get(i)!=null) {
                        arguments[i] = constantsInfo.get(i);
                    } else {
                        arguments[i] = inputObjectInspector.getAllStructFieldRefs().get(i).getFieldObjectInspector();
                    }
                }
                outputObjectInspector = evalUDF.initialize(arguments);
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }

    SchemaInfo schemaInfo = new SchemaInfo();

    ConstantObjectInspectInfo constantsInfo;

    public HiveUDF(String funcName) throws IOException, InstantiationException, IllegalAccessException {
        Class hiveUDFClass = resolveFunc(funcName);
        if (UDF.class.isAssignableFrom(hiveUDFClass)) {
            evalUDF =  new GenericUDFBridge(funcName, false, hiveUDFClass.getName());
        } else if (GenericUDF.class.isAssignableFrom(hiveUDFClass)){
            evalUDF = (GenericUDF)hiveUDFClass.newInstance();
        } else {
            throw new IOException(getErrorMessage(hiveUDFClass));
        }
    }

    public HiveUDF(String funcName, String params) throws IOException, InstantiationException, IllegalAccessException {
        this(funcName);
        constantsInfo = ConstantObjectInspectInfo.parse(params);
    }

    @Override
    public Object exec(Tuple input) throws IOException {
        if (!inited) {
            evalUDF.configure(instantiateMapredContext());
            schemaInfo.init(getInputSchema(), evalUDF, constantsInfo);
            inited = true;
        }
        List inputs = schemaInfo.inputObjectInspector.getStructFieldsDataAsList(input);
        DeferredObject[] arguments = new DeferredObject[inputs.size()];
        for (int i=0 ; i<inputs.size() ; i++) {
            arguments[i] = new DeferredJavaObject(inputs.get(i));
        }
        try {
            Object returnValue = evalUDF.evaluate(arguments);
            return HiveUtils.convertHiveToPig(returnValue, schemaInfo.outputObjectInspector, null);
        } catch (HiveException e) {
            throw new IOException(e);
        }
    }

    @Override
    public List<String> getShipFiles() {
        try {
            if (!inited) {
                schemaInfo.init(getInputSchema(), evalUDF, constantsInfo);
                inited = true;
            }
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
        List<String> files = super.getShipFiles();
        if (evalUDF.getRequiredFiles() != null) {
            files.addAll(Arrays.asList(evalUDF.getRequiredFiles()));
        }
        if (evalUDF.getRequiredJars() != null) {
            files.addAll(Arrays.asList(evalUDF.getRequiredJars()));
        }

        return files;
    }

    @Override
    public Schema outputSchema(Schema input) {
        try {
            if (!inited) {
                schemaInfo.init(getInputSchema(), evalUDF, constantsInfo);
                inited = true;
            }
            ResourceFieldSchema rfs = HiveUtils.getResourceFieldSchema(
                    TypeInfoUtils.getTypeInfoFromObjectInspector(schemaInfo.outputObjectInspector));
            ResourceSchema outputSchema = new ResourceSchema();
            outputSchema.setFields(new ResourceFieldSchema[] {rfs});
            return Schema.getPigSchema(outputSchema);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void finish() {
        try {
            evalUDF.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
