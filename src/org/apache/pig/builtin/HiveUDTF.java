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

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.Collector;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.hive.HiveUtils;

/**
 * Use Hive GenericUDTF.
 * Example:
 *      define explode HiveUDTF('explode');
 *      A = load 'mydata' as (a0:{(b0:chararray)});
 *      B = foreach A generate flatten(explode(a0));
 */
public class HiveUDTF extends HiveUDFBase {

    private boolean inited = false;
    private GenericUDTF udtf;
    private boolean endOfAllInput = false;

    static class SchemaInfo {
        StructObjectInspector inputObjectInspector;
        ObjectInspector outputObjectInspector;
        private void init(Schema inputSchema, GenericUDTF udtf, ConstantObjectInspectInfo constantsInfo) throws IOException {
            ResourceSchema rs = new ResourceSchema(inputSchema);
            ResourceFieldSchema wrappedTupleFieldSchema = new ResourceFieldSchema();
            wrappedTupleFieldSchema.setType(DataType.TUPLE);
            wrappedTupleFieldSchema.setSchema(rs);

            TypeInfo ti = HiveUtils.getTypeInfo(wrappedTupleFieldSchema);
            inputObjectInspector = (StructObjectInspector)HiveUtils.createObjectInspector(ti);
            if (constantsInfo!=null) {
                constantsInfo.injectConstantObjectInspector(inputObjectInspector);
            }

            try {
                outputObjectInspector = udtf.initialize(inputObjectInspector);
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }

    SchemaInfo schemaInfo = new SchemaInfo();

    ConstantObjectInspectInfo constantsInfo;

    private static BagFactory bf = BagFactory.getInstance();
    private HiveUDTFCollector collector = null;

    public HiveUDTF(String funcName) throws InstantiationException, IllegalAccessException, IOException {
        Class hiveUDTFClass = resolveFunc(funcName);
        if (GenericUDTF.class.isAssignableFrom(hiveUDTFClass)) {
            udtf =  (GenericUDTF)hiveUDTFClass.newInstance();
        } else {
            throw new IOException(getErrorMessage(hiveUDTFClass));
        }
    }

    public HiveUDTF(String funcName, String params) throws InstantiationException, IllegalAccessException, IOException {
        this(funcName);
        constantsInfo = ConstantObjectInspectInfo.parse(params);
    }
    @Override
    public Object exec(Tuple input) throws IOException {
        if (!inited) {
            udtf.configure(instantiateMapredContext());
            schemaInfo.init(getInputSchema(), udtf, constantsInfo);
            inited = true;
        }

        if (collector == null) {
            collector = new HiveUDTFCollector();
            udtf.setCollector(collector);
        } else {
            collector.init();
        }

        try {
            if (!endOfAllInput) {
                udtf.process(input.getAll().toArray());
            } else {
                udtf.close();
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
        return collector.getBag();
    }

    @Override
    public Schema outputSchema(Schema input) {
        try {
            if (!inited) {
                schemaInfo.init(getInputSchema(), udtf, constantsInfo);
                inited = true;
            }
            ResourceFieldSchema rfs = HiveUtils.getResourceFieldSchema(
                    TypeInfoUtils.getTypeInfoFromObjectInspector(schemaInfo.outputObjectInspector));

            ResourceSchema tupleSchema = new ResourceSchema();
            tupleSchema.setFields(new ResourceFieldSchema[] {rfs});

            ResourceFieldSchema bagFieldSchema = new ResourceFieldSchema();
            bagFieldSchema.setType(DataType.BAG);
            bagFieldSchema.setSchema(tupleSchema);

            ResourceSchema bagSchema = new ResourceSchema();
            bagSchema.setFields(new ResourceFieldSchema[] {bagFieldSchema});
            return Schema.getPigSchema(bagSchema);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    class HiveUDTFCollector implements Collector {
        DataBag bag = bf.newDefaultBag();

        public void init() {
            bag.clear();
        }

        @Override
        public void collect(Object input) throws HiveException {
            try {
                Tuple outputTuple = (Tuple)HiveUtils.convertHiveToPig(input, schemaInfo.outputObjectInspector, null);
                if (outputTuple.size()==1 && outputTuple.get(0) instanceof Tuple) {
                    bag.add((Tuple)outputTuple.get(0));
                } else {
                    bag.add(outputTuple);
                }
            } catch(Exception e) {
                throw new HiveException(e);
            }
        }

        public DataBag getBag() {
            return bag;
        }
    }

    @Override
    public boolean needEndOfAllInputProcessing() {
        return true;
    }

    @Override
    public void setEndOfAllInput(boolean endOfAllInput) {
        this.endOfAllInput = endOfAllInput;
    }
}
