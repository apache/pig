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
package org.apache.hadoop.owl.testdriver;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.owl.OwlTestCase;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.mapreduce.OwlInputStorageDriver;
import org.apache.hadoop.owl.protocol.ColumnType;
import org.apache.hadoop.owl.protocol.OwlColumnSchema;
import org.apache.hadoop.owl.protocol.OwlLoaderInfo;
import org.apache.hadoop.owl.protocol.OwlSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

public class TestSampleStorageDriverImpl extends OwlTestCase {

    @Test
    public static void testStorageDriver() throws IOException, InterruptedException{
        OwlInputStorageDriver sd = new SampleInputStorageDriverImpl();

        JobContext jobContext = null; //mock this? not needed, we don't use it in the sample driver anyway

        OwlLoaderInfo loaderInfo = new OwlLoaderInfo();

        OwlSchema schema = new OwlSchema();
        schema.addColumnSchema(new OwlColumnSchema("cint1", ColumnType.INT));
        schema.addColumnSchema(new OwlColumnSchema("cbool2", ColumnType.BOOL));
        schema.addColumnSchema(new OwlColumnSchema("cbytes3", ColumnType.BYTES));
        schema.addColumnSchema(new OwlColumnSchema("clong4", ColumnType.LONG));
        schema.addColumnSchema(new OwlColumnSchema("cstring5", ColumnType.STRING));
        schema.addColumnSchema(new OwlColumnSchema("cfloat6", ColumnType.FLOAT));
        schema.addColumnSchema(new OwlColumnSchema("cdouble7", ColumnType.DOUBLE));

        OwlSchema recordSchema = new OwlSchema();
        recordSchema.addColumnSchema(new OwlColumnSchema("rint1", ColumnType.INT));
        recordSchema.addColumnSchema(new OwlColumnSchema("rstring2", ColumnType.STRING));

        OwlColumnSchema recordColumnSchema = new OwlColumnSchema("crecord8", ColumnType.RECORD);
        recordColumnSchema.setSchema(recordSchema);

        schema.addColumnSchema(recordColumnSchema);

        OwlColumnSchema collectionColumnSchema = new OwlColumnSchema("ccoll9",ColumnType.COLLECTION);
        collectionColumnSchema.setSchema(recordSchema);

        schema.addColumnSchema(collectionColumnSchema);

        OwlSchema mapSchema = new OwlSchema();
        mapSchema.addColumnSchema(new OwlColumnSchema("mbool1",ColumnType.BOOL));
        OwlColumnSchema mapColumnSchema = new OwlColumnSchema("cmap10",ColumnType.MAP);
        mapColumnSchema.setSchema(mapSchema);

        schema.addColumnSchema(mapColumnSchema);

        System.out.println("Testing for schema : " + schema.getSchemaString());

        sd.setInputPath(jobContext, "hdfs://inpy/");
        sd.setOutputSchema(jobContext, schema);

        InputFormat<BytesWritable, Tuple> inpf = sd.getInputFormat(loaderInfo);

        int TEST_NUM_RECORDS = 11;
        int TEST_NUM_SPLITS = 3;

        ((SampleInputFormatImpl) inpf).setNumRecords(TEST_NUM_RECORDS);
        ((SampleInputFormatImpl) inpf).setNumSplits(TEST_NUM_SPLITS);

        TaskAttemptContext taskAttemptContext = null; //mock this? not needed, we don't use it.

        int splitCounter = 0;
        for (InputSplit isplit : inpf.getSplits(jobContext)){
            RecordReader<BytesWritable, Tuple> rr = inpf.createRecordReader(isplit, taskAttemptContext);
            int recordCounter = 0;
            while (rr.nextKeyValue()){
                BytesWritable bw = rr.getCurrentKey();
                Tuple tp = rr.getCurrentValue();
                List<Object>fields = tp.getAll();
                System.out.println("["+bw.toString()+"]["+tp.toDelimitedString("|")+"]");
                tupleMatchesSchema(tp,schema);
                recordCounter++;
            }
            assertTrue(recordCounter == TEST_NUM_RECORDS);
            System.out.println("--------");
            splitCounter++;
        }
        assertTrue(splitCounter == TEST_NUM_SPLITS);
    }

    @SuppressWarnings("unchecked")
    private static void tupleMatchesSchema(Tuple tp, OwlSchema schema) throws ExecException, OwlException {
        List<Object> fields = tp.getAll();
        assertEquals(fields.size(),schema.getColumnCount());
        for (int i = 0 ; i < fields.size() ; i++){
            byte type = tp.getType(i);
            Object field = tp.get(i);
            OwlColumnSchema column = schema.columnAt(i);

            if (type == DataType.BOOLEAN){
                assertEquals(column.getType() , ColumnType.BOOL);
            } else if ((type == DataType.BYTE) || (type == DataType.INTEGER)){
                assertEquals(column.getType(),ColumnType.INT);
            } else if (type == DataType.BYTEARRAY){
                assertEquals(column.getType(),ColumnType.BYTES);
            } else if (type == DataType.LONG){
                assertEquals(column.getType(),ColumnType.LONG);
            } else if ((type == DataType.CHARARRAY) || (type == DataType.BIGCHARARRAY)){
                assertEquals(column.getType(),ColumnType.STRING);
            } else if (type == DataType.DOUBLE){
                assertEquals(column.getType(),ColumnType.DOUBLE);
            } else if (type == DataType.FLOAT){
                assertEquals(column.getType(),ColumnType.FLOAT);
            } else if (type == DataType.TUPLE){
                tupleMatchesSchema((Tuple)field,column.getSchema());
            } else if (type == DataType.BAG){
                DataBag db = (DataBag) field;
                Iterator<Tuple> dbi = db.iterator();
                while (dbi.hasNext()){
                    tupleMatchesSchema(dbi.next(),column.getSchema());
                }
            } else if (type == DataType.MAP) {
                Map<String,Object> map = (Map<String,Object>) field;
                for (Object value : map.values()){
                    Tuple mapValueTestContainer = TupleFactory.getInstance().newTuple(1);
                    mapValueTestContainer.set(0, value);
                    tupleMatchesSchema(mapValueTestContainer,column.getSchema());
                }
            } else {
                assertTrue(false);
            }
        }
    }
}
