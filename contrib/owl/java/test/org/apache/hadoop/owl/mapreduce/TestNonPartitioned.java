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

package org.apache.hadoop.owl.mapreduce;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.owl.OwlTestCase;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.driver.OwlDriver;
import org.apache.hadoop.owl.driver.OwlTableBuilder;
import org.apache.hadoop.owl.protocol.ColumnType;
import org.apache.hadoop.owl.protocol.OwlColumnSchema;
import org.apache.hadoop.owl.protocol.OwlDatabase;
import org.apache.hadoop.owl.protocol.OwlKeyValue;
import org.apache.hadoop.owl.protocol.OwlLoaderInfo;
import org.apache.hadoop.owl.protocol.OwlSchema;
import org.apache.hadoop.owl.protocol.OwlTable;
import org.apache.hadoop.owl.protocol.OwlTableName;
import org.apache.hadoop.owl.protocol.OwlKey.DataType;
import org.apache.hadoop.owl.testdriver.CompareOwlObjects;
import org.apache.hadoop.owl.testdriver.SampleDriverStub;
import org.apache.hadoop.owl.testdriver.StorageDriverStub;

import org.apache.pig.data.Tuple;

import org.junit.Test;

/**
 * Test case for non partitioned owltable access through OwlInputFormat 
 */
public class TestNonPartitioned extends OwlTestCase {

    private String dbname = "testnonpartOIFdb";
    private String tableName = "testnonpartOIFTable";
    private String dataLocation = "test/TestNonPartitioned";


    private OwlSchema schema;

    private OwlTableName name = new OwlTableName(dbname, tableName);
    private OwlDriver driver;

    private StorageDriverStub stub;

    public TestNonPartitioned() {
        //Only for junit to use
    }

    public TestNonPartitioned(StorageDriverStub stub) {
        this.stub = stub;
    }

    @SuppressWarnings("boxing")
    void publish(String location, OwlSchema schema) throws OwlException {
        driver.publish(name, null, null, location, schema, new OwlLoaderInfo(stub.getDriverClass().getName()));
    }

    static volatile int rowCount = 0;
    static Object lockObject = new Object();

    public static class MapClass extends
    Mapper<BytesWritable, Tuple, BytesWritable, Tuple> {

        @Override
        public void map(BytesWritable key, Tuple value, Context context
        ) throws IOException, InterruptedException {
            {
                //System.out.println("In map function -> key " + key.toString() + " value " + value.toString());
                context.write(key, value);
                synchronized (lockObject) {
                    rowCount++;
                }
            }
        }
    }

    void runMRTest(int expectedRowCount) throws Exception {

        Configuration conf = new Configuration();
        Job job = new Job(conf, "non partitioned test");
        job.setJarByClass(TestNonPartitioned.class);
        job.setMapperClass(TestNonPartitioned.MapClass.class);

        // input settings
        job.setInputFormatClass(OwlInputFormat.class);
        job.setOutputKeyClass(BytesWritable.class);
        job.setOutputValueClass(stub.getOutputTupleClass());

        //Specify server url, db name, table name and partition predicate through OwlTableInput object
        OwlTableName tabName = new OwlTableName(dbname, tableName);
        OwlTableInputInfo inputInfo = new OwlTableInputInfo(getUri(), tabName, null);
        OwlInputFormat.setInput(job, inputInfo);


        FileSystem fs = new LocalFileSystem();

        stub.dropData(new Path(fs.getWorkingDirectory(), dataLocation + "/outputfile"));
        FileOutputFormat.setOutputPath(job, new Path(dataLocation + "/outputfile"));

        rowCount = 0;
        job.waitForCompletion(true);

        System.out.println("Filter " + null + ", Map job got " + rowCount + " rows");
        assertEquals(expectedRowCount, rowCount);    
    }

    private void cleanup(boolean ignoreException) throws Exception {
        try {
            driver.dropOwlTable(new OwlTable(name));
        } catch(Exception e){ if( ! ignoreException ) throw e; }
        try {
            driver.dropOwlDatabase(new OwlDatabase(dbname, null));
        } catch(Exception e){ if( ! ignoreException ) throw e; }
        try {
            FileSystem fs = new LocalFileSystem();
            stub.dropData(new Path(fs.getWorkingDirectory(), dataLocation));
        }catch(Exception e){ if( ! ignoreException ) throw e; }
    }

    public void initialize() throws Exception {
        FileSystem fs = new LocalFileSystem();

        driver = new OwlDriver(getUri());
        cleanup(true);

        driver.createOwlDatabase(new OwlDatabase(dbname, "hdfs://localhost:9000/data/testNonPartitioned"));

        schema = new OwlSchema();
        schema.addColumnSchema(new OwlColumnSchema("k1", ColumnType.INT));
        schema.addColumnSchema(new OwlColumnSchema("k2", ColumnType.STRING));

        OwlTable table = new OwlTableBuilder().
        setName(name).
        setSchema(schema).
        build();

        driver.createOwlTable(table);


        stub.createRandomData(new Path(fs.getWorkingDirectory(), dataLocation), dataLocation, 1, 11, schema);

        publish(dataLocation, schema);
    }

    public void nonPartitionedTest() throws Exception {
        runMRTest(11); 
    }

    public static void runTest(StorageDriverStub stub) throws Exception {
        TestNonPartitioned test = new TestNonPartitioned(stub);

        test.initialize();
        test.nonPartitionedTest();
        test.cleanup(false);
    }

    @Test
    public static void testSampleStorageDriverNonPartitioned() throws Exception {
        runTest(new SampleDriverStub());
    }
}



