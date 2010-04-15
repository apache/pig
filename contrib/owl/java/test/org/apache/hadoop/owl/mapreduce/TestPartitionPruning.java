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
import org.apache.hadoop.owl.testdriver.StorageDriverStub;
import org.apache.hadoop.owl.testdriver.SampleDriverStub;

import org.apache.pig.data.Tuple;

import org.junit.Test;

/**
 * Test case for partition pruning. Creates and publishes six partitions, queries to ensure that right
 * number of rows reach the mapper.
 */
public class TestPartitionPruning extends OwlTestCase {

    private String dbname = "testPruningdb";
    private String tableName = "testPruningTable";
    private String dataLocation = "test/TestPruning";

    private String partitionLocation1 = dataLocation + "/partition1";
    private String partitionLocation2 = dataLocation + "/partition2";
    private String partitionLocation3 = dataLocation + "/partition3";
    private String partitionLocation4 = dataLocation + "/partition4";
    private String partitionLocation5 = dataLocation + "/partition5";
    private String partitionLocation6 = dataLocation + "/partition6";

    private OwlSchema schema;

    private OwlTableName name = new OwlTableName(dbname, tableName);
    private OwlDriver driver;

    private List<OwlKeyValue> keyValues;
    private OwlKeyValue k1, k2;
    private StorageDriverStub stub;

    public TestPartitionPruning() {
        //Only for junit to use
    }

    public TestPartitionPruning(StorageDriverStub stub) {
        this.stub = stub;
    }

    @SuppressWarnings("boxing")
    void publish(int v1, String v2, String location, OwlSchema schema) throws OwlException {
        k1.setIntValue(v1);
        k2.setStringValue(v2);

        driver.publish(name, keyValues, null, location, schema, new OwlLoaderInfo(stub.getDriverClass().getName()));
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

    void runMRTest(String filter, int expectedRowCount) throws Exception {

        Configuration conf = new Configuration();
        Job job = new Job(conf, "partition pruning test");
        job.setJarByClass(TestPartitionPruning.class);
        job.setMapperClass(TestPartitionPruning.MapClass.class);

        // input settings
        job.setInputFormatClass(OwlInputFormat.class);

        //Specify server url, db name, table name and partition predicate through OwlTableInput object
        OwlTableName tabName = new OwlTableName(dbname, tableName);
        OwlTableInputInfo inputInfo = new OwlTableInputInfo(getUri(), tabName, filter);
        OwlInputFormat.setInput(job, inputInfo);

        //Test that OwlInputFormat.getTableSchema works
        CompareOwlObjects.compareObjects(OwlInputFormat.getTableSchema(job), schema);

        FileSystem fs = new LocalFileSystem();

        stub.dropData(new Path(fs.getWorkingDirectory(), dataLocation + "/outputfile"));
        FileOutputFormat.setOutputPath(job, new Path(dataLocation + "/outputfile"));
        job.setOutputKeyClass(BytesWritable.class);
        job.setOutputValueClass(stub.getOutputTupleClass());

        rowCount = 0;
        job.waitForCompletion(true);

        System.out.println("Filter " + filter + ", Map job got " + rowCount + " rows");
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

        driver.createOwlDatabase(new OwlDatabase(dbname, "hdfs://localhost:9000/data/testPartitionPruning"));

        schema = new OwlSchema();
        schema.addColumnSchema(new OwlColumnSchema("c1", ColumnType.INT));
        schema.addColumnSchema(new OwlColumnSchema("c2", ColumnType.STRING));
        schema.addColumnSchema(new OwlColumnSchema("k1", ColumnType.INT));
        schema.addColumnSchema(new OwlColumnSchema("k2", ColumnType.STRING));

        OwlTable table = new OwlTableBuilder().
        setName(name).
        addPartition("c1", DataType.INT).
        addPartition("c2", DataType.STRING).
        setSchema(schema).
        build();

        driver.createOwlTable(table);

        keyValues = new ArrayList<OwlKeyValue>();
        k1 = new OwlKeyValue("c1", 2008);
        k2 = new OwlKeyValue("c2", "us");
        keyValues.add(k1);
        keyValues.add(k2);

        stub.createRandomData(new Path(fs.getWorkingDirectory(), partitionLocation1), partitionLocation1, 1, 11, schema);
        stub.createRandomData(new Path(fs.getWorkingDirectory(), partitionLocation2), partitionLocation2, 1, 12, schema);
        stub.createRandomData(new Path(fs.getWorkingDirectory(), partitionLocation3), partitionLocation3, 1, 13, schema);
        stub.createRandomData(new Path(fs.getWorkingDirectory(), partitionLocation4), partitionLocation4, 1, 14, schema);
        stub.createRandomData(new Path(fs.getWorkingDirectory(), partitionLocation5), partitionLocation5, 1, 15, schema);
        stub.createRandomData(new Path(fs.getWorkingDirectory(), partitionLocation6), partitionLocation6, 1, 16, schema);

        publish(2008, "us", partitionLocation1, schema);
        publish(2008, "uk", partitionLocation2, schema);
        publish(2009, "us", partitionLocation3, schema);
        publish(2009, "uk", partitionLocation4, schema);
        publish(2009, "in", partitionLocation5, schema );
        publish(2010, "uk", partitionLocation6, schema );
    }

    public void pruningTest() throws Exception {
        runMRTest("c1 = 2008 and c2 = \"us\"", 11); //first partition has 11 rows
        runMRTest("c1 = 2008", 23); //11 + 12
        runMRTest("c1 = 2009 and (c2 = \"us\" or c2 = \"uk\")", 27); //13 + 14
        runMRTest("c1 > 2008 and c2 = \"uk\"", 30);
        runMRTest(null, 81); //all partitions should match
    }

    public static void runTest(StorageDriverStub stub) throws Exception {
        TestPartitionPruning test = new TestPartitionPruning(stub);

        test.initialize();
        test.pruningTest();
        test.cleanup(false);
    }

    @Test
    public static void testSampleStorageDriverPruning() throws Exception {
        runTest(new SampleDriverStub());
    }
}



