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
import org.apache.hadoop.owl.testdriver.StorageDriverStub;
import org.apache.hadoop.owl.testdriver.SampleDriverStub;

import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test case which tests whether schema evolution works and partition key values are present in output.
 * Creates two partitions, one with 2 columns and another with 3 columns, setOutputSchema is set to
 * fetch two data columns and the two partition columns.
 */
public class TestPartitionKey extends OwlTestCase {

    private String dbname = "testPVdb";
    private String tableName = "testPVtable";
    private String dataLocation = "test/TestPartitionValue";
    private String partitionLocation1 = dataLocation + "/partition1";
    private String partitionLocation2 = dataLocation + "/partition2";

    private OwlTableName name = new OwlTableName(dbname, tableName);
    private OwlDriver driver;

    private List<OwlKeyValue> keyValues;
    private OwlKeyValue k1, k2;
    private StorageDriverStub stub;

    public TestPartitionKey() {
        //Only for junit to use
    }

    public TestPartitionKey(StorageDriverStub stub) {
        this.stub = stub;
    }

    @SuppressWarnings("boxing")
    void publish(int v1, String v2, String location, OwlSchema schema) throws OwlException {
        k1.setIntValue(v1);
        k2.setStringValue(v2);

        driver.publish(name, keyValues, null, location, schema, new OwlLoaderInfo(stub.getDriverClass().getName()));
    }

    volatile int mapCounter = 0;
    Object lockObject = new Object();

    public class MapClass extends
    Mapper<BytesWritable, Tuple, BytesWritable, Tuple> {

        @Override
        public void map(BytesWritable key, Tuple value, Context context
        ) throws IOException, InterruptedException {
            {
                synchronized(lockObject) {
                    mapCounter++;
                }

                System.out.println("In map2 function -> key " + new String(key.getBytes()) + " value " + value.toString());
                context.write(key, value);

                Assert.assertEquals("tuple size", 4, value.size());
                Object c2 = value.get(0);
                Object p2 = value.get(1);
                Object c3 = value.get(2);
                Object p1 = value.get(3);

                Assert.assertNotNull("c2 value", c2);
                Assert.assertNotNull("p1 value", p1);
                Assert.assertNotNull("p2 value", p2);

                String keyString = new String(key.getBytes());
                if( keyString.startsWith(partitionLocation1) ) {
                    //If first partition, c3 should be null and p2 should be "uk"
                    Assert.assertNull("c3 value", c3);
                    Assert.assertEquals("p1 value", 2008, p1);
                    Assert.assertEquals("p2 value", "us", p2);
                } else if( keyString.startsWith(partitionLocation2) ) {
                    //If second partition, c3 should be non null and p2 should be "uk"
                    Assert.assertNotNull("c3 value", c3);
                    Assert.assertEquals("p1 value", 2008, p1);
                    Assert.assertEquals("p2 value", "uk", p2);
                } else {
                    Assert.fail("Unknown partition key found" + keyString);
                }

                //No errors in mapper, reset counter
                synchronized(lockObject) {
                    mapCounter--;
                }
            }
        }
    }

    void runMRTest(String filter) throws Exception {

        Configuration conf = new Configuration();
        Job job = new Job(conf, "partition value test");
        job.setJarByClass(TestPartitionKey.class);
        job.setMapperClass(TestPartitionKey.MapClass.class);

        // input settings
        job.setInputFormatClass(OwlInputFormat.class);

        //Specify server url, db name, table name and partition predicate through OwlTableInput object
        OwlTableName tabName = new OwlTableName(dbname, tableName);
        OwlTableInputInfo inputInfo = new OwlTableInputInfo(getUri(), tabName, filter);
        OwlInputFormat.setInput(job, inputInfo);

        OwlSchema schema = new OwlSchema();
        schema.addColumnSchema(new OwlColumnSchema("c2", ColumnType.STRING));
        schema.addColumnSchema(new OwlColumnSchema("p2", ColumnType.STRING));
        schema.addColumnSchema(new OwlColumnSchema("c3", ColumnType.INT));
        schema.addColumnSchema(new OwlColumnSchema("p1", ColumnType.STRING));

        //Set output schema having partition keys also
        OwlInputFormat.setOutputSchema(job, schema);
        FileSystem fs = new LocalFileSystem();

        stub.dropData(new Path(fs.getWorkingDirectory(), dataLocation + "/outputfile"));
        FileOutputFormat.setOutputPath(job, new Path(dataLocation + "/outputfile"));
        job.setOutputKeyClass(BytesWritable.class);
        job.setOutputValueClass(DefaultTuple.class);

        job.waitForCompletion(true);

        assertEquals("Mapper completion status", 0, mapCounter);
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

        driver.createOwlDatabase(new OwlDatabase(dbname, "hdfs://localhost:9000/data/testPartitionValues"));

        OwlSchema schema = new OwlSchema();
        schema.addColumnSchema(new OwlColumnSchema("c1", ColumnType.INT));

        OwlTable table = new OwlTableBuilder().
        setName(name).
        addPartition("p1", DataType.INT).
        addPartition("p2", DataType.STRING).
        setSchema(schema).
        build();

        driver.createOwlTable(table);

        keyValues = new ArrayList<OwlKeyValue>();
        k1 = new OwlKeyValue("p1", 2008);
        k2 = new OwlKeyValue("p2", "us");
        keyValues.add(k1);
        keyValues.add(k2);

        OwlSchema schema1 = new OwlSchema();
        schema1.addColumnSchema(new OwlColumnSchema("c1", ColumnType.INT));
        schema1.addColumnSchema(new OwlColumnSchema("c2", ColumnType.STRING));

        //Seconds partitions schema has extra column
        OwlSchema schema2 = new OwlSchema();
        schema2.addColumnSchema(new OwlColumnSchema("c1", ColumnType.INT));
        schema2.addColumnSchema(new OwlColumnSchema("c2", ColumnType.STRING));
        schema2.addColumnSchema(new OwlColumnSchema("c3", ColumnType.INT));

        stub.createRandomData(new Path(fs.getWorkingDirectory(), partitionLocation1), partitionLocation1, 1, 11, schema1);
        publish(2008, "us", partitionLocation1, schema1);

        stub.createRandomData(new Path(fs.getWorkingDirectory(), partitionLocation2), partitionLocation1, 1, 12, schema2);
        publish(2008, "uk", partitionLocation2, schema2);
    }

    public void partitionValueMRTest() throws Exception {
        runMRTest("p1 = 2008 and p2 = \"us\""); 
        runMRTest("p1 = 2008 and p2 = \"uk\"");
        runMRTest("p1 = 2008");
    }

    public static void runTest(StorageDriverStub stub) throws Exception {
        TestPartitionKey test = new TestPartitionKey(stub);

        test.initialize();
        test.partitionValueMRTest();
        test.cleanup(false);
    }

    @Test
    public static void testSampleStorageDriverPK() throws Exception {
        runTest(new SampleDriverStub());
    }
}

