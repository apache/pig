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
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.owl.OwlTestCase;
import org.apache.hadoop.owl.common.ErrorType;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.driver.DriverVerificationUtil;
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

import org.junit.Test;

/**
 * Testing BasicTableOutputFormat and TableInputFormat using Local FS
 */
public class TestKeyPushDown extends OwlTestCase {

    private static final String OWL_KEY_JOB_INFO= "mapreduce.lib.owl.job.info";
    private static String dbname = "testkeypushdowndb";
    private static String tableName = "testkeypushdowntab";
    private static String partitionLocation = "test/keypushdown/partition";
    private static OwlTableName name = new OwlTableName(dbname, tableName);
    private static OwlDriver driver;
    static List<OwlKeyValue> keyValues;
    static OwlKeyValue k1, k2;

    private static boolean initialized = false;
    private static int numTestsRemaining = 1;

    @SuppressWarnings("boxing")
    static void publish(int v1, String v2, String location) throws OwlException {
        k1.setIntValue(v1);
        k2.setStringValue(v2);

        OwlSchema schema1 = new OwlSchema();
        schema1.addColumnSchema(new OwlColumnSchema("c1", ColumnType.INT));
        schema1.addColumnSchema(new OwlColumnSchema("c2", ColumnType.STRING));
        driver.publish(name, keyValues, null, location, schema1, new OwlLoaderInfo("org.apache.hadoop.zebra.owl.ZebraInputDriver"));
    }

    public static void initialize() throws IOException {
        if (!initialized){
            driver = new OwlDriver(getUri());

            DriverVerificationUtil.dropOwlTableIfExists(driver, name);
            DriverVerificationUtil.dropOwlDatabaseIfExists(driver, dbname);

            driver.createOwlDatabase(new OwlDatabase(dbname, "hdfs://localhost:9000/data/testdatabase"));

            OwlSchema schema = new OwlSchema();
            schema.addColumnSchema(new OwlColumnSchema("c1", ColumnType.INT));

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
            publish(2008, "us", partitionLocation + "1");
            publish(2008, "uk", partitionLocation + "2");

            initialized = true;
        }
    }

    //simple counter for local mode testing
    static OwlJobInfo getJobInfo(String filter) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "keypushdown test");
        job.setInputFormatClass(OwlInputFormat.class);               // input settings

        OwlTableName tabName = new OwlTableName(dbname, tableName);  //Specify server url, db name, table name and partition predicate through OwlTableInput object
        OwlTableInputInfo inputInfo = new OwlTableInputInfo(getUri(), tabName, filter);
        OwlInputFormat.setInput(job, inputInfo);
        String jobString = job.getConfiguration().get(OWL_KEY_JOB_INFO);
        if( jobString == null ) {
            throw new OwlException(ErrorType.ERROR_INPUT_UNINITIALIZED);
        }
        return (OwlJobInfo) SerializeUtil.deserialize(jobString);
    }


    @Test
    public static void testKeyPushed() throws Exception {
        initialize();
        OwlJobInfo jobInfo = getJobInfo("c1 = 2008 and c2 = \"us\"");
        assertEquals(1, jobInfo.getPartitions().size());

        OwlPartitionInfo partitionInfo = jobInfo.getPartitions().get(0);
        Map<String, OwlKeyValue> map = partitionInfo.getPartitionValues().getPartitionMap();
        assertEquals(2, map.size());

        OwlKeyValue k1 = map.get("c1");
        assertEquals((Integer) 2008, k1.getIntValue());

        OwlKeyValue k2 = map.get("c2");
        assertEquals("us", k2.getStringValue());
        cleanup();
    }

    public static void cleanup() throws IOException {
        numTestsRemaining--;
        if (numTestsRemaining == 0){
            driver.dropOwlTable(new OwlTable(name));
            driver.dropOwlDatabase(new OwlDatabase(dbname, null));
        }
    }
}

