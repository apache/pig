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

package org.apache.hadoop.owl.client;

import org.apache.hadoop.owl.OwlTestCase;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.client.CompleteSanityTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SinglePartitionTest extends OwlTestCase {

    private static OwlClient client;
    private CompleteSanityTest spt;

    public SinglePartitionTest() {
        client = new OwlClient(getUri());
        this.spt = new CompleteSanityTest();

    }

    @Before
    public void testInitialize() {
    }

    @Test
    public void testCreateDatabase() throws OwlException {
        spt.createDatabase("testSinglePartitionDatabase", "testloc");
    }

    @Test
    public void testCreatePartitionedOwlTable() throws OwlException {
        spt.createPartitionedOwlTable("testSinglePartitionOwlTable", "testSinglePartitionDatabase",
                "retention", "INTEGER", "country", "STRING", "color", "STRING",
                "shape", "STRING");
    }

    @Test
    public void testDescribeOwlTable() throws OwlException {

        spt.describeOwlTable("testSinglePartitionOwlTable", "testSinglePartitionDatabase", "retention",
                "INTEGER", "country",  "STRING", "color",
                "STRING", "shape", "STRING");
    }

    @Test
    public void testPublishDataElement() throws OwlException {
        spt.publishDataElement("testSinglePartitionOwlTable", "testSinglePartitionDatabase", "retention", 1,
                "INTEGER", "country", "USA", "STRING", "color", "red",
                "STRING", "shape", "round", "STRING",
        "hdfs://dummytestlocation.blah/1234");
    }

    @Test
    public void testSelectPartition() throws OwlException {
        spt.selectPartition("testSinglePartitionOwlTable", "testSinglePartitionDatabase", "retention", 1,
                "INTEGER", "country", "USA", "STRING", "color", "red",
                "STRING", "shape", "round", "STRING",
        "hdfs://dummytestlocation.blah/1234");
    }

    @Test
    public void testAlterToModifyPropertyKeyValue() throws OwlException {
        spt.alterToModifyPropertyKeyValue("testSinglePartitionOwlTable", "testSinglePartitionDatabase",
                "retention", 1, "INTEGER", "country", "USA", "STRING", "color",
                "red", "STRING", "shape", "square", "STRING",
        "hdfs://dummytestlocation.blah/1234");
    }

    @Test
    public void testAlterToAddPropertyKey() throws OwlException {
        spt.alterToAddPropertyKey("testSinglePartitionOwlTable", "testSinglePartitionDatabase", 1, "keyName1", "STRING", 
                "keyName2", "STRING", "country", "USA");
    }

    @Test
    public void testAlterToDropProperty() throws OwlException {
        spt.alterToDropProperty("testSinglePartitionOwlTable", "testSinglePartitionDatabase", "retention", 1,
                "INTEGER", "country", "USA", "STRING", "color", "red",
                "STRING", "shape", "square", "STRING",
        "hdfs://dummytestlocation.blah/1234");
    }


    /*
    @Test
    public void testAlterToDropPartition() throws OwlException {
        spt.alterToDropPartition("testSinglePartitionOwlTable", "testSinglePartitionDatabase", "retention", 1,
                "INTEGER", "country", "USA", "STRING", "color", "red",
                "STRING", "shape", "square", "STRING",
        "hdfs://dummytestlocation.blah/1234");
    }

    // Still results in the following after running:
    // {"executionTime":73,"output":[{"keyValues":[{"dataType":"STRING","keyName":"color","keyType":"PROPERTY","stringValue":"red"}]}],"status":"SUCCESS"}
    // BECK: result.getOutput() has [1] entries.
     */
    
    @Test
    public void testDropOwlTable() throws OwlException {
        spt.dropOwlTable("testSinglePartitionOwlTable", "testSinglePartitionDatabase");
    }

    @Test
    public void testDropDatabase() throws OwlException {
        spt.dropDatabase("testSinglePartitionDatabase");
    }

    @After
    public void testCleanup() throws OwlException {
        spt.cleanup("testSinglePartitionDatabase", "testSinglePartitionOwlTable");
    }

}
