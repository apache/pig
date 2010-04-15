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
package org.apache.hadoop.owl.mapreduce;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.owl.OwlTestCase;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.common.OwlUtil;
import org.apache.hadoop.owl.protocol.ColumnType;
import org.apache.hadoop.owl.protocol.OwlColumnSchema;
import org.apache.hadoop.owl.protocol.OwlLoaderInfo;
import org.apache.hadoop.owl.protocol.OwlSchema;
import org.junit.Test;

public class TestOwlSplit extends OwlTestCase{
    // this private TestSplit class extends InputSplit
    private static class TestSplit extends InputSplit implements Writable{

        public TestSplit(){}

        @Override
        public long getLength() throws IOException, InterruptedException {
            return 0;
        }

        @Override
        public String[] getLocations() throws IOException, InterruptedException {
            return null;
        }

        public void readFields(DataInput arg0) throws IOException {
        }

        public void write(DataOutput arg0) throws IOException {
        }
    }

    private static OwlSplit owlSplit;

    public TestOwlSplit() {
    }


    @Test
    public static void testSerialization() throws OwlException {

        OwlSchema partitionSchema = new OwlSchema();
        partitionSchema.addColumnSchema(new OwlColumnSchema("c1", ColumnType.STRING));

        OwlSchema tableSchema = new OwlSchema();
        tableSchema.addColumnSchema(new OwlColumnSchema("d1", ColumnType.STRING));

        OwlLoaderInfo loaderInfo = new OwlLoaderInfo("zebraInputSplit");
        String location = "hdfs://tmp";

        OwlPartitionInfo partitionInfo = new OwlPartitionInfo(partitionSchema, loaderInfo, location);

        TestSplit testSplit = new TestSplit();
        InputSplit baseSplit = testSplit;

        owlSplit = new OwlSplit(partitionInfo, baseSplit, tableSchema);  

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutput output = new DataOutputStream(bos);

        try {
            // we need to first create an DataOutputStream for OwlSplit to write into
            owlSplit.write(output);
            // remember to close the DataOutputStream to make sure the data has been written
            bos.close();

            ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
            DataInputStream dataInputStream = new DataInputStream(bis);

            OwlSplit newOwlSplit = new OwlSplit();
            newOwlSplit.readFields(dataInputStream);

            // compare newOwlSplit and owlSplit
            OwlPartitionInfo newPartitionInfo = newOwlSplit.getPartitionInfo();
            // compare location string
            assertEquals(newPartitionInfo.getLocation(), owlSplit.getPartitionInfo().getLocation());
            // compare schemas
            assertEquals(newPartitionInfo.getPartitionSchema(), (owlSplit.getPartitionInfo().getPartitionSchema()));
            assertEquals(newOwlSplit.getTableSchema(), owlSplit.getTableSchema());

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
