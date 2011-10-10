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

package org.apache.pig.piggybank.test.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import junit.framework.Assert;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.piggybank.storage.IndexedStorage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests IndexedStorage.
 */
public class TestIndexedStorage extends TestCase {
    public TestIndexedStorage () throws IOException {
    }

    @Override
    @Before
    /**
     * Creates indexed data sets.
     */ 
    public void setUp() throws Exception {
        int [][] keys1 = {{2,2},{3,3},{4,3},{5,5},{6,6},{10,10}};
        List<Tuple> records = generateRecords(keys1);
        writeOutputFile("out/1", records); 
       
        int [][] keys2 = {{3,2},{4,4},{5,5},{11,11},{13,13}};
        records = generateRecords(keys2);
        writeOutputFile("out/2", records); 

        int [][] keys3 = {{7,7},{8,8},{9,9}};
        records = generateRecords(keys3);
        writeOutputFile("out/3", records); 
    }

    @Override
    @After
    /**
     * Deletes all data directories.
     */ 
    public void tearDown() throws Exception {
        Configuration conf = new Configuration();
        LocalFileSystem fs = FileSystem.getLocal(conf);
        fs.delete(new Path("out"), true);
    }


    @Test
    /**
     * Tests getNext called repeatedly returns items in sorted order.
     */ 
    public void testGetNext() throws IOException, InterruptedException {
        IndexedStorage storage = new IndexedStorage("\t","0,1");
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "file:///");
        LocalFileSystem fs = FileSystem.getLocal(conf);
        
        TaskAttemptID taskId = new TaskAttemptID();
        conf.set("mapred.task.id", taskId.toString());
        
        conf.set("mapred.input.dir","out");
        storage.initialize(conf);
       
        Integer key; 
        int [][] correctValues = {{2,2},{3,2},{3,3},{4,3},{4,4},{5,5},{5,5},{6,6},{7,7},{8,8},{9,9},{10,10},{11,11},{13,13}};
        for (int [] outer : correctValues) {
            System.out.println("Testing: (" + outer[0] + "," + outer[1] + ")");
            Tuple read = storage.getNext();
            System.out.println("Read: " + read);

            key = Integer.decode(((DataByteArray)read.get(0)).toString());
            Assert.assertTrue("GetNext did not return the correct value.  Received: " + read, key.equals(outer[0]));
            key = Integer.decode(((DataByteArray)read.get(1)).toString());
            Assert.assertTrue("GetNext did not return the correct value.  Received: " + read, key.equals(outer[1]));
        }
        Tuple read = storage.getNext();
        Assert.assertTrue("GetNext did not return the correct value", (read == null));
    }

    @Test
    /**
     * Tests seekNear and getNext work correctly.
     */ 
    public void testSeek() throws IOException, InterruptedException {
        IndexedStorage storage = new IndexedStorage("\t","0,1");
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "file:///");
        LocalFileSystem fs = FileSystem.getLocal(conf);
        
        TaskAttemptID taskId = new TaskAttemptID();
        conf.set("mapred.task.id", taskId.toString());
        
        conf.set("mapred.input.dir","out");
        storage.initialize(conf);

        TupleFactory tupleFactory = TupleFactory.getInstance();
        Tuple seek = tupleFactory.newTuple(2);
        Integer key;
        Tuple read;

        //Seek to 1,1 (not in index). getNext should return 2
        seek.set(0, new Integer(1)); 
        seek.set(1, new Integer(1)); 
        storage.seekNear(seek);
        read = storage.getNext();
        key = Integer.decode(((DataByteArray)read.get(0)).toString());
        Assert.assertTrue("GetNext did not return the correct value", key.equals(2));
        key = Integer.decode(((DataByteArray)read.get(1)).toString());
        Assert.assertTrue("GetNext did not return the correct value", key.equals(2));

        //Seek to 3,2.  getNext should return 3,2
        seek.set(0, new Integer(3)); 
        seek.set(1, new Integer(2)); 
        storage.seekNear(seek);
        read = storage.getNext();
        key = Integer.decode(((DataByteArray)read.get(0)).toString());
        Assert.assertTrue("GetNext did not return the correct value", key.equals(3));
        key = Integer.decode(((DataByteArray)read.get(1)).toString());
        Assert.assertTrue("GetNext did not return the correct value", key.equals(2));

        //getNext should return 3,3
        read = storage.getNext();
        key = Integer.decode(((DataByteArray)read.get(0)).toString());
        Assert.assertTrue("GetNext did not return the correct value", key.equals(3));
        key = Integer.decode(((DataByteArray)read.get(1)).toString());
        Assert.assertTrue("GetNext did not return the correct value", key.equals(3));

        //Seek to 6,6.  getNext should return 6,6
        seek.set(0, new Integer(6)); 
        seek.set(1, new Integer(6)); 
        storage.seekNear(seek);
        read = storage.getNext();
        key = Integer.decode(((DataByteArray)read.get(0)).toString());
        Assert.assertTrue("GetNext did not return the correct value", key.equals(6));
        key = Integer.decode(((DataByteArray)read.get(1)).toString());
        Assert.assertTrue("GetNext did not return the correct value", key.equals(6));

        //getNext should return 7,7
        read = storage.getNext();
        key = Integer.decode(((DataByteArray)read.get(0)).toString());
        Assert.assertTrue("GetNext did not return the correct value", key.equals(7));
        key = Integer.decode(((DataByteArray)read.get(1)).toString());
        Assert.assertTrue("GetNext did not return the correct value", key.equals(7));

        //Seek to 9,9.  getNext should return 9,9
        seek.set(0, new Integer(9)); 
        seek.set(1, new Integer(9)); 
        storage.seekNear(seek);
        read = storage.getNext();
        key = Integer.decode(((DataByteArray)read.get(0)).toString());
        Assert.assertTrue("GetNext did not return the correct value", key.equals(9));
        key = Integer.decode(((DataByteArray)read.get(1)).toString());
        Assert.assertTrue("GetNext did not return the correct value", key.equals(9));

        //getNext should return 10,10
        read = storage.getNext();
        key = Integer.decode(((DataByteArray)read.get(0)).toString());
        Assert.assertTrue("GetNext did not return the correct value", key.equals(10));
        key = Integer.decode(((DataByteArray)read.get(1)).toString());
        Assert.assertTrue("GetNext did not return the correct value", key.equals(10));


        //Seek to 13,12 (Not in index).  getNext should return 13,13
        seek.set(0, new Integer(13)); 
        seek.set(1, new Integer(12)); 
        storage.seekNear(seek);
        read = storage.getNext();
        key = Integer.decode(((DataByteArray)read.get(0)).toString());
        Assert.assertTrue("GetNext did not return the correct value", key.equals(13));
        key = Integer.decode(((DataByteArray)read.get(1)).toString());
        Assert.assertTrue("GetNext did not return the correct value", key.equals(13));

        //Seek to 20 (Not in index). getNext should return null 
        seek.set(0, new Integer(20)); 
        seek.set(1, new Integer(20)); 
        storage.seekNear(seek);
        read = storage.getNext();
        Assert.assertTrue("GetNext did not return the correct value", (read == null));
    }

    /**
     * Given a list of integers, construct a list of single element tuples.
     */
    private List<Tuple> generateRecords(int [][] keys) throws IOException {
        TupleFactory tupleFactory = TupleFactory.getInstance();
        ArrayList<Tuple> records = new ArrayList<Tuple>(keys.length);
        for (int [] outer : keys) {
            Tuple indexTuple = tupleFactory.newTuple(outer.length);
            int idx = 0;
            for (int key : outer) {
                indexTuple.set(idx, new Integer(key));
                idx++;
            }
            records.add(indexTuple);
        }
        return records;
    }

    /**
     * Given a list of records (Tuples) and a output directory, writes out the 
     * records using IndexStorage in the output directory.
     */
    private void writeOutputFile(String outputDir, List<Tuple> records) throws IOException, InterruptedException {
        IndexedStorage storage = new IndexedStorage("\t","0,1");
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.output.dir", outputDir);

        TaskAttemptID taskId = new TaskAttemptID();
        conf.set("mapred.task.id", taskId.toString());

        OutputFormat out = storage.getOutputFormat();
        TaskAttemptContext ctx = new TaskAttemptContext(conf, taskId); 

        RecordWriter<WritableComparable, Tuple> writer = out.getRecordWriter(ctx);
        for (Tuple t : records) {
           writer.write(NullWritable.get(), t);            
        }
        writer.close(ctx);

        LocalFileSystem fs = FileSystem.getLocal(conf);
        String outputFileName = FileOutputFormat.getUniqueFile(ctx, "part", "");
        fs.rename(new Path(outputDir + "/_temporary/_" + taskId.toString() + "/" + outputFileName), new Path(outputDir + "/" + outputFileName));
        fs.rename(new Path(outputDir + "/_temporary/_" + taskId.toString() + "/." + outputFileName + ".index"), new Path(outputDir + "/." + outputFileName + ".index"));
    }
}
