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

import java.io.File;
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
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.hadoop.executionengine.shims.HadoopShims;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.piggybank.storage.IndexedStorage;
import org.apache.pig.test.Util;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests IndexedStorage.
 */
public class TestIndexedStorage {
    public TestIndexedStorage () throws IOException {
    }

    @BeforeClass
    /**
     * Creates indexed data sets.
     */ 
    public static void setUpBeforeClass() throws Exception {
        PigServer pigServer = new PigServer(ExecType.LOCAL);
        
        String[] input1 = new String[] {
            "2\t2", "3\t3", "4\t3", "5\t5", "6\t6", "10\t10"
        };
        
        createInputFile(pigServer, input1, 1);
        
        String[] input2 = new String[] {
            "3\t2", "4\t4", "5\t5", "11\t11", "13\t13"
        };
        
        createInputFile(pigServer, input2, 2);

        String[] input3 = new String[] {
            "7\t7", "8\t8", "9\t9"
        };
        
        createInputFile(pigServer, input3, 3);
    }
    
    private static void createInputFile(PigServer pigServer, String[] inputs, int id) throws IOException {
        File input = File.createTempFile("tmp", "");
        input.delete();
        Util.createLocalInputFile(input.getAbsolutePath(), inputs);
        
        pigServer.registerQuery("A = load '" + input.getAbsolutePath() + "' as (a0:int, a1:int);");
        
        File output = File.createTempFile("tmp", "");
        output.delete();
        pigServer.store("A", output.getAbsolutePath(), "org.apache.pig.piggybank.storage.IndexedStorage('\t','0,1')");
        
        File outputFile = new File(output.getAbsoluteFile()+"/part-m-00000");
        new File("out/"+id).mkdirs();
        outputFile.renameTo(new File("out/"+id+"/part-m-00000"));
        
        File outputIndexFile = new File(output.getAbsoluteFile()+"/.part-m-00000.index");
        outputIndexFile.renameTo(new File("out/"+id+"/.part-m-00000.index"));
    }

    @AfterClass
    /**
     * Deletes all data directories.
     */ 
    public static void tearDownAfterClass() throws Exception {
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
        
        TaskAttemptID taskId = HadoopShims.createTaskAttemptID("jt", 1, true, 1, 1);
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
        
        TaskAttemptID taskId =  HadoopShims.createTaskAttemptID("jt", 2, true, 2, 2);
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
}
