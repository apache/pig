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
package org.apache.pig.test;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Map.Entry;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.tools.bzip2r.CBZip2InputStream;
import org.apache.tools.bzip2r.CBZip2OutputStream;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestBZip {
    static MiniCluster cluster = MiniCluster.buildCluster();
    
    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cluster.shutDown();
    }
    
   /**
    * Tests the end-to-end writing and reading of a BZip file.
    */
    @Test
    public void testBzipInPig() throws Exception {
        PigServer pig = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
       
        File in = File.createTempFile("junit", ".bz2");
        in.deleteOnExit();
        
        File out = File.createTempFile("junit", ".bz2");
        out.deleteOnExit();
        out.delete();
               
        CBZip2OutputStream cos = 
            new CBZip2OutputStream(new FileOutputStream(in));
        for (int i = 1; i < 100; i++) {
            StringBuffer sb = new StringBuffer();
            sb.append(i).append("\n").append(-i).append("\n");
            byte bytes[] = sb.toString().getBytes();
            cos.write(bytes);
        }
        cos.close();
                       
        pig.registerQuery("AA = load '"
                + Util.generateURI(in.getAbsolutePath(), pig.getPigContext())
                + "';");
        pig.registerQuery("A = foreach (group (filter AA by $0 > 0) all) generate flatten($1);");
        pig.registerQuery("store A into '" + out.getAbsolutePath() + "';");
        FileSystem fs = FileSystem.get(ConfigurationUtil.toConfiguration(
                pig.getPigContext().getProperties()));
        FSDataInputStream is = fs.open(new Path(out.getAbsolutePath() + 
                "/part-r-00000.bz2"));
        CBZip2InputStream cis = new CBZip2InputStream(is, -1, out.length());
        
        // Just a sanity check, to make sure it was a bzip file; we
        // will do the value verification later
        assertEquals(100, cis.read(new byte[100]));
        cis.close();
        
        pig.registerQuery("B = load '" + out.getAbsolutePath() + "';");
        
        Iterator<Tuple> i = pig.openIterator("B");
        HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();
        while (i.hasNext()) {
            Integer val = DataType.toInteger(i.next().get(0));
            map.put(val, val);            
        }
        
        assertEquals(new Integer(99), new Integer(map.keySet().size()));
        
        for (int j = 1; j < 100; j++) {
            assertEquals(new Integer(j), map.get(j));
        }
        
        in.delete();
        out.delete();
    }
    
   /**
    * Tests the end-to-end writing and reading of a BZip file using absolute path with a trailing /.
    */
    @Test
    public void testBzipInPig2() throws Exception {
        PigServer pig = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
       
        File in = File.createTempFile("junit", ".bz2");
        in.deleteOnExit();
        
        File out = File.createTempFile("junit", ".bz2");
        out.deleteOnExit();
        out.delete();
               
        CBZip2OutputStream cos = 
            new CBZip2OutputStream(new FileOutputStream(in));
        for (int i = 1; i < 100; i++) {
            StringBuffer sb = new StringBuffer();
            sb.append(i).append("\n").append(-i).append("\n");
            byte bytes[] = sb.toString().getBytes();
            cos.write(bytes);
        }
        cos.close();
                       
        pig.registerQuery("AA = load '"
                + Util.generateURI(in.getAbsolutePath(), pig.getPigContext())
                + "';");
        pig.registerQuery("A = foreach (group (filter AA by $0 > 0) all) generate flatten($1);");
        pig.registerQuery("store A into '" + out.getAbsolutePath() + "/';");
        FileSystem fs = FileSystem.get(ConfigurationUtil.toConfiguration(
                pig.getPigContext().getProperties()));
        FSDataInputStream is = fs.open(new Path(out.getAbsolutePath() + 
                "/part-r-00000.bz2"));
        CBZip2InputStream cis = new CBZip2InputStream(is, -1, out.length());
        
        // Just a sanity check, to make sure it was a bzip file; we
        // will do the value verification later
        assertEquals(100, cis.read(new byte[100]));
        cis.close();
        
        pig.registerQuery("B = load '" + out.getAbsolutePath() + "';");
        
        Iterator<Tuple> i = pig.openIterator("B");
        HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();
        while (i.hasNext()) {
            Integer val = DataType.toInteger(i.next().get(0));
            map.put(val, val);            
        }
        
        assertEquals(new Integer(99), new Integer(map.keySet().size()));
        
        for (int j = 1; j < 100; j++) {
            assertEquals(new Integer(j), map.get(j));
        }
        
        in.delete();
        out.delete();
    }

    //see PIG-2391
    @Test
    public void testBz2() throws Exception {
        String[] inputData = new String[] {
                "1\t2\r3\t4", // '\r' case - this will be split into two tuples
                "5\t6\r", // '\r\n' case
                "7\t8", // '\n' case
                "9\t10\r" // '\r\n' at the end of file
        };
        
        // bzip compressed input
        File in = File.createTempFile("junit", ".bz2");
        String compressedInputFileName = in.getAbsolutePath();
        in.deleteOnExit();
        
        try {
            CBZip2OutputStream cos = 
                new CBZip2OutputStream(new FileOutputStream(in));
            for (int i = 0; i < inputData.length; i++) {
                StringBuffer sb = new StringBuffer();
                sb.append(inputData[i]).append("\n");
                byte bytes[] = sb.toString().getBytes();
                cos.write(bytes);
            }
            cos.close();
            
            Util.copyFromLocalToCluster(cluster, compressedInputFileName,
                    compressedInputFileName);
            
            // pig script to read compressed input
            PigServer pig = new PigServer(ExecType.MAPREDUCE, cluster
                    .getProperties());
            
            // pig script to read compressed input
            String script ="a = load '" + compressedInputFileName +"';";
            pig.registerQuery(script);
            
            pig.registerQuery("store a into 'intermediate.bz';");
            pig.registerQuery("b = load 'intermediate.bz';");
            Iterator<Tuple> it2 = pig.openIterator("b");
			while (it2.hasNext()) {
				it2.next();
			}
        } finally {
            in.delete();
            Util.deleteFile(cluster, "intermediate.bz");
            Util.deleteFile(cluster, "final.bz");
        }
    }
    /** 
     * Tests that '\n', '\r' and '\r\n' are treated as record delims when using
     * bzip just like they are when using uncompressed text
     */
    @Test
    public void testRecordDelims() throws Exception {
        String[] inputData = new String[] {
                "1\t2\r3\t4", // '\r' case - this will be split into two tuples
                "5\t6\r", // '\r\n' case
                "7\t8", // '\n' case
                "9\t10\r" // '\r\n' at the end of file
        };
        
        // bzip compressed input
        File in = File.createTempFile("junit", ".bz2");
        String compressedInputFileName = in.getAbsolutePath();
        in.deleteOnExit();
        String unCompressedInputFileName = "testRecordDelims-uncomp.txt";
        Util.createInputFile(cluster, unCompressedInputFileName, inputData);
        
        try {
            CBZip2OutputStream cos = 
                new CBZip2OutputStream(new FileOutputStream(in));
            for (int i = 0; i < inputData.length; i++) {
                StringBuffer sb = new StringBuffer();
                sb.append(inputData[i]).append("\n");
                byte bytes[] = sb.toString().getBytes();
                cos.write(bytes);
            }
            cos.close();
            
            Util.copyFromLocalToCluster(cluster, compressedInputFileName,
                    compressedInputFileName);
            
            // pig script to read uncompressed input
            String script = "a = load '" + unCompressedInputFileName +"';";
            PigServer pig = new PigServer(ExecType.MAPREDUCE, cluster
                    .getProperties());
            pig.registerQuery(script);
            Iterator<Tuple> it1 = pig.openIterator("a");
            
            // pig script to read compressed input
            script = "a = load '" + compressedInputFileName +"';";
            pig.registerQuery(script);
            Iterator<Tuple> it2 = pig.openIterator("a");
            
            while(it1.hasNext()) {
                Tuple t1 = it1.next();
                Tuple t2 = it2.next();
                Assert.assertEquals(t1, t2);
            }
            
            Assert.assertFalse(it2.hasNext());
        
        } finally {
            in.delete();
            Util.deleteFile(cluster, unCompressedInputFileName);
        }
        
    }
    
    /**
     * Tests the end-to-end writing and reading of an empty BZip file.
     */
     @Test
     public void testEmptyBzipInPig() throws Exception {
        PigServer pig = new PigServer(ExecType.MAPREDUCE, cluster
                .getProperties());
 
        File in = File.createTempFile("junit", ".tmp");
        in.deleteOnExit();

        File out = File.createTempFile("junit", ".bz2");
        out.deleteOnExit();
        out.delete();
        
        FileOutputStream fos = new FileOutputStream(in);
        fos.write("55\n".getBytes());
        fos.close();
        System.out.println(in.getAbsolutePath());
        
        pig.registerQuery("AA = load '"
                + Util.generateURI(in.getAbsolutePath(), pig.getPigContext())
                + "';");
        pig.registerQuery("A=foreach (group (filter AA by $0 < '0') all) generate flatten($1);");
        pig.registerQuery("store A into '" + out.getAbsolutePath() + "';");
        FileSystem fs = FileSystem.get(ConfigurationUtil.toConfiguration(
                pig.getPigContext().getProperties()));
        FSDataInputStream is = fs.open(new Path(out.getAbsolutePath() + 
                "/part-r-00000.bz2"));
        CBZip2InputStream cis = new CBZip2InputStream(is, -1, out.length());
        
        // Just a sanity check, to make sure it was a bzip file; we
        // will do the value verification later
        assertEquals(-1, cis.read(new byte[100]));
        cis.close();
        
        pig.registerQuery("B = load '" + out.getAbsolutePath() + "';");
        pig.openIterator("B");
        
        in.delete();
        out.delete();
        
    }

    /**
     * Tests the writing and reading of an empty BZip file.
     */
    @Test
    public void testEmptyBzip() throws Exception {
        File tmp = File.createTempFile("junit", ".tmp");
        tmp.deleteOnExit();
        CBZip2OutputStream cos = new CBZip2OutputStream(new FileOutputStream(
                tmp));
        cos.close();
        assertNotSame(0, tmp.length());
        FileSystem fs = FileSystem.getLocal(new Configuration(false));
        CBZip2InputStream cis = new CBZip2InputStream(
                fs.open(new Path(tmp.getAbsolutePath())), -1, tmp.length());
        assertEquals(-1, cis.read(new byte[100]));
        cis.close();
        tmp.delete();
    }
    
    /**
     * Tests the case where a bzip block ends exactly at the end of the {@link InputSplit}
     * with the block header ending a few bits into the last byte of current
     * InputSplit. This case results in dropped records in Pig 0.6 release
     * This test also tests that bzip files couple of dirs deep can be read by
     * specifying the top level dir.
     */
    @Test
    public void testBlockHeaderEndingAtSplitNotByteAligned() throws IOException {
        // the actual input file is at
        // test/org/apache/pig/test/data/bzipdir1.bz2/bzipdir2.bz2/recordLossblockHeaderEndsAt136500.txt.bz2
        // In this test we will load test/org/apache/pig/test/data/bzipdir1.bz2 to also
        // test that the BZip2TextInputFormat can read subdirs recursively
        String inputFileName = 
            "test/org/apache/pig/test/data/bzipdir1.bz2";
        Long expectedCount = 74999L; // number of lines in above file
        // the first block in the above file exactly ends a few bits into the 
        // byte at position 136500 
        int splitSize = 136500;
        try {
            Util.copyFromLocalToCluster(cluster, inputFileName, inputFileName);
            testCount(inputFileName, expectedCount, splitSize, "PigStorage()");
            testCount(inputFileName, expectedCount, splitSize, "TextLoader()");
        } finally {
            Util.deleteFile(cluster, inputFileName);
        }
    }
    
    /**
     *  Tests the case where a bzip block ends exactly at the end of the input 
     *  split (byte aligned with the last byte) and the last byte is a carriage
     *  return.
     */
    @Test
    public void testBlockHeaderEndingWithCR() throws IOException {
        String inputFileName = 
            "test/org/apache/pig/test/data/blockEndingInCR.txt.bz2";
        // number of lines in above file (the value is 1 more than bzcat | wc -l
        // since there is a '\r' which is also treated as a record delim
        Long expectedCount = 82094L; 
        // the first block in the above file exactly ends at the byte at 
        // position 136498 and the last byte is a carriage return ('\r') 
        try {
            int splitSize = 136498;
            Util.copyFromLocalToCluster(cluster, inputFileName, inputFileName);
            testCount(inputFileName, expectedCount, splitSize, "PigStorage()");
        } finally {
            Util.deleteFile(cluster, inputFileName);
        }
    }
    
    /**
     * Tests the case where a bzip block ends exactly at the end of the input
     * split and has more data which results in overcounting (record duplication)
     * in Pig 0.6
     * 
     */
    @Test
    public void testBlockHeaderEndingAtSplitOverCounting() throws IOException {
       
        String inputFileName = 
            "test/org/apache/pig/test/data/blockHeaderEndsAt136500.txt.bz2";
        Long expectedCount = 1041046L; // number of lines in above file
        // the first block in the above file exactly ends a few bits into the 
        // byte at position 136500 
        int splitSize = 136500;
        try {
            Util.copyFromLocalToCluster(cluster, inputFileName, inputFileName);
            testCount(inputFileName, expectedCount, splitSize, "PigStorage()");
        } finally {
            Util.deleteFile(cluster, inputFileName);
        }
    }
    
    private void testCount(String inputFileName, Long expectedCount, 
            int splitSize, String loadFuncSpec) throws IOException {
        String outputFile = "/tmp/bz-output";
        // simple load-store script to verify that the bzip input is getting
        // split
        String scriptToTestSplitting = "a = load '" +inputFileName + "' using " +
        loadFuncSpec + "; store a into '" + outputFile + "';";
        
        String script = "a = load '" + inputFileName + "';" +
        		"b = group a all;" +
        		"c = foreach b generate COUNT_STAR(a);";
        Properties props = new Properties();
        for (Entry<Object, Object> entry : cluster.getProperties().entrySet()) {
            props.put(entry.getKey(), entry.getValue());
        }
        props.setProperty("mapred.max.split.size", Integer.toString(splitSize));
        PigContext pigContext = new PigContext(ExecType.MAPREDUCE, props);
        PigServer pig = new PigServer(pigContext);
        FileSystem fs = FileSystem.get(ConfigurationUtil.toConfiguration(props));
        fs.delete(new Path(outputFile), true);
        Util.registerMultiLineQuery(pig, scriptToTestSplitting);
        
        // verify that > 1 maps were launched due to splitting of the bzip input
        FileStatus[] files = fs.listStatus(new Path(outputFile));
        int numPartFiles = 0;
        for (FileStatus fileStatus : files) {
            if(fileStatus.getPath().getName().startsWith("part")) {
                numPartFiles++;
            }
        }
        assertEquals(true, numPartFiles > 0);
        
        // verify record count to verify we read bzip data correctly
        Util.registerMultiLineQuery(pig, script);
        Iterator<Tuple> it = pig.openIterator("c");
        Long result = (Long) it.next().get(0);
        assertEquals(expectedCount, result);
        
    }
    
    @Test
    public void testBzipStoreInMultiQuery() throws Exception {
        String[] inputData = new String[] {
                "1\t2\r3\t4"
        };
        
        String inputFileName = "input.txt";
        Util.createInputFile(cluster, inputFileName, inputData);
        
        PigServer pig = new PigServer(ExecType.MAPREDUCE, cluster
                .getProperties());
        
        pig.setBatchOn();
        pig.registerQuery("a = load '" +  inputFileName + "';");
        pig.registerQuery("store a into 'output.bz2';");
        pig.registerQuery("store a into 'output';");
        pig.executeBatch();
        
        FileSystem fs = FileSystem.get(ConfigurationUtil.toConfiguration(
                pig.getPigContext().getProperties()));
        FileStatus stat = fs.getFileStatus(new Path("output/part-m-00000"));        
        assertTrue(stat.getLen() > 0);     
        
        stat = fs.getFileStatus(new Path("output.bz2/part-m-00000.bz2"));
        assertTrue(stat.getLen() > 0);     
    }

    @Test
    public void testBzipStoreInMultiQuery2() throws Exception {
        String[] inputData = new String[] {
                "1\t2\r3\t4"
        };
        
        String inputFileName = "input2.txt";
        Util.createInputFile(cluster, inputFileName, inputData);
        
        PigServer pig = new PigServer(ExecType.MAPREDUCE, cluster
                .getProperties());
        PigContext pigContext = pig.getPigContext();
        pigContext.getProperties().setProperty( "output.compression.enabled", "true" );
        pigContext.getProperties().setProperty( "output.compression.codec", "org.apache.hadoop.io.compress.BZip2Codec" );
        
        pig.setBatchOn();
        pig.registerQuery("a = load '" +  inputFileName + "';");
        pig.registerQuery("store a into 'output2.bz2';");
        pig.registerQuery("store a into 'output2';");
        pig.executeBatch();
        
        FileSystem fs = FileSystem.get(ConfigurationUtil.toConfiguration(
                pig.getPigContext().getProperties()));
        FileStatus stat = fs.getFileStatus(new Path("output2/part-m-00000.bz2"));        
        assertTrue(stat.getLen() > 0);     
        
        stat = fs.getFileStatus(new Path("output2.bz2/part-m-00000.bz2"));
        assertTrue(stat.getLen() > 0);     
    }
    
    /** 
     * Tests that Pig throws an Exception when the input files to be loaded are actually
     * a result of concatenating 2 or more bz2 files. Pig should not silently ignore part 
     * of the input data.
     */
    @Test (expected=IOException.class)
    public void testBZ2Concatenation() throws Exception {
        String[] inputData1 = new String[] {
                "1\ta",
                "2\taa"
        };
        String[] inputData2 = new String[] {
                "1\tb",
                "2\tbb"
        };
        String[] inputDataMerged = new String[] {
                "1\ta",
                "2\taa",
                "1\tb",
                "2\tbb"
        };
       
        // bzip compressed input file1
        File in1 = File.createTempFile("junit", ".bz2");
        String compressedInputFileName1 = in1.getAbsolutePath();
        in1.deleteOnExit();
        
        // file2
        File in2 = File.createTempFile("junit", ".bz2");
        String compressedInputFileName2 = in2.getAbsolutePath();
        in1.deleteOnExit();

        String unCompressedInputFileName = "testRecordDelims-uncomp.txt";
        Util.createInputFile(cluster, unCompressedInputFileName, inputDataMerged);
        
        try {
            CBZip2OutputStream cos = 
                new CBZip2OutputStream(new FileOutputStream(in1));
            for (int i = 0; i < inputData1.length; i++) {
                StringBuffer sb = new StringBuffer();
                sb.append(inputData1[i]).append("\n");
                byte bytes[] = sb.toString().getBytes();
                cos.write(bytes);
            }
            cos.close();
            
            CBZip2OutputStream cos2 = 
                new CBZip2OutputStream(new FileOutputStream(in2));
            for (int i = 0; i < inputData2.length; i++) {
                StringBuffer sb = new StringBuffer();
                sb.append(inputData2[i]).append("\n");
                byte bytes[] = sb.toString().getBytes();
                cos2.write(bytes);
            }
            cos2.close();

            // cat
            catInto(compressedInputFileName2, compressedInputFileName1);
            Util.copyFromLocalToCluster(cluster, compressedInputFileName1,
                    compressedInputFileName1);
            
            // pig script to read uncompressed input
            String script = "a = load '" + unCompressedInputFileName +"';";
            PigServer pig = new PigServer(ExecType.MAPREDUCE, cluster
                    .getProperties());
            pig.registerQuery(script);
            Iterator<Tuple> it1 = pig.openIterator("a");
            
            // pig script to read compressed concatenated input
            script = "a = load '" + compressedInputFileName1 +"';";
            pig.registerQuery(script);
            Iterator<Tuple> it2 = pig.openIterator("a");
            
            while(it1.hasNext()) {
                Tuple t1 = it1.next();
                Tuple t2 = it2.next();
                Assert.assertEquals(t1, t2);
            }
            
            Assert.assertFalse(it2.hasNext());
        
        } finally {
            in1.delete();
            in2.delete();
            Util.deleteFile(cluster, unCompressedInputFileName);
        }
        
    }
    
    /*
     * Concatenate the contents of src file to the contents of dest file
     */
    private void catInto(String src, String dest) throws IOException {
    	BufferedWriter out = new BufferedWriter(new FileWriter(dest, true));
    	BufferedReader in = new BufferedReader(new FileReader(src));
    	String str;
    	while ((str = in.readLine()) != null) {
    		out.write(str);
    	}
    	in.close();
    	out.close();
    }
    
    // See PIG-1714
    @Test
    public void testBzipStoreInMultiQuery3() throws Exception {
        String[] inputData = new String[] {
                "1\t2\r3\t4"
        };
        
        String inputFileName = "input3.txt";
        Util.createInputFile(cluster, inputFileName, inputData);

        String inputScript = "set mapred.output.compress true\n" +
                "set mapreduce.output.fileoutputformat.compress true\n" +
                "set mapred.output.compression.codec org.apache.hadoop.io.compress.BZip2Codec\n" +
                "set mapreduce.output.fileoutputformat.compress.codec org.apache.hadoop.io.compress.BZip2Codec\n" +
                "a = load '" + inputFileName + "';\n" +
                "store a into 'output3.bz2';\n" +
                "store a into 'output3';";
        
        String inputScriptName = "script3.txt";
        PrintWriter pw = new PrintWriter(new FileWriter(inputScriptName));
        pw.println(inputScript);
        pw.close();
        
        PigServer pig = new PigServer(ExecType.MAPREDUCE, cluster
                .getProperties());
        
        FileInputStream fis = new FileInputStream(inputScriptName);
        pig.registerScript(fis);
        
        FileSystem fs = FileSystem.get(ConfigurationUtil.toConfiguration(
                pig.getPigContext().getProperties()));
        FileStatus stat = fs.getFileStatus(new Path("output3/part-m-00000.bz2"));        
        assertTrue(stat.getLen() > 0);     
        
        stat = fs.getFileStatus(new Path("output3.bz2/part-m-00000.bz2"));
        assertTrue(stat.getLen() > 0);     
    }
 
}
