/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.pig.piggybank.test.storage;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.pig.ExecType;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigServer;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.junit.Assert;
import org.junit.Test;

public class TestHiveColumnarLoader extends TestCase{

    static Configuration conf = new Configuration();


    static File simpleDataFile = null;
    static File datePartitionedDir = null;
    //used for cleanup
    static List<String> datePartitionedRCFiles;
    static List<String> datePartitionedDirs;

    static private FileSystem fs;

    static int columnMaxSize = 30;

    static int columnCount = 3;

    static int simpleRowCount = 10;


    static String endingDate = null;
    static String startingDate = null;
    static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    static Calendar calendar = null;
    static int datePartitionedRowCount;

    @Override
    public synchronized void setUp() throws Exception {

        fs = LocalFileSystem.getLocal(conf);

        produceSimpleData();

        produceDatePartitionedData();

    }


    @Override
    public  void tearDown(){
        deletePartitionedDirs(datePartitionedDir);
        datePartitionedDir.delete();
    }

    private void deletePartitionedDirs(File file){

        if(file.isDirectory()){
            File[] children = file.listFiles();
            if(children != null){
                for(File child : children){
                    deletePartitionedDirs(child);
                }
            }
        }

        file.delete();

    }

    @Test
    public void test1DayDatePartitionedFilesWithProjection() throws IOException{
        int count = 0;

        String funcSpecString = "org.apache.pig.piggybank.storage.HiveColumnarLoader('f1 string,f2 string,f3 string'" + 
        ", '" + startingDate + ":" + startingDate + "')";

        System.out.println(funcSpecString);

        PigServer server = new PigServer(ExecType.LOCAL);
        server.setBatchOn();
        server.registerFunction("org.apache.pig.piggybank.storage.HiveColumnarLoader", new FuncSpec(funcSpecString));


        server.registerQuery("a = LOAD '" + datePartitionedDir.getAbsolutePath() +  "' using " + funcSpecString + ";");
        server.registerQuery("b = FOREACH a GENERATE f2 as p;");
        Iterator<Tuple> result = server.openIterator("b");

        Tuple t = null;
        while( (t = result.next()) != null) {
            assertEquals(1, t.size());
            assertEquals(DataType.CHARARRAY, t.getType(0));
            count++;
        }

        Assert.assertEquals(50, count);
    }

    @Test
    public void test1DayDatePartitionedFiles() throws IOException{
        int count = 0;

        String funcSpecString = "org.apache.pig.piggybank.storage.HiveColumnarLoader('f1 string,f2 string,f3 string'" + 
        ", '" + startingDate + ":" + startingDate + "')";

        System.out.println(funcSpecString);

        PigServer server = new PigServer(ExecType.LOCAL);
        server.setBatchOn();
        server.registerFunction("org.apache.pig.piggybank.storage.HiveColumnarLoader", new FuncSpec(funcSpecString));


        server.registerQuery("a = LOAD '" + datePartitionedDir.getAbsolutePath() +  "' using " + funcSpecString + ";");
        Iterator<Tuple> result = server.openIterator("a");


        while( (result.next()) != null){
            count++;
        }


        Assert.assertEquals(50, count);
    }

    @Test
    public void testDatePartitionedFiles() throws IOException{
        int count = 0;

        String funcSpecString = "org.apache.pig.piggybank.storage.HiveColumnarLoader('f1 string,f2 string,f3 string'" 
            + ", '" + startingDate + ":" + endingDate + "')";

        System.out.println(funcSpecString);

        PigServer server = new PigServer(ExecType.LOCAL);
        server.setBatchOn();
        server.registerFunction("org.apache.pig.piggybank.storage.HiveColumnarLoader", new FuncSpec(funcSpecString));


        server.registerQuery("a = LOAD '" + datePartitionedDir.getAbsolutePath() +  "' using " + funcSpecString + ";");
        Iterator<Tuple> result = server.openIterator("a");


        while( (result.next()) != null){
            count++;
        }

        Assert.assertEquals(datePartitionedRowCount, count);
    }

    private static void produceDatePartitionedData() throws IOException {
        datePartitionedRowCount = 0;
        datePartitionedDir = new File("testhiveColumnarLoader-dateDir-" + System.currentTimeMillis());
        datePartitionedDir.mkdir();
        datePartitionedDir.deleteOnExit();

        int dates = 4;
        calendar = Calendar.getInstance();

        calendar.set(Calendar.DAY_OF_MONTH, Calendar.MONDAY);
        calendar.set(Calendar.MONTH, Calendar.JANUARY);

        startingDate = dateFormat.format(calendar.getTime());

        datePartitionedRCFiles = new ArrayList<String>();
        datePartitionedDirs  = new ArrayList<String>();

        for(int i = 0; i < dates; i++){

            File file = new File(datePartitionedDir, "daydate=" + dateFormat.format(calendar.getTime()));
            calendar.add(Calendar.DAY_OF_MONTH, 1);

            file.mkdir();
            file.deleteOnExit();

            //for each daydate write 5 partitions
            for(int pi = 0; pi < 5; pi++){
                Path path = new Path(new Path(file.getAbsolutePath()), 
                        "parition" + pi);

                datePartitionedRowCount += 
                    writeRCFileTest(fs, simpleRowCount, path, columnCount, new DefaultCodec(), columnCount);

                new File(path.toString()).deleteOnExit();
                datePartitionedRCFiles.add(path.toString());
                datePartitionedDirs.add(file.toString());

            }

        }

        endingDate = dateFormat.format(calendar.getTime());
    }


    /**
     * Writes out a simple temporary file with 5 columns and 100 rows.<br/>
     * Data is random numbers.
     * @throws SerDeException 
     * @throws IOException 
     */
    private static final void produceSimpleData() throws SerDeException, IOException{
        simpleDataFile = File.createTempFile("testhiveColumnarLoader", ".txt");
        simpleDataFile.deleteOnExit();

        Path path = new Path(simpleDataFile.getPath());

        writeRCFileTest(fs, simpleRowCount, path, columnCount, new DefaultCodec(), columnCount);

    }

    static Random randomCharGenerator = new Random(3);

    static Random randColLenGenerator = new Random(20);

    private static void resetRandomGenerators() {
        randomCharGenerator = new Random(3);
        randColLenGenerator = new Random(20);
    }

    private static int writeRCFileTest(FileSystem fs, int rowCount, Path file,
            int columnNum, CompressionCodec codec, int columnCount) throws IOException {
        fs.delete(file, true);
        int rowsWritten = 0;

        resetRandomGenerators();

        RCFileOutputFormat.setColumnNumber(conf, columnNum);
        RCFile.Writer writer = new RCFile.Writer(fs, conf, file, null, codec);

        byte[][] columnRandom;

        BytesRefArrayWritable bytes = new BytesRefArrayWritable(columnNum);
        columnRandom = new byte[columnNum][];
        for (int i = 0; i < columnNum; i++) {
            BytesRefWritable cu = new BytesRefWritable();
            bytes.set(i, cu);
        }

        for (int i = 0; i < rowCount; i++) {
            nextRandomRow(columnRandom, bytes, columnCount);
            rowsWritten++;
            writer.append(bytes);
        }
        writer.close();

        return rowsWritten;
    }

    private static void nextRandomRow(byte[][] row, BytesRefArrayWritable bytes, int columnCount) {
        bytes.resetValid(row.length);
        for (int i = 0; i < row.length; i++) {

            row[i] = new byte[columnCount];
            for (int j = 0; j < columnCount; j++)
                row[i][j] = getRandomChar(randomCharGenerator);
            bytes.get(i).set(row[i], 0, columnCount);
        }
    }

    private static int CHAR_END = 122 - 7;

    private static byte getRandomChar(Random random) {
        byte b = 0;
        do {
            b = (byte) random.nextInt(CHAR_END);
        } while ((b < 65));
        if (b > 90) {
            b = 7;
        }
        return b;
    }
}
