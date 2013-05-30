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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.pig.ExecType;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.apache.pig.piggybank.storage.AllLoader;
import org.apache.pig.piggybank.storage.HiveColumnarLoader;
import org.apache.pig.piggybank.storage.allloader.LoadFuncHelper;
import org.apache.pig.test.Util;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mortbay.log.Log;

public class TestAllLoader extends TestCase {

    enum TYPE {
        HIVERC(".rc", new HiveRCFileTestWriter()), GZIP_PLAIN(".gz",
                new GzipFileTestWriter());

        String extension;
        FileTestWriter writer;

        TYPE(String extension, FileTestWriter writer) {
            this.extension = extension;
            this.writer = writer;
        }

    }

    String colSeparator = ",";

    /**
     * All test files will contain this amount of records
     */
    int fileRecords = 100;
    /**
     * All files will contain this amount of columns in each row
     */
    int fileColumns = 2;

    final TYPE fileTypes[] = new TYPE[] { TYPE.GZIP_PLAIN, TYPE.HIVERC };

    final String extensionLoaders = "gz:org.apache.pig.builtin.PigStorage('"
            + colSeparator + "'), rc:"
            + HiveColumnarLoader.class.getCanonicalName()
            + "('v1 float,v2 float')";

    File baseDir;

    File simpleDir;

    // --------------- For date partitioning based on daydate= (this will work
    // for year=, day= etc
    File datePartitionDir;
    final String[] datePartitions = new String[] { "daydate=2010-11-01",
            "daydate=2010-11-02", "daydate=2010-11-03", "daydate=2010-11-04",
            "daydate=2010-11-05" };

    // -------------- Logic partitioning that does not involve dates
    File logicPartitionDir;
    final String[] logicPartitions = new String[] { "block=1", "block=2",
            "block=3" };

    // -------------- Logic Tagged partitioning that is /type1/block=1
    // /type2/block2
    // This is needed because the Schema for each path might be different and we
    // want to use a different loader based on if we are
    // looking in type1 or in type2 see taggedExtensionLoaders
    File taggedLogicPartitionDir;
    final String taggedExtensionLoaders = "gz:org.apache.pig.builtin.PigStorage('"
            + colSeparator
            + "'), rc:type1:"
            + HiveColumnarLoader.class.getCanonicalName()
            + "('v1 float,v2 float'), rc:type2:"
            + HiveColumnarLoader.class.getCanonicalName() + "('v1 float')";

    final String[] tags = new String[] { "type1", "type2" };
    final String[] taggedSchemas = new String[] { "(p: float, q:float)",
            "(p:float)" };

    // -------------- Test Load Files by Content
    File filesByContentDir;
    final String contentLoaders = "gz:org.apache.pig.builtin.PigStorage('"
            + colSeparator
            + "'), rc:"
            + HiveColumnarLoader.class.getCanonicalName()
            + "('v1 float,v2 float'), seq::org.apache.hadoop.hive.ql.io.RCFile:"
            + HiveColumnarLoader.class.getCanonicalName()
            + "('v1 float,v2 float')";

    Properties configuration;

    final String allLoaderName = AllLoader.class.getCanonicalName();
    final String allLoaderFuncSpec = allLoaderName + "()";

    PigServer server;

    /**
     * Test that we can load files with the correct loaders as per file content.
     * 
     * @throws IOException
     */
    @Test
    public void testFilesByContentDir() throws IOException {

        server.shutdown();

        configuration.setProperty(LoadFuncHelper.FILE_EXTENSION_LOADERS,
                contentLoaders);

        server = new PigServer(ExecType.LOCAL, configuration);

        server.setBatchOn();
        server.registerFunction(allLoaderName, new FuncSpec(allLoaderFuncSpec));

        server.registerQuery("a = LOAD '" + Util.encodeEscape(filesByContentDir.getAbsolutePath())
                + "' using " + allLoaderFuncSpec + " as (p:float, q:float);");

        readRecordsFromLoader(server, "a", fileRecords);

    }

    /**
     * Test that we can read a tagged logic partitioned directory
     * 
     * @throws IOException
     */
    @Test
    public void testTaggedLogicPartitionDir() throws IOException {

        server.shutdown();

        configuration.setProperty(LoadFuncHelper.FILE_EXTENSION_LOADERS,
                taggedExtensionLoaders);

        server = new PigServer(ExecType.LOCAL, configuration);

        server.setBatchOn();
        server.registerFunction(allLoaderName, new FuncSpec(allLoaderFuncSpec));

        int i = 0;
        for (String tag : tags) {

            String schema = taggedSchemas[i++];

            server.registerQuery(tag + " = LOAD '"
                    + Util.encodeEscape(taggedLogicPartitionDir.getAbsolutePath()) + "/" + tag
                    + "' using " + allLoaderFuncSpec + " as " + schema + ";");

            readRecordsFromLoader(server, tag, fileRecords
                    * logicPartitions.length * fileTypes.length);
        }

    }

    /**
     * Test that we can filter by a logic partition based on a range, in this
     * case block<=2
     * 
     * @throws IOException
     */
    @Test
    public void testLogicPartitionFilter() throws IOException {

        server.registerQuery("a = LOAD '" + Util.encodeEscape(logicPartitionDir.getAbsolutePath())
                + "' using " + allLoaderName + "('block<=2')"
                + " as (q:float, p:float);");

        server.registerQuery("r = FOREACH a GENERATE q, p;");

        Iterator<Tuple> it = server.openIterator("r");

        int count = 0;

        while (it.hasNext()) {
            count++;
            Tuple t = it.next();
            // System.out.println(count + " : " + t.toDelimitedString(","));
            assertEquals(2, t.size());

        }

        // only 2 partitions are used in the query block=3 is filtered out.
        assertEquals(fileRecords * 2 * fileTypes.length, count);

    }

    /**
     * Test that we can extract only the partition column
     * 
     * @throws IOException
     */
    @Test
    public void testLogicPartitionPartitionColumnExtract() throws IOException {

        server.registerQuery("a = LOAD '" + Util.encodeEscape(logicPartitionDir.getAbsolutePath())
                + "' using " + allLoaderFuncSpec
                + " as (q:float, p:float, block:chararray);");

        server.registerQuery("r = foreach a generate block;");

        Iterator<Tuple> it = server.openIterator("r");

        int count = 0;
        Map<String, AtomicInteger> partitionCount = new HashMap<String, AtomicInteger>();

        while (it.hasNext()) {
            count++;
            Tuple t = it.next();

            assertEquals(1, t.size());
            String key = t.get(0).toString();

            AtomicInteger ati = partitionCount.get(key);
            if (ati == null) {
                ati = new AtomicInteger(1);
                partitionCount.put(key, ati);
            } else {
                ati.incrementAndGet();
            }

        }

        // test that all partitions where read
        for (AtomicInteger ati : partitionCount.values()) {
            assertEquals(fileRecords * fileTypes.length, ati.get());
        }

        assertEquals(fileRecords * logicPartitions.length * fileTypes.length,
                count);

    }

    /**
     * Test that we can read a logic partitioned directory
     * 
     * @throws IOException
     */
    @Test
    public void testLogicPartitionDir() throws IOException {

        server.registerQuery("a = LOAD '" + Util.encodeEscape(logicPartitionDir.getAbsolutePath())
                + "' using " + allLoaderFuncSpec + " as (q:float, p:float);");

        readRecordsFromLoader(server, "a", fileRecords * logicPartitions.length
                * fileTypes.length);

    }

    /**
     * Test that we can filter a date partitioned directory by a date range
     * 
     * @throws IOException
     */
    @Test
    public void testDateParitionFilterWithAsSchema() throws IOException {

        server.registerQuery("a = LOAD '"
                + Util.encodeEscape(datePartitionDir.getAbsolutePath())
                + "' using "
                + allLoaderName
                + "('daydate >= \"2010-11-02\" and daydate <= \"2010-11-04\"') AS (q:float, p:float, daydate:chararray); ");

        server.registerQuery("r = FOREACH a GENERATE $0;");

        Iterator<Tuple> it = server.openIterator("r");

        int count = 0;

        while (it.hasNext()) {
            count++;
            Tuple t = it.next();

            assertEquals(1, t.size());

        }
        // we filter out 2 date partitions using only 3
        readRecordsFromLoader(server, "a", fileRecords * 3 * fileTypes.length);
    }

    /**
     * Test that we can filter a date partitioned directory by a date range
     * 
     * @throws IOException
     */
    @Test
    public void testDateParitionFilterWithoutSchema() throws IOException {

        server.registerQuery("a = LOAD '"
                + Util.encodeEscape(datePartitionDir.getAbsolutePath())
                + "' using "
                + allLoaderName
                + "('daydate >= \"2010-11-02\" and daydate <= \"2010-11-04\"'); ");

        server.registerQuery("r = FOREACH a GENERATE $0;");

        Iterator<Tuple> it = server.openIterator("r");

        int count = 0;

        while (it.hasNext()) {
            count++;
            Tuple t = it.next();

            float f = Float.valueOf(t.get(0).toString());
            assertTrue(f < 1L);
            assertEquals(1, t.size());

        }
        // we filter out 2 date partitions using only 3
        readRecordsFromLoader(server, "a", fileRecords * 3 * fileTypes.length);
    }

    /**
     * Test that we can read a date partitioned directory
     * 
     * @throws IOException
     * @throws ParseException
     */
    @Test
    public void testDateParitionDir() throws IOException, ParseException {

        server.registerQuery("a = LOAD '" + Util.encodeEscape(datePartitionDir.getAbsolutePath())
                + "' using " + allLoaderFuncSpec
                + " as (q:float, p:float, daydate:chararray);");

        server.registerQuery("r = FOREACH a GENERATE daydate;");

        Iterator<Tuple> it = server.openIterator("r");

        DateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd");

        Calendar cal = Calendar.getInstance();

        while (it.hasNext()) {
            Date date = dateformat.parse(it.next().get(0).toString());
            cal.setTime(date);
            assertEquals(2010, cal.get(Calendar.YEAR));
            assertEquals(10, cal.get(Calendar.MONTH)); // month starts at 0 so
                                                        // November 11 is 10

        }

        readRecordsFromLoader(server, "a", fileRecords * datePartitions.length
                * fileTypes.length);
    }

    /**
     * Test that we can read from a simple directory
     * 
     * @throws IOException
     */
    @Test
    public void testSimpleDir() throws IOException {

        server.registerQuery("a = LOAD '" + Util.encodeEscape(simpleDir.getAbsolutePath())
                + "' using " + allLoaderFuncSpec + " as (q:float, p:float);");

        readRecordsFromLoader(server, "a", fileRecords * fileTypes.length);

    }

    /**
     * Validates that the loadAlias can read the correct amount of records
     * 
     * @param server
     * @param loadAlias
     * @throws IOException
     */
    private void readRecordsFromLoader(PigServer server, String loadAlias,
            int totalRowCount) throws IOException {

        Iterator<Tuple> result = server.openIterator(loadAlias);
        int count = 0;

        while ((result.next()) != null) {
            count++;
        }

        Log.info("Validating expected: " + totalRowCount + " against " + count);
        assertEquals(totalRowCount, count);
    }

    @Before
    public void setUp() throws Exception {

        Configuration hadoopConf = new Configuration();
        FileSystem.setDefaultUri(hadoopConf,
                LocalFileSystem.getDefaultUri(hadoopConf));

        if (baseDir == null) {
            configuration = new Properties();
            configuration.setProperty(LoadFuncHelper.FILE_EXTENSION_LOADERS,
                    extensionLoaders);

            baseDir = new File("build/test/testAllLoader");
            if (baseDir.exists()) {
                FileUtil.fullyDelete(baseDir);
            }

            assertTrue(baseDir.mkdirs());

            createSimpleDir();
            createDatePartitionDir();
            createLogicPartitionDir();
            createTaggedLogicPartitionDir();
            createFileByContentDir();

            server = new PigServer(ExecType.LOCAL, configuration);

            server.setBatchOn();
        }
    }

    /**
     * Write out all files without there extensions
     * 
     * @throws IOException
     */
    private void createFileByContentDir() throws IOException {

        filesByContentDir = new File(baseDir, "filesByContentDir");
        assertTrue(filesByContentDir.mkdirs());

        // for each type create the files without its extension

        File uniqueFile = new File(filesByContentDir, ""
                + System.currentTimeMillis());
        TYPE.HIVERC.writer.writeTestData(uniqueFile, fileRecords, fileColumns,
                colSeparator);

    }

    /**
     * Write out the tagged logical partitioning directories e.g. type=1/block=1
     * 
     * @throws IOException
     */
    private void createTaggedLogicPartitionDir() throws IOException {
        taggedLogicPartitionDir = new File(baseDir, "taggedLogicPartitionDir");
        assertTrue(taggedLogicPartitionDir.mkdirs());

        // for each tag create a directory
        for (String tag : tags) {
            // for each logic partition create a directory
            File tagDir = new File(taggedLogicPartitionDir, tag);
            for (String partition : logicPartitions) {
                File logicPartition = new File(tagDir, partition);

                assertTrue(logicPartition.mkdirs());

                for (TYPE fileType : fileTypes) {
                    writeFile(logicPartition, fileType);
                }
            }

        }
    }

    /**
     * Write out logical partitioned directories with one file per fileType:TYPE
     * 
     * @throws IOException
     */
    private void createLogicPartitionDir() throws IOException {

        logicPartitionDir = new File(baseDir, "logicPartitionDir");
        assertTrue(logicPartitionDir.mkdirs());

        // for each logic partition create a directory
        for (String partition : logicPartitions) {
            File logicPartition = new File(logicPartitionDir, partition);

            assertTrue(logicPartition.mkdirs());

            for (TYPE fileType : fileTypes) {
                writeFile(logicPartition, fileType);
            }
        }

    }

    /**
     * Write out date partitioned directories with one file per fileType:TYPE
     * 
     * @throws IOException
     */
    private void createDatePartitionDir() throws IOException {

        datePartitionDir = new File(baseDir, "dateParitionDir");
        assertTrue(datePartitionDir.mkdirs());

        // for each date partition create a directory
        for (String partition : datePartitions) {
            File datePartition = new File(datePartitionDir, partition);

            assertTrue(datePartition.mkdirs());

            for (TYPE fileType : fileTypes) {
                writeFile(datePartition, fileType);
            }
        }

    }

    /**
     * Write out a simple directory with one file per fileType:TYPE
     * 
     * @throws IOException
     */
    private void createSimpleDir() throws IOException {

        simpleDir = new File(baseDir, "simpleDir");
        assertTrue(simpleDir.mkdirs());

        for (TYPE fileType : fileTypes) {
            writeFile(simpleDir, fileType);
        }

    }

    /**
     * Create a unique file name with format
     * [currentTimeInMillis].[type.extension]
     * 
     * @param dir
     * @param type
     * @throws IOException
     */
    private void writeFile(File dir, TYPE type) throws IOException {
        File uniqueFile = new File(dir, System.currentTimeMillis()
                + type.extension);
        type.writer.writeTestData(uniqueFile, fileRecords, fileColumns,
                colSeparator);
    }

    @After
    public void tearDown() throws Exception {

        server.shutdown();

        FileUtil.fullyDelete(baseDir);
        baseDir = null;

    }

    /**
     * Simple interface to help with writting test data.
     * 
     */
    private static interface FileTestWriter {
        void writeTestData(File file, int recordCounts, int columnCount,
                String colSeparator) throws IOException;
    }

    /**
     * Write Gzip Content
     * 
     */
    private static class GzipFileTestWriter implements FileTestWriter {

        @Override
        public void writeTestData(File file, int recordCounts, int columnCount,
                String colSeparator) throws IOException {

            // write random test data
            GzipCodec gzipCodec = new GzipCodec();
            CompressionOutputStream out = gzipCodec
                    .createOutputStream(new FileOutputStream(file));
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
                    out));

            try {

                for (int r = 0; r < recordCounts; r++) {
                    // foreach row write n columns

                    for (int c = 0; c < columnCount; c++) {

                        if (c != 0) {
                            writer.append(colSeparator);
                        }

                        writer.append(String.valueOf(Math.random()));

                    }
                    writer.append("\n");

                }

            } finally {
                writer.close();
                out.close();
            }

        }

    }

    private static class HiveRCFileTestWriter implements FileTestWriter {

        @Override
        public void writeTestData(File file, int recordCounts, int columnCount,
                String colSeparator) throws IOException {

            // write random test data

            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.getLocal(conf);

            RCFileOutputFormat.setColumnNumber(conf, columnCount);
            RCFile.Writer writer = new RCFile.Writer(fs, conf, new Path(
                    file.getAbsolutePath()));

            BytesRefArrayWritable bytes = new BytesRefArrayWritable(columnCount);

            for (int c = 0; c < columnCount; c++) {
                bytes.set(c, new BytesRefWritable());
            }

            try {

                for (int r = 0; r < recordCounts; r++) {
                    // foreach row write n columns

                    for (int c = 0; c < columnCount; c++) {

                        byte[] stringbytes = String.valueOf(Math.random())
                                .getBytes();
                        bytes.get(c).set(stringbytes, 0, stringbytes.length);

                    }

                    writer.append(bytes);

                }

            } finally {
                writer.close();
            }

        }

    }

}
