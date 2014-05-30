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

import static org.apache.pig.builtin.mock.Storage.tuple;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.ResourceSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.test.utils.TypeCheckingTestUtil;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestPigStorage  {
    private static PigServer pig;
    private static PigContext pigContext;
    private static Properties properties;
    private static MiniGenericCluster cluster;
    private static final String datadir = "build/test/tmpdata/";

    @Before
    public void setup() throws IOException {
        // some tests are in map-reduce mode and some in local - so before
        // each test, we will re-initialize FileLocalizer so that temp files
        // are created correctly depending on the ExecType in the test.
        Util.resetStateForExecModeSwitch();

        // If needed, a test can change that. Most tests are local so we save a bit
        // of typing here.
        pig = new PigServer(ExecType.LOCAL);
        Util.deleteDirectory(new File(datadir));
        try {
            pig.mkdirs(datadir);
        } catch (IOException e) {};
        Util.createLocalInputFile(datadir + "originput",
                new String[] {"A,1", "B,2", "C,3", "D,2",
                "A,5", "B,5", "C,8", "A,8",
                "D,8", "A,9"});

    }

    @After
    public void tearDown() throws Exception {
        Util.deleteDirectory(new File(datadir));
        pig.shutdown();
    }

    @BeforeClass
    public static void oneTimeSetup() {
        cluster = MiniGenericCluster.buildCluster();
        properties = cluster.getProperties();
        pigContext = new PigContext(ExecType.LOCAL, new Properties());
    }

    @AfterClass
    public static void shutdown() {
        cluster.shutDown();
    }

    private static void assertAliasIs(String alias, List<Tuple> expectedResults)
            throws IOException {
        Iterator<Tuple> iter = pig.openIterator(alias);
        int counter = 0;
        while (iter.hasNext()) {
            Assert.assertEquals(expectedResults.get(counter++).toString(), iter.next().toString());
        }
        Assert.assertEquals(expectedResults.size(), counter);
    }

    @Test
    public void testBlockBoundary() throws ExecException {

        // This tests PigStorage loader with records exactly
        // on the boundary of the file blocks.
        Properties props = new Properties();
        for (Entry<Object, Object> entry : properties.entrySet()) {
            props.put(entry.getKey(), entry.getValue());
        }
        props.setProperty("mapred.max.split.size", "20");
        Util.resetStateForExecModeSwitch();
        PigServer pigServer = new PigServer(cluster.getExecType(), props);
        String[] inputs = {
                "abcdefgh1", "abcdefgh2", "abcdefgh3",
                "abcdefgh4", "abcdefgh5", "abcdefgh6",
                "abcdefgh7", "abcdefgh8", "abcdefgh9"
        };

        String[] expected = {
                "(abcdefgh1)", "(abcdefgh2)", "(abcdefgh3)",
                "(abcdefgh4)", "(abcdefgh5)", "(abcdefgh6)",
                "(abcdefgh7)", "(abcdefgh8)", "(abcdefgh9)"
        };

        System.setProperty("pig.overrideBlockSize", "20");

        String INPUT_FILE = "tmp.txt";

        try {

            PrintWriter w = new PrintWriter(new FileWriter(INPUT_FILE));
            for (String s : inputs) {
                w.println(s);
            }
            w.close();

            Util.copyFromLocalToCluster(cluster, INPUT_FILE, INPUT_FILE);

            pigServer.registerQuery("a = load '" + INPUT_FILE + "';");

            Iterator<Tuple> iter = pigServer.openIterator("a");
            int counter = 0;
            while (iter.hasNext()){
                assertEquals(expected[counter++].toString(), iter.next().toString());
            }

            assertEquals(expected.length, counter);

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            new File(INPUT_FILE).delete();
            try {
                Util.deleteFile(cluster, INPUT_FILE);
            } catch (IOException e) {
                e.printStackTrace();
                Assert.fail();
            }
        }
    }

    /**
     * Test to verify that PigStorage works fine in the following scenario:
     * The column prune optimization determines only columns 2 and 3 are needed
     * and there are records in the data which have only 1 column (malformed data).
     * In this case, PigStorage should return an empty tuple to represent columns
     * 2 and 3 and {@link POProject} would handle catching any
     * {@link IndexOutOfBoundsException} resulting from accessing a field in the
     * tuple and substitute a null.
     */
    @Test
    public void testPruneColumnsWithMissingFields() throws IOException {
        String inputFileName = "TestPigStorage-testPruneColumnsWithMissingFields-input.txt";
        Util.createLocalInputFile(
                inputFileName,
                new String[] {"1\t2\t3", "4", "5\t6\t7"});
        String script = "a = load '" + inputFileName + "' as (i:int, j:int, k:int);" +
                "b = foreach a generate j, k;";
        Util.registerMultiLineQuery(pig, script);
        Iterator<Tuple> it = pig.openIterator("b");
        assertEquals(Util.createTuple(new Integer[] { 2, 3}), it.next());
        assertEquals(Util.createTuple(new Integer[] { null, null}), it.next());
        assertEquals(Util.createTuple(new Integer[] { 6, 7}), it.next());
        assertFalse(it.hasNext());

    }

    @Test
    public void testPigStorageNoSchema() throws Exception {
        //if the schema file does not exist, and '-schema' option is used
        // it should result in an error
        pigContext.connect();
        String query = "a = LOAD '" + datadir + "originput' using PigStorage('\\t', '-schema') " +
        "as (f1:chararray, f2:int);";
        try{
            pig.registerQuery(query);
            pig.dumpSchema("a");
        }catch(FrontendException ex){
            assertEquals(ex.toString(), 1000, ex.getErrorCode());
            return;
        }
        fail("no exception caught");
    }

    @Test
    public void testPigStorageSchema() throws Exception {
        pigContext.connect();
        String query = "a = LOAD '" + datadir + "originput' using PigStorage('\\t') " +
        "as (f1:chararray, f2:int);";
        pig.registerQuery(query);
        Schema origSchema = pig.dumpSchema("a");
        pig.store("a", datadir + "aout", "PigStorage('\\t', '-schema')");

        // aout now has a schema.

        // Verify that loading a-out with no given schema produces
        // the original schema.

        pig.registerQuery("b = LOAD '" + datadir + "aout' using PigStorage('\\t');");
        Schema genSchema = pig.dumpSchema("b");
        Assert.assertTrue("generated schema equals original" ,
                Schema.equals(genSchema, origSchema, true, false));

        // Verify that giving our own schema works
        String [] aliases ={"foo", "bar"};
        byte[] types = {DataType.INTEGER, DataType.LONG};
        Schema newSchema = TypeCheckingTestUtil.genFlatSchema(
                aliases,types);
        pig.registerQuery("c = LOAD '" + datadir + "aout' using PigStorage('\\t', '-schema') "+
        "as (foo:int, bar:long);");
        Schema newGenSchema = pig.dumpSchema("c");
        Assert.assertTrue("explicit schema overrides metadata",
                Schema.equals(newSchema, newGenSchema, true, false));

        // Verify that explicitly requesting no schema works
        pig.registerQuery("d = LOAD '" + datadir + "aout' using PigStorage('\\t', '-noschema');");
        genSchema = pig.dumpSchema("d");
        assertNull(genSchema);
    }

    @Test
    public void testPruneColumnsWithSchema() throws Exception {
        pigContext.connect();
        String query = "a = LOAD '" + datadir + "originput' using PigStorage(',') " +
        "as (f1:chararray, f2:int);";
        pig.registerQuery(query);
        pig.store("a", datadir + "aout", "PigStorage('\\t', '-schema')");

        // aout now has a schema.

        // Verify that loaded data has the correct data type after the prune
        pig.registerQuery("b = LOAD '" + datadir + "aout' using PigStorage('\\t'); c = FOREACH b GENERATE f2;");

        Iterator<Tuple> it = pig.openIterator("c");
        Assert.assertTrue("results were produced", it.hasNext());

        Tuple t = it.next();

        Assert.assertTrue("data is correct type", t.get(0) instanceof Integer);
    }

    @Test
    public void testSchemaConversion() throws Exception {

        Util.createLocalInputFile(datadir + "originput2",
                new String[] {"1", "2", "3", "2",
                              "5", "5", "8", "8",
                              "8", "9"});

        pig.registerQuery("A = LOAD '" + datadir + "originput2' using PigStorage('\\t') " +
        "as (f:int);");
        pig.registerQuery("B = group A by f;");
        Schema origSchema = pig.dumpSchema("B");
        ResourceSchema rs1 = new ResourceSchema(origSchema);
        pig.registerQuery("STORE B into '" + datadir + "bout' using PigStorage('\\t', '-schema');");

        pig.registerQuery("C = LOAD '" + datadir + "bout' using PigStorage('\\t', '-schema');");
        Schema genSchema = pig.dumpSchema("C");
        ResourceSchema rs2 = new ResourceSchema(genSchema);
        Assert.assertTrue("generated schema equals original" , ResourceSchema.equals(rs1, rs2));

        pig.registerQuery("C1 = LOAD '" + datadir + "bout' as (a0:int, A: {t: (f:int) } );");
        pig.registerQuery("D = foreach C1 generate a0, SUM(A);");

        List<Tuple> expectedResults = Util.getTuplesFromConstantTupleStrings(
                new String[] {
                        "(1,1L)",
                        "(2,4L)",
                        "(3,3L)",
                        "(5,10L)",
                        "(8,24L)",
                        "(9,9L)"
                });

        Iterator<Tuple> iter = pig.openIterator("D");
        int counter = 0;
        while (iter.hasNext()) {
            Assert.assertEquals(expectedResults.get(counter++).toString(), iter.next().toString());
        }

        Assert.assertEquals(expectedResults.size(), counter);
    }

    @Test
    public void testSchemaConversion2() throws Exception {

        pig.registerQuery("A = LOAD '" + datadir + "originput' using PigStorage(',') " +
        "as (f1:chararray, f2:int);");
        pig.registerQuery("B = group A by f1;");
        Schema origSchema = pig.dumpSchema("B");
        ResourceSchema rs1 = new ResourceSchema(origSchema);
        pig.registerQuery("STORE B into '" + datadir + "cout' using PigStorage('\\t', '-schema');");

        pig.registerQuery("C = LOAD '" + datadir + "cout' using PigStorage('\\t', '-schema');");
        Schema genSchema = pig.dumpSchema("C");
        ResourceSchema rs2 = new ResourceSchema(genSchema);
        Assert.assertTrue("generated schema equals original" , ResourceSchema.equals(rs1, rs2));

        pig.registerQuery("C1 = LOAD '" + datadir + "cout' as (a0:chararray, A: {t: (f1:chararray, f2:int) } );");
        pig.registerQuery("D = foreach C1 generate a0, SUM(A.f2);");

        List<Tuple> expectedResults = Util.getTuplesFromConstantTupleStrings(
                new String[] {
                        "('A',23L)",
                        "('B',7L)",
                        "('C',11L)",
                        "('D',10L)"
                });

        Iterator<Tuple> iter = pig.openIterator("D");
        int counter = 0;
        while (iter.hasNext()) {
            Assert.assertEquals(expectedResults.get(counter++).toString(), iter.next().toString());
        }

        Assert.assertEquals(expectedResults.size(), counter);
    }

    @Test
    public void testSchemaDataNotMatchWITHCast() throws Exception {
        pig.registerQuery("A = LOAD '" + datadir + "originput' using PigStorage(',') as (x:chararray);");

        List<Tuple> expectedResults = Util.getTuplesFromConstantTupleStrings(
                new String[] {
                        "('A')",
                        "('B')",
                        "('C')",
                        "('D')",
                        "('A')",
                        "('B')",
                        "('C')",
                        "('A')",
                        "('D')",
                        "('A')",
                });

        assertAliasIs("A", expectedResults);
    }

    @Test
    public void testSchemaDataNotMatchNOCast() throws Exception {
        pig.registerQuery("A = LOAD '" + datadir + "originput' using PigStorage(',') as (x:bytearray);");

        List<Tuple> expectedResults = Util.getTuplesFromConstantTupleStrings(
                new String[] {
                        "('A')",
                        "('B')",
                        "('C')",
                        "('D')",
                        "('A')",
                        "('B')",
                        "('C')",
                        "('A')",
                        "('D')",
                        "('A')",
                });

        assertAliasIs("A", expectedResults);
    }

    @Test
    public void testSchemaDataNotMatchAsEXTRACoumns() throws Exception {
        pig.registerQuery("A = LOAD '" + datadir + "originput' using PigStorage(',') as (x,y,z);");

        List<Tuple> expectedResults = Util.getTuplesFromConstantTupleStrings(
                new String[] {
                        "('A',1,NULL)",
                        "('B',2,NULL)",
                        "('C',3,NULL)",
                        "('D',2,NULL)",
                        "('A',5,NULL)",
                        "('B',5,NULL)",
                        "('C',8,NULL)",
                        "('A',8,NULL)",
                        "('D',8,NULL)",
                        "('A',9,NULL)",
                });

        assertAliasIs("A", expectedResults);
    }

    /**
     * See PIG-1830
     * @throws IOException
     */
    @Test
    public void testByteArrayConversion() throws IOException {
        Util.createLocalInputFile(datadir + "originput2",
                new String[] {"peter\t1", "samir\t2", "michael\t4",
                "peter\t2", "peter\t4", "samir\t1", "john\t"
        });
        Util.createLocalInputFile(datadir + ".pig_schema",
                new String[] {
                "{\"fields\":[{\"name\":\"name\",\"type\":55,\"schema\":null," +
                "\"description\":\"autogenerated from Pig Field Schema\"}," +
                "{\"name\":\"val\",\"type\":10,\"schema\":null,\"description\":"+
                "\"autogenerated from Pig Field Schema\"}],\"version\":0," +
                "\"sortKeys\":[],\"sortKeyOrders\":[]}"
        });
        pig.registerQuery("Events = LOAD '" + datadir + "originput2' USING PigStorage('\\t', '-schema');");
        pig.registerQuery("Sessions = GROUP Events BY name;");
        Iterator<Tuple> sessions = pig.openIterator("Sessions");
        while (sessions.hasNext()) {
            System.out.println(sessions.next());
        }
    }

    // See PIG-1993
    @Test
    public void testColumnPrune() throws IOException {
        Util.createLocalInputFile(datadir + "originput2",
                new String[] {"peter\t1", "samir\t2", "michael\t4",
                "peter\t2", "peter\t4", "samir\t1", "john\t"
        });
        Util.createLocalInputFile(datadir + ".pig_schema",
                new String[] {
                "{\"fields\":[{\"name\":\"name\",\"type\":55,\"schema\":null," +
                "\"description\":\"autogenerated from Pig Field Schema\"}," +
                "{\"name\":\"val\",\"type\":10,\"schema\":null,\"description\":"+
                "\"autogenerated from Pig Field Schema\"}],\"version\":0," +
                "\"sortKeys\":[],\"sortKeyOrders\":[]}"
        });
        pig.registerQuery("Events = LOAD '" + datadir + "originput2' USING PigStorage('\\t', '-schema');");
        pig.registerQuery("EventsName = foreach Events generate name;");
        Iterator<Tuple> sessions = pig.openIterator("EventsName");
        sessions.next().toString().equals("(1)");
        sessions.next().toString().equals("(2)");
        sessions.next().toString().equals("(4)");
        sessions.next().toString().equals("(2)");
        sessions.next().toString().equals("(4)");
        sessions.next().toString().equals("(1)");
        sessions.next().toString().equals("()");
        Assert.assertFalse(sessions.hasNext());
    }

    @Test
    public void testPigStorageSchemaHeader() throws Exception {
        pigContext.connect();
        String query = "a = LOAD '" + datadir + "originput' using PigStorage(',') " +
                "as (foo:chararray, bar:int);";
        pig.registerQuery(query);
        pig.registerQuery("a2 = FOREACH a GENERATE *, 1;"); // adds a field with a null schema name
        pig.registerQuery("STORE a2 into '" + datadir + "nout' using PigStorage('\\t', '-schema');");

        String outPath = FileLocalizer.fullPath(datadir + "nout/.pig_header",
                pig.getPigContext());
        Assert.assertTrue(FileLocalizer.fileExists(outPath,
                pig.getPigContext()));

        String[] header = Util.readOutput(pig.getPigContext(), outPath);
        Assert.assertArrayEquals("Headers are not the same.", new String[] {"foo\tbar\t$2"}, header);
    }

    @Test
    public void testPigStorageSchemaHeaderDelimiter() throws Exception {
        pigContext.connect();
        String query = "a = LOAD '" + datadir + "originput' using PigStorage(',') " +
                "as (foo:chararray, bar:int);";
        pig.registerQuery(query);
        pig.registerQuery("STORE a into '" + datadir + "dout' using PigStorage('#', '-schema');");
        pig.registerQuery("STORE a into '" + datadir + "eout' using PigStorage('\\t', '-schema');");

        String outPath = FileLocalizer.fullPath(datadir + "dout/.pig_header",
                pig.getPigContext());
        Assert.assertTrue(FileLocalizer.fileExists(outPath,
                pig.getPigContext()));

        String[] header = Util.readOutput(pig.getPigContext(), outPath);
        Assert.assertArrayEquals("Headers are not the same.", new String[] {"foo#bar"}, header);

        outPath = FileLocalizer.fullPath(datadir + "eout/.pig_header",
                pig.getPigContext());
        Assert.assertTrue(FileLocalizer.fileExists(outPath,
                pig.getPigContext()));

        header = Util.readOutput(pig.getPigContext(), outPath);
        Assert.assertArrayEquals("Headers are not the same.", new String[] {"foo\tbar"}, header);
    }

    private void putInputFile(String filename) throws IOException {
        Util.createLocalInputFile(filename, new String[] {});
    }

    private void putSchemaFile(String schemaFilename, ResourceSchema testSchema) throws JsonGenerationException, JsonMappingException, IOException {
        new ObjectMapper().writeValue(new File(schemaFilename), testSchema);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testPigStorageSchemaSearch() throws Exception {
        String globtestdir = "build/test/tmpglobbingdata/";
        ResourceSchema testSchema = new ResourceSchema(Utils.parseSchema("a0:chararray"));
        PigStorage pigStorage = new PigStorage();
        pigContext.connect();
        try{
            Util.deleteDirectory(new File(datadir));
            pig.mkdirs(globtestdir+"a");
            pig.mkdirs(globtestdir+"a/a0");
            putInputFile(globtestdir+"a/a0/input");
            pig.mkdirs(globtestdir+"a/b0");
            putInputFile(globtestdir+"a/b0/input");
            pig.mkdirs(globtestdir+"b");
        } catch (IOException e) {};

        Configuration conf = ConfigurationUtil.toConfiguration(pigContext.getProperties());
        // if schema file is not found, schema is null
        ResourceSchema schema = pigStorage.getSchema(globtestdir, new Job(conf));
        Assert.assertTrue(schema==null);

        // if .pig_schema is in the input directory
        putSchemaFile(globtestdir+"a/a0/.pig_schema", testSchema);
        schema = pigStorage.getSchema(globtestdir+"a/a0", new Job(conf));
        Assert.assertTrue(ResourceSchema.equals(schema, testSchema));
        new File(globtestdir+"a/a0/.pig_schema").delete();

        // .pig_schema in one of globStatus returned directory
        putSchemaFile(globtestdir+"a/.pig_schema", testSchema);
        schema = pigStorage.getSchema(globtestdir+"*", new Job(conf));
        Assert.assertTrue(ResourceSchema.equals(schema, testSchema));
        new File(globtestdir+"a/.pig_schema").delete();

        putSchemaFile(globtestdir+"b/.pig_schema", testSchema);
        schema = pigStorage.getSchema(globtestdir+"*", new Job(conf));
        Assert.assertTrue(ResourceSchema.equals(schema, testSchema));
        new File(globtestdir+"b/.pig_schema").delete();

        // if .pig_schema is deep in the globbing, it will not get used
        putSchemaFile(globtestdir+"a/a0/.pig_schema", testSchema);
        schema = pigStorage.getSchema(globtestdir+"*", new Job(conf));
        Assert.assertTrue(schema==null);
        putSchemaFile(globtestdir+"a/.pig_schema", testSchema);
        schema = pigStorage.getSchema(globtestdir+"*", new Job(conf));
        Assert.assertTrue(ResourceSchema.equals(schema, testSchema));
        new File(globtestdir+"a/a0/.pig_schema").delete();
        new File(globtestdir+"a/.pig_schema").delete();

        pigStorage = new PigStorage("\t", "-schema");
        putSchemaFile(globtestdir+"a/.pig_schema", testSchema);
        schema = pigStorage.getSchema(globtestdir+"{a,b}", new Job(conf));
        Assert.assertTrue(ResourceSchema.equals(schema, testSchema));
    }

    /**
     * This is for testing source tagging option on PigStorage. When a user
     * specifies '-tagFile' as an option, PigStorage must prepend the input
     * source path to the tuple and "INPUT_FILE_NAME" to schema.
     *
     * @throws Exception
     */
    @Test
    public void testPigStorageSourceTagSchema() throws Exception {
        pigContext.connect();
        String query = "a = LOAD '" + datadir + "originput' using PigStorage('\\t') " +
        "as (f1:chararray, f2:int);";
        pig.registerQuery(query);
        pig.store("a", datadir + "aout", "PigStorage('\\t', '-schema')");
        // aout now has a schema.

        // Verify that loading a-out with '-tagFile' produces
        // the original schema, and prepends 'INPUT_FILE_NAME' to
        // original schema.
        pig.registerQuery("b = LOAD '" + datadir + "aout' using PigStorage('\\t', '-tagFile');");
        Schema genSchema = pig.dumpSchema("b");
        String[] fileAliases = {"INPUT_FILE_NAME", "f1", "f2"};
        byte[] fileTypes = {DataType.CHARARRAY, DataType.CHARARRAY, DataType.INTEGER};
        Schema newSchema = TypeCheckingTestUtil.genFlatSchema(
                fileAliases,fileTypes);
        Assert.assertTrue("schema with -tagFile preprends INPUT_FILE_NAME",
                Schema.equals(newSchema, genSchema, true, false));

        // Verify that loading a-out with '-tagPath' produces
        // the original schema, and prepends 'INPUT_FILE_PATH' to
        // original schema.
        pig.registerQuery("b = LOAD '" + datadir + "aout' using PigStorage('\\t', '-tagPath');");
        genSchema = pig.dumpSchema("b");
        String[] pathAliases = {"INPUT_FILE_PATH", "f1", "f2"};
        byte[] pathTypes = {DataType.CHARARRAY, DataType.CHARARRAY, DataType.INTEGER};
        newSchema = TypeCheckingTestUtil.genFlatSchema(pathAliases,pathTypes);
        Assert.assertTrue("schema with -tagPath preprends INPUT_FILE_PATH",
                Schema.equals(newSchema, genSchema, true, false));


        // Verify that explicitly requesting no schema works
        pig.registerQuery("d = LOAD '" + datadir + "aout' using PigStorage('\\t', '-noschema');");
        genSchema = pig.dumpSchema("d");
        assertNull(genSchema);

        // Verify specifying your own schema works
        pig.registerQuery("b = LOAD '" + datadir + "aout' using PigStorage('\\t', '-tagFile') " +
        "as (input_file:chararray, foo:chararray, bar:int);");
        genSchema = pig.dumpSchema("b");
        String[] newAliases = {"input_file", "foo", "bar"};
        byte[] newTypes = {DataType.CHARARRAY, DataType.CHARARRAY, DataType.INTEGER};
        newSchema = TypeCheckingTestUtil.genFlatSchema(newAliases,newTypes);
        Assert.assertTrue("explicit schema overrides metadata",
                Schema.equals(newSchema, genSchema, true, false));
    }

    @Test
    public void testPigStorageSourceTagValue() throws Exception {
        final String storeFileName = "part-m-00000";
        pigContext.connect();

        String query = "a = LOAD '" + datadir + "' using PigStorage('\\t') " +
        "as (f1:chararray, f2:int);";
        pig.registerQuery(query);
        // Storing in 'aout' directory will store contents in part-m-00000
        pig.store("a", datadir + "aout", "PigStorage('\\t', '-schema')");

        // Verify input source tag is present when using -tagFile or -tagPath
        pig.registerQuery("b = LOAD '" + datadir + "aout' using PigStorage('\\t', '-tagFile');");
        pig.registerQuery("c = foreach b generate INPUT_FILE_NAME;");
        Iterator<Tuple> iter = pig.openIterator("c");
        while(iter.hasNext()) {
            Tuple tuple = iter.next();
            String inputFileName = (String)tuple.get(0);
            assertEquals("tagFile value must be part-m-00000", inputFileName, storeFileName);
        }
    }

    @Test
    public void testIncompleteDataWithPigSchema() throws Exception {
        File parent = new File(datadir, "incomplete_data_with_pig_schema_1");
        parent.deleteOnExit();
        parent.mkdirs();
        File tmpInput = File.createTempFile("tmp", "tmp");
        tmpInput.deleteOnExit();
        File outFile = new File(parent, "out");
        pig.registerQuery("a = load '"+Util.encodeEscape(tmpInput.getAbsolutePath())+"' as (x:int, y:chararray, z:chararray);");
        pig.store("a", outFile.getAbsolutePath(), "PigStorage('\\t', '-schema')");
        File schemaFile = new File(outFile, ".pig_schema");

        parent = new File(datadir, "incomplete_data_with_pig_schema_2");
        parent.deleteOnExit();
        File inputDir = new File(parent, "input");
        inputDir.mkdirs();
        File inputSchemaFile = new File(inputDir, ".pig_schema");
        FileUtils.moveFile(schemaFile, inputSchemaFile);
        File inputFile = new File(inputDir, "data");
        Util.writeToFile(inputFile, new String[]{"1"});
        pig.registerQuery("a = load '"+Util.encodeEscape(inputDir.getAbsolutePath())+"';");
        Iterator<Tuple> it = pig.openIterator("a");
        assertTrue(it.hasNext());
        assertEquals(tuple(1,null,null), it.next());
        assertFalse(it.hasNext());

        // Now, test with prune
        pig.registerQuery("a = load '"+Util.encodeEscape(inputDir.getAbsolutePath())+"'; b = foreach a generate y, z;");
        it = pig.openIterator("b");
        assertTrue(it.hasNext());
        assertEquals(tuple(null,null), it.next());
        assertFalse(it.hasNext());
    }


    @Test
    public void testPigStorageSchemaWithOverwrite() throws Exception {
        pigContext.connect();
        String query = "a = LOAD '" + datadir
                + "originput' using PigStorage(',') "
                + "as (f1:chararray, f2:int);";

        List<Tuple> expectedResults = Util
                .getTuplesFromConstantTupleStrings(new String[] { "('A',1L)",
                        "('B',2L)", "('C',3L)", "('D',2L)", "('A',5L)",
                        "('B',5L)", "('C',8L)", "('A',8L)", "('D',8L)",
                        "('A',9L)", });

        pig.registerQuery(query);
        pig.store("a", datadir + "aout", "PigStorage(',')");
        // below shouldn't fail & we should get the same result in the end
        pig.store("a", datadir + "aout", "PigStorage(',', '--overwrite true')");
        pig.registerQuery("b = LOAD '" + datadir + "aout' using PigStorage(',');");
        Iterator<Tuple> iter = pig.openIterator("b");
        int counter = 0;
        while (iter.hasNext()) {
            String tuple = iter.next().toString();
            Assert.assertEquals(expectedResults.get(counter++).toString(),
                    tuple);
        }
        Assert.assertEquals(expectedResults.size(), counter);

    }

    @Test(expected = Exception.class)
    public void testPigStorageSchemaFailureWithoutOverwrite() throws Exception {
        pigContext.connect();
        String query = "a = LOAD '" + datadir + "originput' using PigStorage(',') "
                + "as (f1:chararray, f2:int);";
        pig.registerQuery(query);
        // should fail without the overwrite flag
        pig.store("a", datadir + "aout", "PigStorage(',')");
        pig.store("a", datadir + "aout", "PigStorage(',')");
    }

}
