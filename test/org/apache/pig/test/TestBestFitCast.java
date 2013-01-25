/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.ExecType;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigException;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.LogUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class TestBestFitCast {
    private PigServer pigServer;
    private static MiniCluster cluster = MiniCluster.buildCluster();
    String inputFile, inputFile2;
    int LOOP_SIZE = 20;

    @Before
    public void setUp() throws Exception {
        pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        inputFile = "TestBestFitCast-input.txt";
        String[] input = new String[LOOP_SIZE];
        long l = 0;
        for (int i = 1; i <= LOOP_SIZE; i++) {
            input[i - 1] = (l + "\t" + i);
        }
        Util.createInputFile(cluster, inputFile, input);

        inputFile2 = "TestBestFitCast-input2.txt";
        l = 0;
        for (int i = 1; i <= LOOP_SIZE; i++) {
            input[i - 1] = (l + "\t" + i + "\t" + i);
        }
        Util.createInputFile(cluster, inputFile2, input);
    }

    @After
    public void tearDown() throws Exception {
        Util.deleteFile(cluster, inputFile);
        Util.deleteFile(cluster, inputFile2);
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cluster.shutDown();
    }

    public static class UDF1 extends EvalFunc<Tuple> {
        /**
         * java level API
         *
         * @param input expects a single numeric DataAtom value
         * @param output returns a single numeric DataAtom value, cosine value of the argument
         */
        @Override
        public Tuple exec(Tuple input) throws IOException {
            return input;
        }

        /*
         * (non-Javadoc)
         * @see org.apache.pig.EvalFunc#getArgToFuncMapping()
         */
        @Override //TODO add BigInteger and BigDecimal
        public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
            List<FuncSpec> funcList = new ArrayList<FuncSpec>();
            funcList.add(new FuncSpec(this.getClass().getName(), new Schema(Arrays.asList(
                    new Schema.FieldSchema(null, DataType.FLOAT), new Schema.FieldSchema(null,
                            DataType.FLOAT)))));
            funcList.add(new FuncSpec(this.getClass().getName(), new Schema(Arrays.asList(
                    new Schema.FieldSchema(null, DataType.LONG), new Schema.FieldSchema(null,
                            DataType.DOUBLE)))));
            funcList.add(new FuncSpec(this.getClass().getName(), new Schema(new Schema.FieldSchema(
                    null, DataType.FLOAT))));
            funcList.add(new FuncSpec(this.getClass().getName(), new Schema(new Schema.FieldSchema(
                    null, DataType.INTEGER))));
            funcList.add(new FuncSpec(this.getClass().getName(), new Schema(new Schema.FieldSchema(
                    null, DataType.DOUBLE))));
            /*
             * funcList.add(new FuncSpec(DoubleMax.class.getName(),
             * Schema.generateNestedSchema(DataType.BAG, DataType.DOUBLE)));
             * funcList.add(new FuncSpec(FloatMax.class.getName(),
             * Schema.generateNestedSchema(DataType.BAG, DataType.FLOAT)));
             * funcList.add(new FuncSpec(IntMax.class.getName(),
             * Schema.generateNestedSchema(DataType.BAG, DataType.INTEGER)));
             * funcList.add(new FuncSpec(LongMax.class.getName(),
             * Schema.generateNestedSchema(DataType.BAG, DataType.LONG)));
             * funcList.add(new FuncSpec(StringMax.class.getName(),
             * Schema.generateNestedSchema(DataType.BAG, DataType.CHARARRAY)));
             */
            return funcList;
        }

    }

    public static class UDF2 extends EvalFunc<String> {
        /**
         * java level API
         *
         * @param input expects a single numeric DataAtom value
         * @param output returns a single numeric DataAtom value, cosine value of the argument
         */
        @Override
        public String exec(Tuple input) throws IOException {
            try {
                String str = (String)input.get(0);
                return str.toUpperCase();
            } catch (Exception e) {
                return null;
            }
        }

        /*
         * (non-Javadoc)
         * @see org.apache.pig.EvalFunc#getArgToFuncMapping()
         */
        @Override
        public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
            List<FuncSpec> funcList = new ArrayList<FuncSpec>();
            funcList.add(new FuncSpec(this.getClass().getName(), new Schema(new Schema.FieldSchema(
                    null, DataType.CHARARRAY))));
            return funcList;
        }

    }

    /**
     * For testing with input schemas which have byte arrays
     */
    public static class UDF3 extends EvalFunc<Tuple> {

        /**
         * a UDF which simply returns its input as output
         */
        @Override
        public Tuple exec(Tuple input) throws IOException {
            return input;
        }

        /*
         * (non-Javadoc)
         * @see org.apache.pig.EvalFunc#getArgToFuncMapping()
         */
        @Override
        public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
            List<FuncSpec> funcList = new ArrayList<FuncSpec>();

            // the following schema should match when the input is
            // just a {bytearray} - exact match
            funcList.add(new FuncSpec(this.getClass().getName(),
                    new Schema(new Schema.FieldSchema(null, DataType.BYTEARRAY))));
            // the following schema should match when the input is
            // just a {int} - exact match
            funcList.add(new FuncSpec(this.getClass().getName(),
                    new Schema(new Schema.FieldSchema(null, DataType.INTEGER))));

            // The following two schemas will cause conflict when input schema
            // is {float, bytearray} since bytearray can be casted either to long
            // or double. However when input schema is {bytearray, int}, it should work
            // since bytearray should get casted to float and int to long. Likewise if
            // input schema is {bytearray, long} or {bytearray, double} it should work
            funcList.add(new FuncSpec(this.getClass().getName(),
                    new Schema(Arrays.asList(new Schema.FieldSchema(null, DataType.FLOAT),
                            new Schema.FieldSchema(null, DataType.DOUBLE)))));
            funcList.add(new FuncSpec(this.getClass().getName(),
                    new Schema(Arrays.asList(new Schema.FieldSchema(null, DataType.FLOAT),
                            new Schema.FieldSchema(null, DataType.LONG)))));

            // The following two schemas will cause conflict when input schema is
            // {bytearray, int, int} since the two ints could be casted to long, double
            // or double, long. Likewise input schema of either {bytearray, long, long}
            // or {bytearray, double, double} would cause conflict. Input schema of
            // {bytearray, long, double} or {bytearray, double, long} should not cause
            // conflict since only the bytearray needs to be casted to float. Input schema
            // of {float, bytearray, long} or {float, long, bytearray} should also
            // work since only the bytearray needs to be casted. Input schema of
            // {float, bytearray, int} will cause conflict since we could cast int to
            // long or double and bytearray to long or double. Input schema of
            // {bytearray, long, int} should work and should match the first schema below for
            // matching wherein the bytearray is cast to float and the int to double.
            funcList.add(new FuncSpec(this.getClass().getName(),
                    new Schema(Arrays.asList(new Schema.FieldSchema(null, DataType.FLOAT),
                            new Schema.FieldSchema(null, DataType.DOUBLE),
                            new Schema.FieldSchema(null, DataType.LONG)))));
            funcList.add(new FuncSpec(this.getClass().getName(),
                    new Schema(Arrays.asList(new Schema.FieldSchema(null, DataType.FLOAT),
                            new Schema.FieldSchema(null, DataType.LONG),
                            new Schema.FieldSchema(null, DataType.DOUBLE)))));

            return funcList;
        }

    }

    @Test
    public void testByteArrayCast1() throws IOException {
        // Passing (float, bytearray)
        // Ambiguous matches: (float, long) , (float, double)
        boolean exceptionCaused = false;
        try {
            pigServer.registerQuery("A = LOAD '" + inputFile + "' as (x:float, y);");
            pigServer.registerQuery("B = FOREACH A generate x, " + UDF3.class.getName() + "(x,y);");
            Iterator<Tuple> iter = pigServer.openIterator("B");
        } catch (Exception e) {
            exceptionCaused = true;
            PigException pe = LogUtils.getPigException(e);
            String msg = (pe == null ? e.getMessage() : pe.getMessage());
            assertTrue(msg.contains("Multiple matching functions"));
            assertTrue(msg.contains("{float,double}, {float,long}"));
        }
        assertTrue(exceptionCaused);
    }

    @Test
    public void testByteArrayCast2() throws IOException, ExecException {
        // Passing (bytearray, int)
        // Possible matches: (float, long) , (float, double)
        // Chooses (float, long) since in both cases bytearray is cast to float and the
        // cost of casting int to long < int to double
        pigServer.registerQuery("A = LOAD '" + inputFile + "' as (x, y:int);");
        pigServer.registerQuery("B = FOREACH A generate x, " + UDF3.class.getName() + "(x,y);");
        Iterator<Tuple> iter = pigServer.openIterator("B");
        assertTrue("No Output received", iter.hasNext());
        int cnt = 0;
        while (iter.hasNext()) {
            Tuple t = iter.next();
            assertTrue(((Tuple)t.get(1)).get(0) instanceof Float);
            assertEquals((Float)((Tuple)t.get(1)).get(0), (Float)0.0f);
            assertTrue(((Tuple)t.get(1)).get(1) instanceof Long);
            assertEquals((Long)((Tuple)t.get(1)).get(1), new Long(cnt + 1));
            ++cnt;
        }
        assertEquals(LOOP_SIZE, cnt);
    }

    @Test
    public void testByteArrayCast3() throws IOException, ExecException {
        // Passing (bytearray, long)
        // Possible matches: (float, long) , (float, double)
        // Chooses (float, long) since that is the only exact match without bytearray
        pigServer.registerQuery("A = LOAD '" + inputFile + "' as (x, y:long);");
        pigServer.registerQuery("B = FOREACH A generate x, " + UDF3.class.getName() + "(x, y);");
        Iterator<Tuple> iter = pigServer.openIterator("B");
        assertTrue("No Output received", iter.hasNext());
        int cnt = 0;
        while (iter.hasNext()) {
            Tuple t = iter.next();
            assertTrue(((Tuple)t.get(1)).get(0) instanceof Float);
            assertEquals((Float)((Tuple)t.get(1)).get(0), (Float)0.0f);
            assertTrue(((Tuple)t.get(1)).get(1) instanceof Long);
            assertEquals((Long)((Tuple)t.get(1)).get(1), new Long(cnt + 1));
            ++cnt;
        }
        assertEquals(LOOP_SIZE, cnt);
    }

    @Test
    public void testByteArrayCast4() throws IOException, ExecException {
        // Passing (bytearray, double)
        // Possible matches: (float, long) , (float, double)
        // Chooses (float, double) since that is the only exact match without bytearray
        pigServer.registerQuery("A = LOAD '" + inputFile + "' as (x, y:double);");
        pigServer.registerQuery("B = FOREACH A generate x, " + UDF3.class.getName() + "(x,y);");
        Iterator<Tuple> iter = pigServer.openIterator("B");
        assertTrue("No Output received", iter.hasNext());
        int cnt = 0;
        while (iter.hasNext()) {
            Tuple t = iter.next();
            assertTrue(((Tuple)t.get(1)).get(0) instanceof Float);
            assertEquals((Float)((Tuple)t.get(1)).get(0), (Float)0.0f);
            assertTrue(((Tuple)t.get(1)).get(1) instanceof Double);
            assertEquals((Double)((Tuple)t.get(1)).get(1), new Double(cnt + 1));
            ++cnt;
        }
        assertEquals(LOOP_SIZE, cnt);
    }

    @Test
    public void testByteArrayCast5() throws IOException, ExecException {
        // Passing (bytearray, int, int )
        // Ambiguous matches: (float, long, double) , (float, double, long)
        // bytearray can be casted to float but the two ints cannot be unambiguously
        // casted
        boolean exceptionCaused = false;
        try {
            pigServer.registerQuery("A = LOAD '" + inputFile + "' as (x, y:int);");
            pigServer.registerQuery("B = FOREACH A generate x, " + UDF3.class.getName()
                    + "(x,y, y);");
            Iterator<Tuple> iter = pigServer.openIterator("B");
        } catch (Exception e) {
            exceptionCaused = true;
            PigException pe = LogUtils.getPigException(e);
            String msg = (pe == null ? e.getMessage() : pe.getMessage());
            assertTrue(msg.contains("Multiple matching functions"));
            assertTrue(msg.contains("({float,double,long}, {float,long,double})"));
        }
        assertTrue(exceptionCaused);
    }

    @Test
    public void testByteArrayCast6() throws IOException, ExecException {
        // Passing (bytearray, long, long )
        // Ambiguous matches: (float, long, double) , (float, double, long)
        // bytearray can be casted to float but the two longs cannot be
        // unambiguously casted
        boolean exceptionCaused = false;
        try {
            pigServer.registerQuery("A = LOAD '" + inputFile + "' as (x, y:long);");
            pigServer.registerQuery("B = FOREACH A generate x, " + UDF3.class.getName()
                    + "(x,y, y);");
            Iterator<Tuple> iter = pigServer.openIterator("B");
        } catch (Exception e) {
            exceptionCaused = true;
            PigException pe = LogUtils.getPigException(e);
            String msg = (pe == null ? e.getMessage() : pe.getMessage());
            assertTrue(msg.contains("Multiple matching functions"));
            assertTrue(msg.contains("({float,double,long}, {float,long,double})"));
        }
        assertTrue(exceptionCaused);
    }

    @Test
    public void testByteArrayCast7() throws IOException, ExecException {
        // Passing (bytearray, double, double )
        // Ambiguous matches: (float, long, double) , (float, double, long)
        // bytearray can be casted to float but the two doubles cannot be
        // casted with a permissible cast
        boolean exceptionCaused = false;
        try {
            pigServer.registerQuery("A = LOAD '" + inputFile + "' as (x, y:double);");
            pigServer.registerQuery("B = FOREACH A generate x, " + UDF3.class.getName()
                    + "(x,y, y);");
            Iterator<Tuple> iter = pigServer.openIterator("B");
        } catch (Exception e) {
            exceptionCaused = true;
            PigException pe = LogUtils.getPigException(e);
            String msg = (pe == null ? e.getMessage() : pe.getMessage());
            assertTrue(msg.contains("Could not infer the matching function"));
        }
        assertTrue(exceptionCaused);
    }

    @Test
    public void testByteArrayCast8() throws IOException, ExecException {
        // Passing (bytearray, long, double)
        // Possible matches: (float, long, double) , (float, double, long)
        // Chooses (float, long, double) since that is the only exact match without bytearray
        pigServer.registerQuery("A = LOAD '" + inputFile2 + "' as (x, y:long, z:double);");
        pigServer.registerQuery("B = FOREACH A generate x, " + UDF3.class.getName() + "(x,y,z);");
        Iterator<Tuple> iter = pigServer.openIterator("B");
        assertTrue("No Output received", iter.hasNext());
        int cnt = 0;
        while (iter.hasNext()) {
            Tuple t = iter.next();
            assertTrue(((Tuple)t.get(1)).get(0) instanceof Float);
            assertEquals((Float)((Tuple)t.get(1)).get(0), (Float)0.0f);
            assertTrue(((Tuple)t.get(1)).get(1) instanceof Long);
            assertEquals((Long)((Tuple)t.get(1)).get(1), new Long(cnt + 1));
            assertTrue(((Tuple)t.get(1)).get(2) instanceof Double);
            assertEquals((Double)((Tuple)t.get(1)).get(2), new Double(cnt + 1));
            ++cnt;
        }
        assertEquals(LOOP_SIZE, cnt);
    }

    @Test
    public void testByteArrayCast9() throws IOException, ExecException {
        // Passing (bytearray, double, long)
        // Possible matches: (float, long, double) , (float, double, long)
        // Chooses (float, double, long) since that is the only exact match without bytearray
        pigServer.registerQuery("A = LOAD '" + inputFile2 + "' as (x, y:double, z:long);");
        pigServer.registerQuery("B = FOREACH A generate x, " + UDF3.class.getName() + "(x,y,z);");
        Iterator<Tuple> iter = pigServer.openIterator("B");
        assertTrue("No Output received", iter.hasNext());
        int cnt = 0;
        while (iter.hasNext()) {
            Tuple t = iter.next();
            assertTrue(((Tuple)t.get(1)).get(0) instanceof Float);
            assertEquals((Float)((Tuple)t.get(1)).get(0), (Float)0.0f);
            assertTrue(((Tuple)t.get(1)).get(1) instanceof Double);
            assertEquals((Double)((Tuple)t.get(1)).get(1), new Double(cnt + 1));
            assertTrue(((Tuple)t.get(1)).get(2) instanceof Long);
            assertEquals((Long)((Tuple)t.get(1)).get(2), new Long(cnt + 1));
            ++cnt;
        }
        assertEquals(LOOP_SIZE, cnt);
    }

    @Test
    public void testByteArrayCast10() throws IOException, ExecException {
        // Passing (float, long, bytearray)
        // Possible matches: (float, long, double) , (float, double, long)
        // Chooses (float, long, double) since that is the only exact match without bytearray
        pigServer.registerQuery("A = LOAD '" + inputFile2 + "' as (x:float, y:long, z);");
        pigServer.registerQuery("B = FOREACH A generate x, " + UDF3.class.getName() + "(x,y,z);");
        Iterator<Tuple> iter = pigServer.openIterator("B");
        assertTrue("No Output received", iter.hasNext());
        int cnt = 0;
        while (iter.hasNext()) {
            Tuple t = iter.next();
            assertTrue(((Tuple)t.get(1)).get(0) instanceof Float);
            assertEquals((Float)((Tuple)t.get(1)).get(0), (Float)0.0f);
            assertTrue(((Tuple)t.get(1)).get(1) instanceof Long);
            assertEquals((Long)((Tuple)t.get(1)).get(1), new Long(cnt + 1));
            assertTrue(((Tuple)t.get(1)).get(2) instanceof Double);
            assertEquals((Double)((Tuple)t.get(1)).get(2), new Double(cnt + 1));
            ++cnt;
        }
        assertEquals(LOOP_SIZE, cnt);
    }

    @Test
    public void testByteArrayCast11() throws IOException, ExecException {
        // Passing (float, bytearray, long)
        // Possible matches: (float, long, double) , (float, double, long)
        // Chooses (float, double, long) since that is the only exact match without bytearray
        pigServer.registerQuery("A = LOAD '" + inputFile2 + "' as (x:float, y, z:long);");
        pigServer.registerQuery("B = FOREACH A generate x, " + UDF3.class.getName() + "(x,y,z);");
        Iterator<Tuple> iter = pigServer.openIterator("B");
        assertTrue("No Output received", iter.hasNext());
        int cnt = 0;
        while (iter.hasNext()) {
            Tuple t = iter.next();
            assertTrue(((Tuple)t.get(1)).get(0) instanceof Float);
            assertEquals((Float)((Tuple)t.get(1)).get(0), (Float)0.0f);
            assertTrue(((Tuple)t.get(1)).get(1) instanceof Double);
            assertEquals((Double)((Tuple)t.get(1)).get(1), new Double(cnt + 1));
            assertTrue(((Tuple)t.get(1)).get(2) instanceof Long);
            assertEquals((Long)((Tuple)t.get(1)).get(2), new Long(cnt + 1));
            ++cnt;
        }
        assertEquals(LOOP_SIZE, cnt);
    }

    @Test
    public void testByteArrayCast12() throws IOException, ExecException {
        // Passing (float, bytearray, int )
        // Ambiguous matches: (float, long, double) , (float, double, long)
        // will cause conflict since we could cast int to
        // long or double and bytearray to long or double.
        boolean exceptionCaused = false;
        try {
            pigServer.registerQuery("A = LOAD '" + inputFile2 + "' as (x:float, y, z:int);");
            pigServer.registerQuery("B = FOREACH A generate x, " + UDF3.class.getName()
                    + "(x,y, y);");
            Iterator<Tuple> iter = pigServer.openIterator("B");
        } catch (Exception e) {
            exceptionCaused = true;
            PigException pe = LogUtils.getPigException(e);
            String msg = (pe == null ? e.getMessage() : pe.getMessage());
            assertTrue(msg.contains("Multiple matching functions"));
            assertTrue(msg.contains("({float,double,long}, {float,long,double}"));
        }
        assertTrue(exceptionCaused);
    }

    @Test
    public void testByteArrayCast13() throws IOException, ExecException {
        // Passing (bytearray, long, int)
        // Possible matches: (float, long, double) , (float, double, long)
        // Chooses (float, long, double) since for the bytearray there is a
        // single unambiguous cast to float. For the other two args, it is
        // less "costlier" to cast the last int to double than cast the long
        // to double and int to long
        pigServer.registerQuery("A = LOAD '" + inputFile2 + "' as (x, y:long, z:int);");
        pigServer.registerQuery("B = FOREACH A generate x, " + UDF3.class.getName() + "(x,y,z);");
        Iterator<Tuple> iter = pigServer.openIterator("B");
        assertTrue("No Output received", iter.hasNext());
        int cnt = 0;
        while (iter.hasNext()) {
            Tuple t = iter.next();
            assertTrue(((Tuple)t.get(1)).get(0) instanceof Float);
            assertEquals((Float)((Tuple)t.get(1)).get(0), (Float)0.0f);
            assertTrue(((Tuple)t.get(1)).get(1) instanceof Long);
            assertEquals((Long)((Tuple)t.get(1)).get(1), new Long(cnt + 1));
            assertTrue(((Tuple)t.get(1)).get(2) instanceof Double);
            assertEquals((Double)((Tuple)t.get(1)).get(2), new Double(cnt + 1));
            ++cnt;
        }
        assertEquals(LOOP_SIZE, cnt);
    }

    @Test
    public void testByteArrayCast14() throws IOException, ExecException {
        // Passing (bag{(bytearray)})
        // Possible matches: bag{(bytearray)}, bag{(int)}, bag{(long)}, bag{(float)}, bag{(double)}
        // Chooses bag{(bytearray)} because it is an exact match
        pigServer.registerQuery("A = LOAD '" + inputFile + "' as (x, y);");
        pigServer.registerQuery("B = group A all;");
        pigServer.registerQuery("C = FOREACH B generate SUM(A.y);");
        Iterator<Tuple> iter = pigServer.openIterator("C");
        Tuple t = iter.next();
        assertTrue(t.get(0) instanceof Double);
        assertEquals(new Double(210), (Double)t.get(0));
    }

    @Test
    public void testByteArrayCast15() throws IOException, ExecException {
        // Passing (bytearray)
        // Possible matches: (bytearray), (int)
        // Chooses (bytearray) because that is an exact match
        pigServer.registerQuery("A = LOAD '" + inputFile + "' as (x, y);");
        pigServer.registerQuery("B = FOREACH A generate " + UDF3.class.getName() + "(y);");
        Iterator<Tuple> iter = pigServer.openIterator("B");
        assertTrue("No Output received", iter.hasNext());
        int cnt = 0;
        while (iter.hasNext()) {
            Tuple t = iter.next();
            assertTrue(((Tuple)t.get(0)).get(0) instanceof DataByteArray);
            byte[] expected = Integer.toString(cnt + 1).getBytes();
            byte[] actual = ((DataByteArray)((Tuple)t.get(0)).get(0)).get();
            assertEquals(expected.length, actual.length);
            for (int i = 0; i < expected.length; i++) {
                assertEquals(expected[i], actual[i]);
            }
            ++cnt;
        }
        assertEquals(LOOP_SIZE, cnt);
    }

    @Test
    public void testByteArrayCast16() throws IOException, ExecException {
        // Passing (int)
        // Possible matches: (bytearray), (int)
        // Chooses (int) because that is an exact match
        pigServer.registerQuery("A = LOAD '" + inputFile + "' as (x, y:int);");
        pigServer.registerQuery("B = FOREACH A generate " + UDF3.class.getName() + "(y);");
        Iterator<Tuple> iter = pigServer.openIterator("B");
        assertTrue("No Output received", iter.hasNext());
        int cnt = 0;
        while (iter.hasNext()) {
            Tuple t = iter.next();
            assertTrue(((Tuple)t.get(0)).get(0) instanceof Integer);
            assertEquals(new Integer(cnt + 1), (Integer)((Tuple)t.get(0)).get(0));
            ++cnt;
        }
        assertEquals(LOOP_SIZE, cnt);
    }

    @Test
    public void testIntSum() throws IOException, ExecException {
        // Passing (bag{(int)})
        // Possible matches: bag{(bytearray)}, bag{(int)}, bag{(long)}, bag{(float)}, bag{(double)}
        // Chooses bag{(int)} since it is an exact match
        pigServer.registerQuery("A = LOAD '" + inputFile + "' as (x, y:int);");
        pigServer.registerQuery("B = group A all;");
        pigServer.registerQuery("C = FOREACH B generate SUM(A.y);");
        Iterator<Tuple> iter = pigServer.openIterator("C");
        Tuple t = iter.next();
        assertTrue(t.get(0) instanceof Long);
        assertEquals(new Long(210), (Long)t.get(0));
    }

    @Test
    public void testLongSum() throws IOException, ExecException {
        // Passing (bag{(long)})
        // Possible matches: bag{(bytearray)}, bag{(int)}, bag{(long)}, bag{(float)}, bag{(double)}
        // Chooses bag{(long)} since it is an exact match
        pigServer.registerQuery("A = LOAD '" + inputFile + "' as (x, y:long);");
        pigServer.registerQuery("B = group A all;");
        pigServer.registerQuery("C = FOREACH B generate SUM(A.y);");
        Iterator<Tuple> iter = pigServer.openIterator("C");
        Tuple t = iter.next();
        assertTrue(t.get(0) instanceof Long);
        assertEquals(new Long(210), (Long)t.get(0));
    }

    @Test
    public void testFloatSum() throws IOException, ExecException {
        // Passing (bag{(float)})
        // Possible matches: bag{(bytearray)}, bag{(int)}, bag{(long)}, bag{(float)}, bag{(double)}
        // Chooses bag{(float)} since it is an exact match
        pigServer.registerQuery("A = LOAD '" + inputFile + "' as (x, y:float);");
        pigServer.registerQuery("B = group A all;");
        pigServer.registerQuery("C = FOREACH B generate SUM(A.y);");
        Iterator<Tuple> iter = pigServer.openIterator("C");
        Tuple t = iter.next();
        assertTrue(t.get(0) instanceof Double);
        assertEquals(new Double(210), (Double)t.get(0));
    }

    @Test
    public void testDoubleSum() throws IOException, ExecException {
        // Passing (bag{(double)})
        // Possible matches: bag{(bytearray)}, bag{(int)}, bag{(long)}, bag{(float)}, bag{(double)}
        // Chooses bag{(double)} since it is an exact match
        pigServer.registerQuery("A = LOAD '" + inputFile + "' as (x, y:double);");
        pigServer.registerQuery("B = group A all;");
        pigServer.registerQuery("C = FOREACH B generate SUM(A.y);");
        Iterator<Tuple> iter = pigServer.openIterator("C");
        Tuple t = iter.next();
        assertTrue(t.get(0) instanceof Double);
        assertEquals(new Double(210), (Double)t.get(0));
    }

    @Test
    public void test1() throws Exception {
        // Passing (long, int)
        // Possible matches: (float, float) , (long, double)
        // Chooses (long, double) as it has only one cast compared to two for (float, float)
        pigServer.registerQuery("A = LOAD '" + inputFile + "' as (x:long, y:int);");
        pigServer.registerQuery("B = FOREACH A generate x, " + UDF1.class.getName() + "(x,y);");
        Iterator<Tuple> iter = pigServer.openIterator("B");
        assertTrue("No Output received", iter.hasNext());
        int cnt = 0;
        while (iter.hasNext()) {
            Tuple t = iter.next();
            assertEquals(true, ((Tuple)t.get(1)).get(0) instanceof Long);
            assertEquals(true, ((Tuple)t.get(1)).get(1) instanceof Double);
            ++cnt;
        }
        assertEquals(20, cnt);
    }

    @Test
    public void test2() throws Exception {
        // Passing (int, int)
        // Possible matches: (float, float) , (long, double)
        // Throws Exception as ambiguous definitions found
        try {
            pigServer.registerQuery("A = LOAD '" + inputFile + "' as (x:long, y:int);");
            pigServer.registerQuery("B = FOREACH A generate x, " + UDF1.class.getName() + "(y,y);");
            pigServer.openIterator("B");
        } catch (Exception e) {
            PigException pe = LogUtils.getPigException(e);
            String msg = (pe == null ? e.getMessage() : pe.getMessage());
            assertEquals(true, msg.contains("as multiple or none of them fit"));
        }

    }

    @Test
    public void test3() throws Exception {
        // Passing (int, int)
        // Possible matches: (float, float) , (long, double)
        // Chooses (float, float) as both options lead to same score and (float, float) occurs
        // first.
        pigServer.registerQuery("A = LOAD '" + inputFile + "' as (x:long, y:int);");
        pigServer.registerQuery("B = FOREACH A generate x, " + UDF1.class.getName()
                + "((float)y,(float)y);");
        Iterator<Tuple> iter = pigServer.openIterator("B");
        assertTrue("No Output received", iter.hasNext());
        int cnt = 0;
        while (iter.hasNext()) {
            Tuple t = iter.next();
            assertEquals(true, ((Tuple)t.get(1)).get(0) instanceof Float);
            assertEquals(true, ((Tuple)t.get(1)).get(1) instanceof Float);
            ++cnt;
        }
        assertEquals(20, cnt);
    }

    @Test
    public void test4() throws Exception {
        // Passing (long)
        // Possible matches: (float), (integer), (double)
        // Chooses (float) as it leads to a better score that to (double)
        pigServer.registerQuery("A = LOAD '" + inputFile + "' as (x:long, y:int);");
        pigServer.registerQuery("B = FOREACH A generate x, " + UDF1.class.getName() + "(x);");
        Iterator<Tuple> iter = pigServer.openIterator("B");
        assertTrue("No Output received", iter.hasNext());
        int cnt = 0;
        while (iter.hasNext()) {
            Tuple t = iter.next();
            assertEquals(true, ((Tuple)t.get(1)).get(0) instanceof Float);
            ++cnt;
        }
        assertEquals(20, cnt);
    }

    @Test
    public void test5() throws Exception {
        // Passing bytearrays
        // Possible matches: (float, float) , (long, double)
        // Throws exception since more than one funcSpec and inp is bytearray
        try {
            pigServer.registerQuery("A = LOAD '" + inputFile + "';");
            pigServer.registerQuery("B = FOREACH A generate $0, " + UDF1.class.getName()
                    + "($1,$1);");
            pigServer.openIterator("B");
        } catch (Exception e) {
            PigException pe = LogUtils.getPigException(e);
            String msg = (pe == null ? e.getMessage() : pe.getMessage());
            assertEquals(true, msg.contains("Multiple matching functions"));
        }

    }

    @Test
    public void test6() throws Exception {
        // test UDF with single mapping function
        // where bytearray is passed in as input parameter
        Util.createInputFile(cluster, "test6", new String[] { "abc" });
        pigServer.registerQuery("A = LOAD 'test6';");
        pigServer.registerQuery("B = FOREACH A GENERATE " + UDF2.class.getName() + "($0);");
        Iterator<Tuple> iter = pigServer.openIterator("B");
        assertTrue("No Output received", iter.hasNext());
        Tuple t = iter.next();
        assertEquals("ABC", t.get(0));
        Util.deleteFile(cluster, "test6");
    }
}
