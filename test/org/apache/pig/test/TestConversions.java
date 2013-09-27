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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.builtin.Utf8StorageConverter;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.parser.ParserException;
import org.apache.pig.test.utils.GenRandomData;
import org.apache.pig.test.utils.TestHelper;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Test;

/**
 * Test class to test conversions from bytes to types
 * and vice versa
 *
 */
public class TestConversions {

    PigStorage ps = new PigStorage();
    Random r = new Random(42L);
    final int MAX = 10;

    @Before
    public void setUp() {
        DateTimeZone.setDefault(DateTimeZone.forOffsetMillis(DateTimeZone.UTC.getOffset(null)));
    }

    @Test
    public void testBytesToBoolean() throws IOException {
        // valid booleans
        String[] a = { "true", "True", "TRUE", "false", "False", "FALSE" };
        Boolean[] b = { Boolean.TRUE, Boolean.TRUE, Boolean.TRUE,
                Boolean.FALSE, Boolean.FALSE, Boolean.FALSE };

        for (int i = 0; i < b.length; ++i) {
            byte[] bytes = a[i].getBytes();
            assertEquals(b[i], ps.getLoadCaster().bytesToBoolean(bytes));
        }

        // invalid booleans
        // the string that is neither "true" nor "false" cannot be converted to a boolean value
        a = new String[] { "neither true nor false" };
        for (String s : a) {
            byte[] bytes = s.getBytes();
            Boolean bool = ps.getLoadCaster().bytesToBoolean(bytes);
            assertNull(bool);
        }
    }

    @Test
    public  void testBytesToInteger() throws IOException
    {
        // valid ints
        String[] a = {"1234 ", " 4321", " 12345 ", "1", "-2345",  "1234567", "1.1", "-23.45", ""};
        Integer[] ia = {1234, 4321, 12345, 1, -2345, 1234567, 1, -23};

        for (int i = 0; i < ia.length; i++) {
            byte[] b = a[i].getBytes();
            assertEquals(ia[i], ps.getLoadCaster().bytesToInteger(b));
        }

        // invalid ints
        a = new String[]{"1234567890123456", "This is an int", "", "   "};
        for (String s : a) {
            byte[] b = s.getBytes();
            Integer i = ps.getLoadCaster().bytesToInteger(b);
            assertNull(i);
        }
    }

    @Test
    public  void testBytesToFloat() throws IOException
    {
        // valid floats
        String[] a = {"1", "-2.345",  "12.12334567", "1.02e-2",".23344",
              "23.1234567897", "12312.33", "002312.33", "1.02e-2", ""};

        Float[] f = {1f, -2.345f,  12.12334567f, 1.02e-2f,.23344f, 23.1234567f, // 23.1234567f is a truncation case
             12312.33f, 2312.33f, 1.02e-2f };

        for (int j = 0; j < f.length; j++) {
            byte[] b = a[j].getBytes();
            assertEquals(f[j], ps.getLoadCaster().bytesToFloat(b));
        }

        // invalid floats
        a = new String[]{"1a.1", "23.1234567a890123456",  "This is a float", "", "  "};
        for (String s : a) {
            byte[] b = s.getBytes();
            Float fl = ps.getLoadCaster().bytesToFloat(b);
            assertNull(fl);

        }
    }

    @Test
    public  void testBytesToDouble() throws IOException
    {
        // valid doubles
        String[] a = {"1", "-2.345",  "12.12334567890123456", "1.02e12","-.23344", ""};
        Double[] d = {1.0, -2.345,  12.12334567890123456, 1.02e12, -.23344};
        for (int j = 0; j < d.length; j++) {
            byte[] b = a[j].getBytes();
            assertEquals(d[j], ps.getLoadCaster().bytesToDouble(b));
        }

        // invalid doubles
        a = new String[]{"-0x1.1", "-23a.45",  "This is a double", "", "   "};
        for (String s : a) {
            byte[] b = s.getBytes();
            Double dl = ps.getLoadCaster().bytesToDouble(b);
            assertNull(dl);

        }
    }

    @Test
    public  void testBytesToLong() throws IOException
    {
        // valid Longs
        String[] a = {"1703598819951657279 ", " 999888123L  ", " 1234 ", " 1234", "1234 ", "1", "-2345",  "123456789012345678", "1.1", "-23.45",
              "21345345", "3422342", ""};
        Long[] la = {1703598819951657279L, 999888123L, 1234L, 1234L, 1234L, 1L, -2345L, 123456789012345678L, 1L, -23L,
             21345345L, 3422342L};

        for (int i = 0; i < la.length; i++) {
            byte[] b = a[i].getBytes();
            assertEquals(la[i], ps.getLoadCaster().bytesToLong(b));
        }

        // invalid longs
        a = new String[]{"This is a long", "1.0e1000", "", "    "};
        for (String s : a) {
            byte[] b = s.getBytes();
            Long l = ps.getLoadCaster().bytesToLong(b);
            assertNull(l);
        }
    }

    @Test
    public  void testBytesToChar() throws IOException
    {
        // valid Strings
        String[] a = {"1", "-2345",  "text", "hello\nworld", ""};

        for (String s : a) {
            byte[] b = s.getBytes();
            assertEquals(s, ps.getLoadCaster().bytesToCharArray(b));
        }
    }

    @Test
    public  void testBytesToTuple() throws IOException
    {
        for (int i = 0; i < MAX; i++) {
            Tuple t = GenRandomData.genRandSmallBagTextTuple(r, 1, 100);

            ResourceFieldSchema fs = GenRandomData.getSmallBagTextTupleFieldSchema();

            Tuple convertedTuple = ps.getLoadCaster().bytesToTuple(t.toString().getBytes(), fs);
            assertTrue(TestHelper.tupleEquals(t, convertedTuple));
        }

    }

    @Test
    public  void testBytesToBag() throws IOException
    {
        ResourceFieldSchema fs = GenRandomData.getFullTupTextDataBagFieldSchema();

        for (int i = 0; i < MAX; i++) {
            DataBag b = GenRandomData.genRandFullTupTextDataBag(r,5,100);
            DataBag convertedBag = ps.getLoadCaster().bytesToBag(b.toString().getBytes(), fs);
            assertTrue(TestHelper.bagEquals(b, convertedBag));
        }

    }

    @Test
    public  void testBytesToMap() throws IOException
    {
        ResourceFieldSchema fs = GenRandomData.getRandMapFieldSchema();

        for (int i = 0; i < MAX; i++) {
            Map<String, Object>  m = GenRandomData.genRandMap(r,5);
            String expectedMapString = DataType.mapToString(m);
            Map<String, Object> convertedMap = ps.getLoadCaster().bytesToMap(expectedMapString.getBytes(), fs);
            assertTrue(TestHelper.mapEquals(m, convertedMap));
        }

    }

    @Test
    public void testBooleanToBytes() throws IOException {
        assertTrue(DataType.equalByteArrays(Boolean.TRUE.toString().getBytes(), ((Utf8StorageConverter)ps.getLoadCaster()).toBytes(Boolean.TRUE)));
        assertTrue(DataType.equalByteArrays(Boolean.FALSE.toString().getBytes(), ((Utf8StorageConverter)ps.getLoadCaster()).toBytes(Boolean.FALSE)));
    }

    @Test
    public void testIntegerToBytes() throws IOException {
        Integer i = r.nextInt();
        assertTrue(DataType.equalByteArrays(i.toString().getBytes(), ((Utf8StorageConverter)ps.getLoadCaster()).toBytes(i)));
    }

    @Test
    public void testLongToBytes() throws IOException {
        Long l = r.nextLong();
        assertTrue(DataType.equalByteArrays(l.toString().getBytes(), ((Utf8StorageConverter)ps.getLoadCaster()).toBytes(l)));
    }

    @Test
    public void testFloatToBytes() throws IOException {
        Float f = r.nextFloat();
        assertTrue(DataType.equalByteArrays(f.toString().getBytes(), ((Utf8StorageConverter)ps.getLoadCaster()).toBytes(f)));
    }

    @Test
    public void testDoubleToBytes() throws IOException {
        Double d = r.nextDouble();
        assertTrue(DataType.equalByteArrays(d.toString().getBytes(), ((Utf8StorageConverter)ps.getLoadCaster()).toBytes(d)));
    }

    @Test
    public void testDateTimeToBytes() throws IOException {
        DateTime dt = new DateTime(r.nextLong());
        assertTrue(DataType.equalByteArrays(dt.toString().getBytes(), ((Utf8StorageConverter)ps.getLoadCaster()).toBytes(dt)));
    }

    @Test
    public void testCharArrayToBytes() throws IOException {
        String s = GenRandomData.genRandString(r);
        assertTrue(s.equals(new String(((Utf8StorageConverter)ps.getLoadCaster()).toBytes(s))));
    }

    @Test
    public void testTupleToBytes() throws IOException {
        Tuple t = GenRandomData.genRandSmallBagTextTuple(r, 1, 100);
        assertTrue(DataType.equalByteArrays(t.toString().getBytes(), ((Utf8StorageConverter)ps.getLoadCaster()).toBytes(t)));
    }

    @Test
    public void testBagToBytes() throws IOException {
        DataBag b = GenRandomData.genRandFullTupTextDataBag(r,5,100);
        assertTrue(DataType.equalByteArrays(b.toString().getBytes(), ((Utf8StorageConverter)ps.getLoadCaster()).toBytes(b)));
    }

    @Test
    public void testMapToBytes() throws IOException {
        Map<String, Object>  m = GenRandomData.genRandMap(r,5);
        assertTrue(DataType.equalByteArrays(DataType.mapToString(m).getBytes(), ((Utf8StorageConverter)ps.getLoadCaster()).toBytes(m)));
    }

    @Test
    public void testBytesToBagWithConversion() throws IOException {
        DataBag b = GenRandomData.genFloatDataBag(r,5,100);
        ResourceFieldSchema fs = GenRandomData.getFloatDataBagFieldSchema(5);
        DataBag convertedBag = ps.getLoadCaster().bytesToBag(b.toString().getBytes(), fs);

        Iterator<Tuple> iter1 = b.iterator();
        Iterator<Tuple> iter2 = convertedBag.iterator();
        for (int i=0;i<100;i++) {
            Tuple t1 = (Tuple)iter1.next();
            assertTrue(iter2.hasNext());
            Tuple t2 = (Tuple)iter2.next();
            for (int j=0;j<5;j++) {
                assertTrue(t2.get(j) instanceof Integer);
                Integer expectedValue = ((Float)t1.get(j)).intValue();
                assertEquals(expectedValue, t2.get(j));
            }
        }
    }

    @Test
    public void testBytesToTupleWithConversion() throws IOException {
        for (int i=0;i<100;i++) {
            Tuple t = GenRandomData.genMixedTupleToConvert(r);
            ResourceFieldSchema fs = GenRandomData.getMixedTupleToConvertFieldSchema();
            Tuple convertedTuple = ps.getLoadCaster().bytesToTuple(t.toString().getBytes(), fs);

            assertTrue(convertedTuple.get(0) instanceof String);
            assertEquals(convertedTuple.get(0), ((Integer)t.get(0)).toString());

            assertTrue(convertedTuple.get(1) instanceof Long);
            Integer origValue1 = (Integer)t.get(1);
            assertEquals(convertedTuple.get(1), Long.valueOf(origValue1.longValue()));

            assertNull(convertedTuple.get(2));

            assertTrue(convertedTuple.get(3) instanceof Double);
            Float origValue3 = (Float)t.get(3);
            assertEquals(((Double)convertedTuple.get(3)).doubleValue(), origValue3.doubleValue(), 0.01);

            assertTrue(convertedTuple.get(4) instanceof Float);
            Double origValue4 = (Double)t.get(4);
            assertEquals((Float)convertedTuple.get(4), origValue4.floatValue(), 0.01);

            assertTrue(convertedTuple.get(5) instanceof String);
            assertEquals(convertedTuple.get(5), t.get(5));

            assertNull(convertedTuple.get(6));

            assertNull(convertedTuple.get(7));

            assertNull(convertedTuple.get(8));

            assertTrue(convertedTuple.get(9) instanceof Boolean);
            String origValue9 = (String)t.get(9);
            assertEquals(Boolean.valueOf(origValue9), convertedTuple.get(9));
        }
    }

    @Test
    public void testBytesToComplexTypeMisc() throws IOException, ParserException {
        String s = "(a,b";
        Schema schema = Utils.getSchemaFromString("t:tuple(a:chararray, b:chararray)");
        ResourceFieldSchema rfs = new ResourceSchema(schema).getFields()[0];
        Tuple t = ps.getLoadCaster().bytesToTuple(s.getBytes(), rfs);
        assertNull(t);

        s = "{(a,b}";
        schema = Utils.getSchemaFromString("b:bag{t:tuple(a:chararray, b:chararray)}");
        rfs = new ResourceSchema(schema).getFields()[0];
        DataBag b = ps.getLoadCaster().bytesToBag(s.getBytes(), rfs);
        assertNull(b);

        s = "{(a,b)";
        schema = Utils.getSchemaFromString("b:bag{t:tuple(a:chararray, b:chararray)}");
        rfs = new ResourceSchema(schema).getFields()[0];
        b = ps.getLoadCaster().bytesToBag(s.getBytes(), rfs);
        assertNull(b);

        s = "[ab]";
        schema = Utils.getSchemaFromString("m:map[chararray]");
        rfs = new ResourceSchema(schema).getFields()[0];
        Map<String, Object> m = ps.getLoadCaster().bytesToMap(s.getBytes(), rfs);
        assertNull(m);

        s = "[a#b";
        m = ps.getLoadCaster().bytesToMap(s.getBytes(), rfs);
        assertNull(m);

        s = "[a#]";
        m = ps.getLoadCaster().bytesToMap(s.getBytes(), rfs);
        Map.Entry<String, Object> entry = m.entrySet().iterator().next();
        assertEquals("a", entry.getKey());
        assertNull(entry.getValue());

        s = "[#]";
        m = ps.getLoadCaster().bytesToMap(s.getBytes(), rfs);
        assertNull(m);

        s = "[a#}";
        m = ps.getLoadCaster().bytesToMap(s.getBytes(), rfs);
        assertNull(m);

        s = "[a#)";
        m = ps.getLoadCaster().bytesToMap(s.getBytes(), rfs);
        assertNull(m);

        s = "(a,b)";
        schema = Utils.getSchemaFromString("t:tuple()");
        rfs = new ResourceSchema(schema).getFields()[0];
        t = ps.getLoadCaster().bytesToTuple(s.getBytes(), rfs);
        assertEquals(2, t.size());
        assertTrue(t.get(0) instanceof DataByteArray);
        assertEquals("a", t.get(0).toString());
        assertTrue(t.get(1) instanceof DataByteArray);
        assertEquals("b", t.get(1).toString());

        s = "[a#(1,2,3)]";
        schema = Utils.getSchemaFromString("m:map[]");
        rfs = new ResourceSchema(schema).getFields()[0];
        m = ps.getLoadCaster().bytesToMap(s.getBytes(), rfs);
        entry = m.entrySet().iterator().next();
        assertEquals("a", entry.getKey());
        assertTrue(entry.getValue() instanceof DataByteArray);
        assertEquals("(1,2,3)", entry.getValue().toString());

        s = "(a,b,(123,456,{(1,2,3)}))";
        schema = Utils.getSchemaFromString("t:tuple()");
        rfs = new ResourceSchema(schema).getFields()[0];
        t = ps.getLoadCaster().bytesToTuple(s.getBytes(), rfs);
        assertTrue(t.size()==3);
        assertTrue(t.get(0) instanceof DataByteArray);
        assertEquals("a", t.get(0).toString());
        assertTrue(t.get(1) instanceof DataByteArray);
        assertEquals("b", t.get(1).toString());
        assertTrue(t.get(2) instanceof DataByteArray);
        assertEquals("(123,456,{(1,2,3)})", t.get(2).toString());

        s = "(a,b,(123,456,{(1,2,3}))";
        schema = Utils.getSchemaFromString("t:tuple()");
        rfs = new ResourceSchema(schema).getFields()[0];
        t = ps.getLoadCaster().bytesToTuple(s.getBytes(), rfs);
        assertNull(t);
    }

    @Test
    public void testOverflow() throws IOException, ParserException {
        Schema schema;
        ResourceFieldSchema rfs;
        Tuple tuple, convertedTuple;
        tuple = TupleFactory.getInstance().newTuple(1);

        schema = Utils.getSchemaFromString("t:tuple(a:int)");
        rfs = new ResourceSchema(schema).getFields()[0];

        // long bigger than Integer.MAX_VALUE
        tuple.set(0, Integer.valueOf(Integer.MAX_VALUE).longValue() + 1);
        convertedTuple = ps.getLoadCaster().bytesToTuple(tuple.toString().getBytes(), rfs);
        assertNull("Invalid cast to int: " + tuple.get(0) + " -> " + convertedTuple.get(0), convertedTuple.get(0));

        // long smaller than Integer.MIN_VALUE
        tuple.set(0, Integer.valueOf(Integer.MIN_VALUE).longValue() - 1);
        convertedTuple = ps.getLoadCaster().bytesToTuple(tuple.toString().getBytes(), rfs);
        assertNull("Invalid cast to int: " + tuple.get(0) + " -> " + convertedTuple.get(0), convertedTuple.get(0));

        // double bigger than Integer.MAX_VALUE
        tuple.set(0, Integer.valueOf(Integer.MAX_VALUE).doubleValue() + 1);
        convertedTuple = ps.getLoadCaster().bytesToTuple(tuple.toString().getBytes(), rfs);
        assertNull("Invalid cast to int: " + tuple.get(0) + " -> " + convertedTuple.get(0), convertedTuple.get(0));

        // double smaller than Integer.MIN_VALUE
        tuple.set(0, Integer.valueOf(Integer.MIN_VALUE).doubleValue() - 1);
        convertedTuple = ps.getLoadCaster().bytesToTuple(tuple.toString().getBytes(), rfs);
        assertNull("Invalid cast to int: " + tuple.get(0) + " -> " + convertedTuple.get(0), convertedTuple.get(0));

        schema = Utils.getSchemaFromString("t:tuple(a:long)");
        rfs = new ResourceSchema(schema).getFields()[0];

        // double bigger than Long.MAX_VALUE
        tuple.set(0, Long.valueOf(Long.MAX_VALUE).doubleValue() + 10000);
        convertedTuple = ps.getLoadCaster().bytesToTuple(tuple.toString().getBytes(), rfs);
        assertNull("Invalid cast to long: " + tuple.get(0) + " -> " + convertedTuple.get(0), convertedTuple.get(0));

        // double smaller than Long.MIN_VALUE
        tuple.set(0, Long.valueOf(Long.MIN_VALUE).doubleValue() - 10000);
        convertedTuple = ps.getLoadCaster().bytesToTuple(tuple.toString().getBytes(), rfs);
        assertNull("Invalid cast to long: " + tuple.get(0) + " -> " + convertedTuple.get(0), convertedTuple.get(0));
    }
}
