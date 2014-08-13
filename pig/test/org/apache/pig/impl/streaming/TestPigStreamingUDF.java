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
package org.apache.pig.impl.streaming;

import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.builtin.ToDate;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.WritableByteArray;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestPigStreamingUDF {
    PigStreamingUDF ps = new PigStreamingUDF();
    TupleFactory tf = TupleFactory.getInstance();

    @Test
    public void testSerialize__nullTuple() throws IOException {
        byte[] expectedOutput = "|_\n".getBytes();
        Assert.assertTrue(assertEquals(expectedOutput, ps.serializeToBytes(null)));
    }
    
    @Test
    public void testSerialize__emptyTuple() throws IOException {
        Tuple t = tf.newTuple(0);
        byte[] expectedOutput = "|_\n".getBytes();
        Assert.assertTrue(assertEquals(expectedOutput, ps.serializeToBytes(t)));
    }

    @Test
    public void testSerialize__record() throws IOException {
        Tuple t = tf.newTuple(1);
        t.set(0, "12\n34\n");
        byte[] expectedOutput = "C12\n34\n|_\n".getBytes();
        Assert.assertTrue(assertEquals(expectedOutput, ps.serializeToBytes(t)));
    }
 
    @Test
    public void testSerialize__string() throws IOException {
        Tuple t = tf.newTuple(1);
        t.set(0, "1234");
        byte[] expectedOutput = "C1234|_\n".getBytes();
        Assert.assertTrue(assertEquals(expectedOutput, ps.serializeToBytes(t)));
    }

    @Test
    public void testSerialize__emptyString() throws IOException {
        Tuple t = tf.newTuple(1);
        t.set(0, "");
        byte[] expectedOutput = "C|_\n".getBytes();
        Assert.assertTrue(assertEquals(expectedOutput, ps.serializeToBytes(t)));
    }

    @Test
    public void testSerialize__nullString() throws IOException {
        Tuple t = tf.newTuple(1);
        t.set(0, null);
        byte[] expectedOutput = "|-_|_\n".getBytes();
        Assert.assertTrue(assertEquals(expectedOutput, ps.serializeToBytes(t)));
    }
    
    @Test
    public void testSerialize__boolean() throws IOException {
        Tuple t = tf.newTuple(1);
        t.set(0, false);
        byte[] expectedOutput = "Bfalse|_\n".getBytes();
        Assert.assertTrue(assertEquals(expectedOutput, ps.serializeToBytes(t)));
    }
    
    @Test
    public void testSerialize__datetime() throws IOException {
        DateTime dt = new DateTime();
        Tuple t = tf.newTuple(1);
        t.set(0, dt);
        byte[] expectedOutput = ("T" + ISODateTimeFormat.dateTime().print(dt) +"|_\n").getBytes();
        Assert.assertTrue(assertEquals(expectedOutput, ps.serializeToBytes(t)));
    }

    @Test
    public void testSerialize__multifieldTuple() throws IOException {
        Tuple t = tf.newTuple(2);
        t.set(0, 1234);
        t.set(1, "sam");
        byte[] expectedOutput = "I1234|\t_Csam|_\n".getBytes();
        Assert.assertTrue(assertEquals(expectedOutput, ps.serializeToBytes(t)));
    }

    @Test
    public void testSerialize__multifieldTupleWithNull() throws IOException {
        Tuple t = tf.newTuple(3);
        t.set(0, 1234);
        t.set(1, "sam");
        t.set(2, null);
        byte[] expectedOutput = "I1234|\t_Csam|\t_|-_|_\n".getBytes();
        Assert.assertTrue(assertEquals(expectedOutput, ps.serializeToBytes(t)));
    }

    @Test
    public void testSerialize__nestedTuple() throws IOException {
        Tuple t = tf.newTuple(2);
        Tuple nestedTuple = tf.newTuple(2);
        nestedTuple.set(0, "sam");
        nestedTuple.set(1, "oes");
        t.set(0, nestedTuple);
        t.set(1, "basil");
        byte[] expectedOutput = "|(_Csam|,_Coes|)_|\t_Cbasil|_\n".getBytes();
        Assert.assertTrue(assertEquals(expectedOutput, ps.serializeToBytes(t)));
    }

    @Test
    public void testSerialize__bag() throws IOException {
        Tuple t = tf.newTuple(1);
        Tuple t1 = tf.newTuple(2);
        Tuple t2 = tf.newTuple(2);
        List<Tuple> bagTuples = new ArrayList<Tuple>();
        bagTuples.add(t1);
        bagTuples.add(t2);
        t1.set(0, "A");
        t1.set(1, "B");
        t2.set(0, 1);
        t2.set(1, 2);
        DataBag b = DefaultBagFactory.getInstance().newDefaultBag(bagTuples);
        t.set(0,b);
        byte[] expectedOutput = "|{_|(_CA|,_CB|)_|,_|(_I1|,_I2|)_|}_|_\n".getBytes();
        Assert.assertTrue(assertEquals(expectedOutput, ps.serializeToBytes(t)));
    }

    @Test
    public void testSerialize__map() throws IOException {
        Tuple t =tf.newTuple(1);
        Map<String, String> m = new TreeMap<String, String>();
        m.put("A", "B");
        m.put("C", "D");
        t.set(0,m);
        byte[] expectedOutput = "|[_CA#CB|,_CC#CD|]_|_\n".getBytes();
        Assert.assertTrue(assertEquals(expectedOutput, ps.serializeToBytes(t)));
    }

    @Test
    public void testSerialize__complex_map() throws IOException {
        Tuple t = tf.newTuple(1);
        Map<String, Object> inner_map = new TreeMap<String, Object>();
        inner_map.put("A", 1);
        inner_map.put("B", "E");
        
        Map<String, Object> outer_map = new TreeMap<String, Object>();
        outer_map.put("C", "F");
        outer_map.put("D", inner_map);
        
        t.set(0,outer_map);
        byte[] expectedOutput = "|[_CC#CF|,_CD#|[_CA#I1|,_CB#CE|]_|]_|_\n".getBytes();
        Assert.assertTrue(assertEquals(expectedOutput, ps.serializeToBytes(t)));
    }

    @Test
    public void testSerialize__allDataTypes() throws IOException {
        Tuple t = tf.newTuple(8);
        t.set(0, null);
        t.set(1, true);
        t.set(2, 2);
        t.set(3, 3L);
        t.set(4, 4.0f);
        t.set(5, 5.0d);
        t.set(6, new DataByteArray("six"));
        t.set(7, "seven");
        byte[] expectedOutput = "|-_|\t_Btrue|\t_I2|\t_L3|\t_F4.0|\t_D5.0|\t_Asix|\t_Cseven|_\n".getBytes();
        Assert.assertTrue(assertEquals(expectedOutput, ps.serializeToBytes(t)));
    }
    

    @Test
    public void testDeserialize__newline() throws IOException {
        byte[] input = "12\n34\n|_".getBytes();
        FieldSchema schema = new FieldSchema("", DataType.CHARARRAY);
        PigStreamingUDF sp = new PigStreamingUDF(schema);
        
        Tuple out = sp.deserialize(input, 0, input.length);
        Assert.assertEquals(tf.newTuple("12\n34\n"), out);
    }

    @Test
    public void testDeserialize__string() throws IOException {
        byte[] input = "1234|_".getBytes();
        FieldSchema schema = new FieldSchema("", DataType.CHARARRAY);
        PigStreamingUDF sp = new PigStreamingUDF(schema);

        Object out = sp.deserialize(input, 0, input.length);
        Assert.assertEquals(tf.newTuple("1234"), out);
    }
    
    @Test
    public void testDeserialize__emptyString() throws IOException {
        byte[] input = "|_".getBytes();
        FieldSchema schema = new FieldSchema("", DataType.CHARARRAY);
        PigStreamingUDF sp = new PigStreamingUDF(schema);

        Object out = sp.deserialize(input, 0, input.length);
        Assert.assertEquals(tf.newTuple(""), out);
    }

    @Test
    public void testDeserialize__nullString() throws IOException {
        byte[] input = "|-_|_".getBytes();
        FieldSchema schema = new FieldSchema("", DataType.CHARARRAY);
        PigStreamingUDF sp = new PigStreamingUDF(schema);
        
        Tuple expectedOutput = tf.newTuple(1);
        expectedOutput.set(0, null);

        Object out = sp.deserialize(input, 0, input.length);
        Assert.assertEquals(expectedOutput, out);
    }
    
    @Test
    public void testDeserialize__boolean() throws IOException {
        byte[] input = "true|_".getBytes();
        FieldSchema schema = new FieldSchema("", DataType.BOOLEAN);
        PigStreamingUDF sp = new PigStreamingUDF(schema);

        Object out = sp.deserialize(input, 0, input.length);
        Assert.assertEquals(tf.newTuple(Boolean.TRUE), out);
    }
    
    @Test
    public void testDeserialize__datetime() throws IOException {
        String dateString = "2013-08-22T09:22:08.784004";
        byte[] input = (dateString + "|_").getBytes();
        FieldSchema schema = new FieldSchema("", DataType.DATETIME);
        PigStreamingUDF sp = new PigStreamingUDF(schema);

        Object out = sp.deserialize(input, 0, input.length);
        Assert.assertEquals(tf.newTuple(ToDate.extractDateTime(dateString)), out);
    }

    @Test
    public void testDeserialize__multifieldTuple() throws IOException {
        byte[] input = "|(_1234|,_sam|)_|_".getBytes();
        FieldSchema f1 = new FieldSchema("", DataType.INTEGER);
        FieldSchema f2 = new FieldSchema("", DataType.CHARARRAY);
        List<FieldSchema> fsl = new ArrayList<FieldSchema>();
        fsl.add(f1);
        fsl.add(f2);
        Schema schema = new Schema(fsl);

        FieldSchema fs = new FieldSchema("", schema, DataType.TUPLE);
        Tuple expectedOutput = tf.newTuple(2);
        expectedOutput.set(0, 1234);
        expectedOutput.set(1, "sam");
        PigStreamingUDF sp = new PigStreamingUDF(fs);
        
        Object out = sp.deserialize(input, 0, input.length);
        Assert.assertEquals(tf.newTuple(expectedOutput), out);
    }
    
    @Test
    public void testDeserialize__nestedTuple() throws IOException {
        byte[] input = "|(_|(_sammy|,_oes|)_|,_1234|,_sam|)_|_".getBytes();
        FieldSchema f1Inner = new FieldSchema("", DataType.CHARARRAY);
        FieldSchema f2Inner = new FieldSchema("", DataType.CHARARRAY);
        List<FieldSchema> fslInner = new ArrayList<FieldSchema>();
        fslInner.add(f1Inner);
        fslInner.add(f2Inner);
        Schema schemaInner = new Schema(fslInner);
        
        FieldSchema f1 = new FieldSchema("", schemaInner, DataType.TUPLE);
        FieldSchema f2 = new FieldSchema("", DataType.INTEGER);
        FieldSchema f3 = new FieldSchema("", DataType.CHARARRAY);
        List<FieldSchema> fsl = new ArrayList<FieldSchema>();
        fsl.add(f1);
        fsl.add(f2);
        fsl.add(f3);
        Schema schema = new Schema(fsl);
        FieldSchema fs = new FieldSchema("", schema, DataType.TUPLE);
        PigStreamingUDF sp = new PigStreamingUDF(fs);
        
        Tuple expectedOutputInner = tf.newTuple(2);
        expectedOutputInner.set(0, "sammy");
        expectedOutputInner.set(1, "oes");
        
        Tuple expectedOutput = tf.newTuple(3);
        expectedOutput.set(0, expectedOutputInner);
        expectedOutput.set(1, 1234);
        expectedOutput.set(2, "sam");
        
        Object out = sp.deserialize(input, 0, input.length);
        Assert.assertEquals(tf.newTuple(expectedOutput), out);
    }
    
    @Test
    public void testDeserialize__bag() throws IOException {
        byte[] input = "|{_|(_A|,_1|)_|,_|(_B|,_2|)_|}_|_".getBytes();
        FieldSchema f1Inner = new FieldSchema("", DataType.CHARARRAY);
        FieldSchema f2Inner = new FieldSchema("", DataType.INTEGER);
        List<FieldSchema> fslInner = new ArrayList<FieldSchema>();
        fslInner.add(f1Inner);
        fslInner.add(f2Inner);
        Schema schemaInner = new Schema(fslInner);
        FieldSchema fsInner = new FieldSchema("", schemaInner, DataType.TUPLE);
        
        List<FieldSchema> fsl = new ArrayList<FieldSchema>();
        fsl.add(fsInner);
        Schema schema = new Schema(fsl);
        
        FieldSchema fs = new FieldSchema("", schema, DataType.BAG);
        PigStreamingUDF sp = new PigStreamingUDF(fs);

        
        Tuple expectedOutputInner1 = tf.newTuple(2);
        expectedOutputInner1.set(0, "A");
        expectedOutputInner1.set(1, 1);
        
        Tuple expectedOutputInner2 = tf.newTuple(2);
        expectedOutputInner2.set(0, "B");
        expectedOutputInner2.set(1, 2);
        
        List<Tuple> tuples = new ArrayList<Tuple>();
        tuples.add(expectedOutputInner1);
        tuples.add(expectedOutputInner2);
        DataBag expectedOutput = DefaultBagFactory.getInstance().newDefaultBag(tuples);

        Object out = sp.deserialize(input, 0, input.length);
        Assert.assertEquals(tf.newTuple(expectedOutput), out);
    }
    
    @Test
    public void testDeserialize__map() throws IOException {
        byte[] input = "|[_A#B|,_C#D|]_|_".getBytes();
        FieldSchema fs = new FieldSchema("", DataType.MAP);
        PigStreamingUDF sp = new PigStreamingUDF(fs);

        
        Map<String, String> expectedOutput = new TreeMap<String, String>();
        expectedOutput.put("A", "B");
        expectedOutput.put("C", "D");        
        
        Object out = sp.deserialize(input, 0, input.length);
        Assert.assertEquals(tf.newTuple(expectedOutput), out);
    }
    
    @Test
    public void testDeserialize__bug() throws Exception {
        byte[] input = "|(_|-_|,_32|,_987654321098765432|,_987654321098765432|)_|_".getBytes();

        FieldSchema f1 = new FieldSchema("", DataType.CHARARRAY);
        FieldSchema f2 = new FieldSchema("", DataType.INTEGER);
        FieldSchema f3 = new FieldSchema("", DataType.LONG);
        FieldSchema f4 = new FieldSchema("", DataType.LONG);

        List<FieldSchema> fsl = new ArrayList<FieldSchema>();
        fsl.add(f1);
        fsl.add(f2);
        fsl.add(f3);
        fsl.add(f4);
        Schema schema = new Schema(fsl);
        FieldSchema fs = new FieldSchema("", schema, DataType.TUPLE);
        PigStreamingUDF sp = new PigStreamingUDF(fs);

        
        Tuple expectedOutput1 = tf.newTuple(4);
        expectedOutput1.set(0, null);
        expectedOutput1.set(1, 32);
        expectedOutput1.set(2, 987654321098765432L);
        expectedOutput1.set(3, 987654321098765432L);
        
        Object out = sp.deserialize(input, 0, input.length);
        Assert.assertEquals(tf.newTuple(expectedOutput1), out);
    }
    
    private boolean assertEquals(byte[] expected, WritableByteArray wba) {
        byte[] data = wba.getData();
        if (expected.length != wba.getLength()) {
            return false;
        }

        for (int i = 0; i < expected.length; i++) {
            if (expected[i] != data[i]) {
                return false;
            }
        }

        return true;
    }
}
