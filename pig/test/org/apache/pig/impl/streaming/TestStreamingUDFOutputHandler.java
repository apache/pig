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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;

import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;



@RunWith(JUnit4.class)
public class TestStreamingUDFOutputHandler {
    TupleFactory tf = TupleFactory.getInstance();
    
    @Test
    public void testGetValue__noNewLine() throws Exception{
        FieldSchema fs = new FieldSchema("", DataType.CHARARRAY);
        String data = "a|_\n";
        
        PigStreamingUDF deserializer = new PigStreamingUDF(fs);
        OutputHandler outty = new StreamingUDFOutputHandler(deserializer);
        outty.bindTo(null, getIn(data), 0, 0);
        
        Tuple t = outty.getNext();
        
        Assert.assertEquals(tf.newTuple("a"), t);
    }

    @Test
    public void testGetValue__embeddedNewLine() throws Exception{
        FieldSchema fs = new FieldSchema("", DataType.CHARARRAY);
        String data = "abc\ndef\nghi\njkl|_\n";
        
        PigStreamingUDF deserializer = new PigStreamingUDF(fs);
        OutputHandler outty = new StreamingUDFOutputHandler(deserializer);
        outty.bindTo(null, getIn(data), 0, 0);
        
        Tuple t = outty.getNext();
        
        Assert.assertEquals(tf.newTuple("abc\ndef\nghi\njkl"), t);
    }
    
    private BufferedPositionedInputStream getIn(String input) throws UnsupportedEncodingException {
        InputStream stream = new ByteArrayInputStream(input.getBytes("UTF-8"));
        BufferedPositionedInputStream result = new BufferedPositionedInputStream(stream);
        return result;
    }
}
