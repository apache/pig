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
