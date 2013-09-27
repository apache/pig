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
package org.apache.pig.builtin;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.LoadCaster;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextInputFormat;
import org.apache.pig.bzip2r.Bzip2TextInputFormat;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.joda.time.DateTime;


/**
 * This load function simply creates a tuple for each line of text that has a
 * single chararray field that
 * contains the line of text.
 */
public class TextLoader extends LoadFunc implements LoadCaster {
    protected RecordReader in = null;
    private TupleFactory mTupleFactory = TupleFactory.getInstance();
    private String loadLocation;

    @Override
    public Tuple getNext() throws IOException {
        try {
            boolean notDone = in.nextKeyValue();
            if (!notDone) {
                return null;
            }
            Text value = (Text) in.getCurrentValue();
            byte[] ba = value.getBytes();
            // make a copy of the bytes representing the input since
            // TextInputFormat will reuse the byte array
            return mTupleFactory.newTuple(new DataByteArray(ba, 0, value.getLength()));
        } catch (InterruptedException e) {
            throw new IOException("Error getting input");
        }
    }

    /**
     * TextLoader does not support conversion to Boolean
     * @throws IOException if the value cannot be cast.
     */
    @Override
    public Boolean bytesToBoolean(byte[] b) throws IOException {
        int errCode = 2109;
        String msg = "TextLoader does not support conversion to Boolean.";
        throw new ExecException(msg, errCode, PigException.BUG);
    }

    /**
     * TextLoader does not support conversion to Integer
     * @throws IOException if the value cannot be cast.
     */
    @Override
    public Integer bytesToInteger(byte[] b) throws IOException {
        int errCode = 2109;
        String msg = "TextLoader does not support conversion to Integer.";
        throw new ExecException(msg, errCode, PigException.BUG);
    }

    /**
     * TextLoader does not support conversion to Long
     * @throws IOException if the value cannot be cast.
     */
    @Override
    public Long bytesToLong(byte[] b) throws IOException {
        int errCode = 2109;
        String msg = "TextLoader does not support conversion to Long.";
        throw new ExecException(msg, errCode, PigException.BUG);
    }

    /**
     * TextLoader does not support conversion to Float
     * @throws IOException if the value cannot be cast.
     */
    @Override
    public Float bytesToFloat(byte[] b) throws IOException {
        int errCode = 2109;
        String msg = "TextLoader does not support conversion to Float.";
        throw new ExecException(msg, errCode, PigException.BUG);
    }

    /**
     * TextLoader does not support conversion to Double
     * @throws IOException if the value cannot be cast.
     */
    @Override
    public Double bytesToDouble(byte[] b) throws IOException {
        int errCode = 2109;
        String msg = "TextLoader does not support conversion to Double.";
        throw new ExecException(msg, errCode, PigException.BUG);
    }

    /**
     * TextLoader does not support conversion to DateTime
     * @throws IOException if the value cannot be cast.
     */
    @Override
    public DateTime bytesToDateTime(byte[] b) throws IOException {
        int errCode = 2109;
        String msg = "TextLoader does not support conversion to DateTime.";
        throw new ExecException(msg, errCode, PigException.BUG);
    }

    /**
     * Cast data from bytes to chararray value.
     * @param b byte array to be cast.
     * @return String value.
     * @throws IOException if the value cannot be cast.
     */
    @Override
    public String bytesToCharArray(byte[] b) throws IOException {
        return new String(b);
    }

    @Override
    public Map<String, Object> bytesToMap(byte[] b, ResourceFieldSchema schema) throws IOException {
        return bytesToMap(b, schema);
    }

    /**
     * TextLoader does not support conversion to Tuple
     * @throws IOException if the value cannot be cast.
     */
    @Override
    public Tuple bytesToTuple(byte[] b, ResourceFieldSchema schema) throws IOException {
        int errCode = 2109;
        String msg = "TextLoader does not support conversion to Tuple.";
        throw new ExecException(msg, errCode, PigException.BUG);
    }

    /**
     * TextLoader does not support conversion to Bag
     * @throws IOException if the value cannot be cast.
     */
    public DataBag bytesToBag(byte[] b, ResourceFieldSchema schema) throws IOException {
        int errCode = 2109;
        String msg = "TextLoader does not support conversion to Bag.";
        throw new ExecException(msg, errCode, PigException.BUG);
    }

    public byte[] toBytes(DataBag bag) throws IOException {
        int errCode = 2109;
        String msg = "TextLoader does not support conversion from Bag.";
        throw new ExecException(msg, errCode, PigException.BUG);
    }

    public byte[] toBytes(String s) throws IOException {
        return s.getBytes();
    }

    public byte[] toBytes(Double d) throws IOException {
        int errCode = 2109;
        String msg = "TextLoader does not support conversion from Double.";
        throw new ExecException(msg, errCode, PigException.BUG);
    }

    public byte[] toBytes(Float f) throws IOException {
        int errCode = 2109;
        String msg = "TextLoader does not support conversion from Float.";
        throw new ExecException(msg, errCode, PigException.BUG);
    }

    public byte[] toBytes(Boolean b) throws IOException {
        int errCode = 2109;
        String msg = "TextLoader does not support conversion from Boolean.";
        throw new ExecException(msg, errCode, PigException.BUG);
    }

    public byte[] toBytes(Integer i) throws IOException {
        int errCode = 2109;
        String msg = "TextLoader does not support conversion from Integer.";
        throw new ExecException(msg, errCode, PigException.BUG);
    }

    public byte[] toBytes(Long l) throws IOException {
        int errCode = 2109;
        String msg = "TextLoader does not support conversion from Long.";
        throw new ExecException(msg, errCode, PigException.BUG);
    }

    public byte[] toBytes(DateTime dt) throws IOException {
        int errCode = 2109;
        String msg = "TextLoader does not support conversion from DateTime.";
        throw new ExecException(msg, errCode, PigException.BUG);
    }

    public byte[] toBytes(Map<String, Object> m) throws IOException {
        int errCode = 2109;
        String msg = "TextLoader does not support conversion from Map.";
        throw new ExecException(msg, errCode, PigException.BUG);
    }

    public byte[] toBytes(Tuple t) throws IOException {
        int errCode = 2109;
        String msg = "TextLoader does not support conversion from Tuple.";
        throw new ExecException(msg, errCode, PigException.BUG);
    }

    @Override
    public BigInteger bytesToBigInteger(byte[] b) throws IOException {
        int errCode = 2109;
        String msg = "TextLoader does not support conversion to BigInteger.";
        throw new ExecException(msg, errCode, PigException.BUG);
    }

    @Override
    public BigDecimal bytesToBigDecimal(byte[] b) throws IOException {
        int errCode = 2109;
        String msg = "TextLoader does not support conversion to BigDecimal.";
        throw new ExecException(msg, errCode, PigException.BUG);
    }

    @Override
    public InputFormat getInputFormat() {
        if(loadLocation.endsWith(".bz2") || loadLocation.endsWith(".bz")) {
            return new Bzip2TextInputFormat();
        } else {
            return new PigTextInputFormat();
        }
    }

    @Override
    public LoadCaster getLoadCaster() {
        return this;
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) {
        in = reader;
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        loadLocation = location;
        FileInputFormat.setInputPaths(job, location);
    }
}
