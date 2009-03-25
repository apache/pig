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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Map;

import org.apache.pig.ExecType;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.logicalLayer.schema.Schema;


/**
 * This load function simply creates a tuple for each line of text that has a single field that
 * contains the line of text.
 */
public class TextLoader implements LoadFunc{
    BufferedPositionedInputStream in;
    final private static Charset utf8 = Charset.forName("UTF8");
    long end;
    private TupleFactory mTupleFactory = TupleFactory.getInstance();

    public void bindTo(String fileName, BufferedPositionedInputStream in, long offset, long end) throws IOException {
        this.in = in;
        this.end = end;
        // Since we are not block aligned we throw away the first
        // record and count on a different instance to read it
        if (offset != 0)
            getNext();
    }

    public Tuple getNext() throws IOException {
        if (in == null || in.getPosition() > end)
            return null;
        String line;
        if ((line = in.readLine(utf8, (byte)'\n')) != null) {
            if (line.length()>0 && line.charAt(line.length()-1)=='\r' && System.getProperty("os.name").toUpperCase().startsWith("WINDOWS"))
                line = line.substring(0, line.length()-1);
            return mTupleFactory.newTuple(new DataByteArray(line.getBytes()));
        }
        return null;
    }

    /**
     * TextLoader does not support conversion to Boolean.
     * @throws IOException if the value cannot be cast.
     */
    public Boolean bytesToBoolean(byte[] b) throws IOException {
        int errCode = 2109;
        String msg = "TextLoader does not support conversion to Boolean.";
        throw new ExecException(msg, errCode, PigException.BUG);
    }
    
    /**
     * TextLoader does not support conversion to Integer
     * @throws IOException if the value cannot be cast.
     */
    public Integer bytesToInteger(byte[] b) throws IOException {
        int errCode = 2109;
        String msg = "TextLoader does not support conversion to Integer.";
        throw new ExecException(msg, errCode, PigException.BUG);
    }

    /**
     * TextLoader does not support conversion to Long
     * @throws IOException if the value cannot be cast.
     */
    public Long bytesToLong(byte[] b) throws IOException {
        int errCode = 2109;
        String msg = "TextLoader does not support conversion to Long.";
        throw new ExecException(msg, errCode, PigException.BUG);
    }

    /**
     * TextLoader does not support conversion to Float
     * @throws IOException if the value cannot be cast.
     */
    public Float bytesToFloat(byte[] b) throws IOException {
        int errCode = 2109;
        String msg = "TextLoader does not support conversion to Float.";
        throw new ExecException(msg, errCode, PigException.BUG);
    }

    /**
     * TextLoader does not support conversion to Double
     * @throws IOException if the value cannot be cast.
     */
    public Double bytesToDouble(byte[] b) throws IOException {
        int errCode = 2109;
        String msg = "TextLoader does not support conversion to Double.";
        throw new ExecException(msg, errCode, PigException.BUG);
    }

    /**
     * Cast data from bytes to chararray value.  
     * @param b byte array to be cast.
     * @return String value.
     * @throws IOException if the value cannot be cast.
     */
    public String bytesToCharArray(byte[] b) throws IOException {
        return new String(b);
    }

    /**
     * TextLoader does not support conversion to Map
     * @throws IOException if the value cannot be cast.
     */
    public Map<Object, Object> bytesToMap(byte[] b) throws IOException {
        int errCode = 2109;
        String msg = "TextLoader does not support conversion to Map.";
        throw new ExecException(msg, errCode, PigException.BUG);
    }

    /**
     * TextLoader does not support conversion to Tuple
     * @throws IOException if the value cannot be cast.
     */
    public Tuple bytesToTuple(byte[] b) throws IOException {
        int errCode = 2109;
        String msg = "TextLoader does not support conversion to Tuple.";
        throw new ExecException(msg, errCode, PigException.BUG);
    }

    /**
     * TextLoader does not support conversion to Bag
     * @throws IOException if the value cannot be cast.
     */
    public DataBag bytesToBag(byte[] b) throws IOException {
        int errCode = 2109;
        String msg = "TextLoader does not support conversion to Bag.";
        throw new ExecException(msg, errCode, PigException.BUG);
    }

    /**
     * TextLoader doesn't make use of this.
     */
    public void fieldsToRead(Schema schema) {}

    /**
     * TextLoader does not provide a schema.
     */
    public Schema determineSchema(String fileName, ExecType execType,
            DataStorage storage) throws IOException {
        // TODO Auto-generated method stub
        return null;
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

    public byte[] toBytes(Map<Object, Object> m) throws IOException {
        int errCode = 2109;
        String msg = "TextLoader does not support conversion from Map.";
        throw new ExecException(msg, errCode, PigException.BUG);
    }

    public byte[] toBytes(Tuple t) throws IOException {
        int errCode = 2109;
        String msg = "TextLoader does not support conversion from Tuple.";
        throw new ExecException(msg, errCode, PigException.BUG);
    }
}
