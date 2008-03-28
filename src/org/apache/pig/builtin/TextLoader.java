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

import org.apache.pig.LoadFunc;
import org.apache.pig.data.DataBag;
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
        // record and cound on a different instance to read it
        if (offset != 0)
            getNext();
    }

    public Tuple getNext() throws IOException {
        if (in == null || in.getPosition() > end)
            return null;
        String line;
        if ((line = in.readLine(utf8, (byte)'\n')) != null) {
            return mTupleFactory.newTuple(new String(line));
        }
        return null;
    }

    /**
     * TextLoader does not support conversion to Boolean.
     * @throws IOException if the value cannot be cast.
     */
    public Boolean bytesToBoolean(byte[] b) throws IOException {
        throw new IOException("TextLoader does not support conversion to Boolean");
    }
    
    /**
     * TextLoader does not support conversion to Integer
     */
    public Integer bytesToInteger(byte[] b) throws IOException {
        throw new IOException("TextLoader does not support conversion to Integer");
    }

    /**
     * TextLoader does not support conversion to Long
     */
    public Long bytesToLong(byte[] b) throws IOException {
        throw new IOException("TextLoader does not support conversion to Long");
    }

    /**
     * TextLoader does not support conversion to Float
     */
    public Float bytesToFloat(byte[] b) throws IOException {
        throw new IOException("TextLoader does not support conversion to Float");
    }

    /**
     * TextLoader does not support conversion to Double
     */
    public Double bytesToDouble(byte[] b) throws IOException {
        throw new IOException("TextLoader does not support conversion to Double");
    }

    /**
     * Cast data from bytes to chararray value.  
     * @param bytes byte array to be cast.
     * @return String value.
     * @throws IOException if the value cannot be cast.
     */
    public String bytesToCharArray(byte[] b) throws IOException {
        return new String(b);
    }

    /**
     * TextLoader does not support conversion to Map
     */
    public Map<Object, Object> bytesToMap(byte[] b) throws IOException {
        throw new IOException("TextLoader does not support conversion to Map");
    }

    /**
     * TextLoader does not support conversion to Tuple
     */
    public Tuple bytesToTuple(byte[] b) throws IOException {
        throw new IOException("TextLoader does not support conversion to Tuple");
    }

    /**
     * TextLoader does not support conversion to Bag
     */
    public DataBag bytesToBag(byte[] b) throws IOException {
        throw new IOException("TextLoader does not support conversion to Bag");
    }

    /**
     * TextLoader doesn't make use of this.
     */
    public void fieldsToRead(Schema schema) {}

    /**
     * TextLoader does not provide a schema.
     */
    public Schema determineSchema(URL fileName) throws IOException {
        return null;
    }

}
