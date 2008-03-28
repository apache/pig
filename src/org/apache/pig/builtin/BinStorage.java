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

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Map;

import org.apache.pig.LoadFunc;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataReaderWriter;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.logicalLayer.schema.Schema;


public class BinStorage implements LoadFunc, StoreFunc {
    public static final byte RECORD_1 = 0x21;
    public static final byte RECORD_2 = 0x31;
    public static final byte RECORD_3 = 0x41;

    Iterator<Tuple>     i              = null;
    protected BufferedPositionedInputStream in = null;
    private DataInputStream inData = null;
    protected long                end            = Long.MAX_VALUE;
    
    /**
     * Simple binary nested reader format
     */
    public BinStorage() {
    }

    public Tuple getNext() throws IOException {
        
        byte b = 0;
//      skip to next record
        while (true) {
            if (in == null || in.getPosition() >=end) {
                return null;
            }
            b = (byte) in.read();
            if(b != RECORD_1 && b != -1) {
                continue;
            }
            if(b == -1) return null;
            b = (byte) in.read();
            if(b != RECORD_2 && b != -1) {
                continue;
            }
            if(b == -1) return null;
            b = (byte) in.read();
            if(b != RECORD_3 && b != -1) {
                continue;
            }
            if(b == -1) return null;
            break;
        }
        return (Tuple)DataReaderWriter.readDatum(inData);
    }

    public void bindTo(String fileName, BufferedPositionedInputStream in, long offset, long end) throws IOException {
        this.in = in;
        inData = new DataInputStream(in);
        this.end = end;
    }


    DataOutputStream         out     = null;
  
    public void bindTo(OutputStream os) throws IOException {
        this.out = new DataOutputStream(new BufferedOutputStream(os));
    }

    public void finish() throws IOException {
        out.flush();
    }

    public void putNext(Tuple t) throws IOException {
        out.write(RECORD_1);
        out.write(RECORD_2);
        out.write(RECORD_3);
        t.write(out);
    }

	public DataBag bytesToBag(byte[] b) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	public Boolean bytesToBoolean(byte[] b) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	public String bytesToCharArray(byte[] b) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	public Double bytesToDouble(byte[] b) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	public Float bytesToFloat(byte[] b) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	public Integer bytesToInteger(byte[] b) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	public Long bytesToLong(byte[] b) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	public Map<Object, Object> bytesToMap(byte[] b) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	public Tuple bytesToTuple(byte[] b) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	public Schema determineSchema(String fileName, BufferedPositionedInputStream in, long end) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	public void fieldsToRead(Schema schema) {
		// TODO Auto-generated method stub
		
	}
}
