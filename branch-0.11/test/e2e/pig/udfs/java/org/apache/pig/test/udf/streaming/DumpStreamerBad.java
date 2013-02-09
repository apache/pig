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

package org.apache.pig.test.udf.streaming;

import java.io.IOException;
import java.net.URL;

import org.apache.pig.StreamToPig;
import org.apache.pig.LoadCaster;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DefaultTupleFactory;
import java.nio.charset.Charset;

import org.apache.pig.builtin.Utf8StorageConverter;

// This is a loader for data produced by DumpStore.
// Line format: (field1, field2, ... fieldn)\n

public class DumpStreamerBad implements StreamToPig {
    final private static Charset utf8 = Charset.forName("UTF8");

    @Override
    public Tuple deserialize(byte[] bytes) throws IOException {
        String line = new String(bytes, utf8);
        Tuple t = DefaultTupleFactory.getInstance().newTuple();
        String tmp = line.substring(1, line.length() - 2);
        String[] fields = tmp.split(",");
        int i;
        try{
            for (i = 0; i < fields.length; i++)
		t.set(i, fields[i].trim());
	}catch (Exception e){
	    throw new IOException(e.getMessage());
	}		

        return t;
    }

    @Override
    public LoadCaster getLoadCaster() throws IOException {
        return new Utf8StorageConverter();
    }   

}
