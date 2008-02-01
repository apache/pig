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
package org.apache.pig.backend.hadoop.executionengine.mapreduceExec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.pig.LoadFunc;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.eval.EvalSpec;
import org.apache.pig.impl.util.ObjectSerializer;


public class PigSplit implements InputSplit {
    Path   file;
    long start;
    long length;
    EvalSpec groupbySpec;
    EvalSpec evalSpec;
    int    index;
    String parser;
    FileSystem fs;
    PigContext pigContext;

    public PigSplit() {}

    public PigSplit(PigContext pigContext, FileSystem fs, Path path, String parser, EvalSpec groupbySpec, EvalSpec evalSpec, int index, long start, long length) {
		this.fs = fs;
		this.file = path;
		this.start = start;
		this.length = length;
		this.groupbySpec = groupbySpec;
		this.evalSpec = evalSpec;
		this.index = index;
		this.parser = parser;
		this.pigContext = pigContext;
    }

    public String getParser() {
        return parser;
    }

    public long getStart() {
	return start;
    }
    
    public long getLength() {
	return length;
    }
    public EvalSpec getGroupbySpec() {
        return groupbySpec;
    }

    public EvalSpec getEvalSpec() {
        return evalSpec;
    }

    public Path getPath() {
        return file;
    }

    public int getIndex() {
        return index;
    }

    public LoadFunc getLoadFunction() {
        LoadFunc loader = null;
        if (this.parser == null) {
            loader = new PigStorage();
        } else {
            try {
                loader = (LoadFunc) PigContext.instantiateFuncFromSpec(this.parser);
            }catch(Exception exp) {
                throw new RuntimeException("can't instantiate " + parser);
            }
        }
        return loader;
    }


    public String[] getLocations() throws IOException {
		String hints[][] = fs.getFileCacheHints(file, start, length);
		int total = 0;
		for(int i = 0; i < hints.length; i++) {
		    total += hints[i].length;
		}
		String locations[] = new String[total];
		int count = 0;
		for(int i = 0; i < hints.length; i++) {
		    for(int j = 0; j < hints[i].length; j++) {
			locations[count++] = hints[i][j];
		    }
		}
		return locations;
    }

    public void readFields(DataInput is) throws IOException {
	file = new Path(is.readUTF());
	start = is.readLong();
	length = is.readLong();
	pigContext = (PigContext)readObject(is);
	groupbySpec = (EvalSpec)readObject(is);
	if(groupbySpec != null ) groupbySpec.instantiateFunc(pigContext);
	evalSpec = (EvalSpec) readObject(is);
	if(evalSpec != null) evalSpec.instantiateFunc(pigContext);
	index = is.readInt();
	parser = is.readUTF();
    }

    public void write(DataOutput os) throws IOException {
	os.writeUTF(file.toString());
	os.writeLong(start);
	os.writeLong(length);
	writeObject(pigContext, os);
	writeObject(groupbySpec, os);
	writeObject(evalSpec, os);
	os.writeInt(index);
	os.writeUTF(parser);
    }
    
    private void writeObject(Serializable obj, DataOutput os) throws IOException{
    	ByteArrayOutputStream baos = new ByteArrayOutputStream();
    	ObjectOutputStream oos = new ObjectOutputStream(baos);
    	oos.writeObject(obj);
    	byte[] bytes = baos.toByteArray();
    	os.writeInt(bytes.length);
    	os.write(bytes);
    }
    
    private Object readObject(DataInput is) throws IOException{
    	byte[] bytes = new byte[is.readInt()];
    	is.readFully(bytes);
    	ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
    	try{
    		return ois.readObject();
    	}catch (ClassNotFoundException cnfe){
    		IOException newE = new IOException(cnfe.getMessage());
                newE.initCause(cnfe);
                throw newE;
    	}
    }
    
}
