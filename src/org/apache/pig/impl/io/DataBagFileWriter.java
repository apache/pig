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
 /*
package org.apache.pig.impl.io;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.data.Tuple;



public class DataBagFileWriter {
	File store;
	DataOutputStream out;

	public DataBagFileWriter(File store) throws IOException{
		this.store = store;
		out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(store)));
	}
	
	public void write(Tuple t) throws IOException{
		t.write(out);
	}
	
	public long write(Iterator<Tuple> iter) throws IOException{
	
		long initialSize = getFileLength();
		while (iter.hasNext())
			iter.next().write(out);
		
		return getFileLength() - initialSize;
	}
	
	public long getFileLength() throws IOException{
		out.flush();
		return store.length();
	}
	
	
	public void close() throws IOException{
		flush();
		out.close();
	}
	
	public void flush() throws IOException{
		out.flush();
	}
	
}
*/
