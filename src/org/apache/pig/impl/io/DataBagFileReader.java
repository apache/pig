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
package org.apache.pig.impl.io;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.data.Tuple;


public class DataBagFileReader {
	File store;
	
	public DataBagFileReader(File f) throws IOException{
		store = f;
	}
	
	private class myIterator implements Iterator<Tuple>{
		DataInputStream in;
		Tuple nextTuple;
		
		public myIterator() throws IOException{
			in = new DataInputStream(new BufferedInputStream(new FileInputStream(store)));
			getNextTuple();
		}
		
		private void getNextTuple() throws IOException{
			try{
				nextTuple = new Tuple();
		        nextTuple.readFields(in);
			} catch (EOFException e) {
				in.close();
				nextTuple = null;
			}
		}
		
		public boolean hasNext(){
			return nextTuple != null;
		}
		
		public Tuple next(){
			Tuple returnValue = nextTuple;
			if (returnValue!=null){
				try{
					getNextTuple();
				}catch (IOException e){
					throw new RuntimeException(e.getMessage());
				}
			}
			return returnValue;
		}
		
		public void remove(){
			throw new RuntimeException("Read only cursor");
		}
	}

	public Iterator<Tuple> content() throws IOException{
		return new myIterator();		
	}
	
	public void clear() throws IOException{
		store.delete();
	}
}
