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

import org.apache.pig.data.Datum;
import org.apache.pig.data.DatumImpl;


public class DataBagFileReader {
	File store;
	
	public DataBagFileReader(File f) throws IOException{
		store = f;
	}
	
	private class myIterator implements Iterator<Datum>{
		DataInputStream in;
		Datum nextDatum;
		
		public myIterator() throws IOException{
			in = new DataInputStream(new BufferedInputStream(new FileInputStream(store)));
			getNextDatum();
		}
		
		private void getNextDatum() throws IOException{
			try{
				/*
				nextDatum = new Datum();
		        nextDatum.readFields(in);
				*/
				nextDatum = DatumImpl.readDatum(in);
			} catch (EOFException e) {
				in.close();
				nextDatum = null;
			}
		}
		
		public boolean hasNext(){
			return nextDatum != null;
		}
		
		public Datum next(){
			Datum returnValue = nextDatum;
			if (returnValue!=null){
				try{
					getNextDatum();
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

	public Iterator<Datum> content() throws IOException{
		return new myIterator();		
	}
	
	public void clear() throws IOException{
		store.delete();
	}
}
