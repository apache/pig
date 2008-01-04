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
package org.apache.pig.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.Iterator;
import java.lang.String;


public class DataMap extends Datum {

	Map<String, Datum> content = new HashMap<String, Datum>();
	
	@Override
	public boolean equals(Object other) {
		return compareTo(other) == 0;
	}

	public int compareTo(Object other) {
		if (!(other instanceof DataMap))
			return -1;
		DataMap mbOther = (DataMap) other;
		if (mbOther.cardinality()!=cardinality())
			return cardinality() - mbOther.cardinality();
		for (String key: content.keySet()){
			if (!content.get(key).equals(mbOther.get(key)))
				return -1;
		}
		return 0;
	}
	
	/**
	 * 
	 * @return the cardinality of the data map
	 */
	public int cardinality(){
		return content.size();
	}
	
	/**
	 * Adds the key value pair to the map
	 * @param key
	 * @param value
	 */
	public void put(String key, Datum value){
		content.put(key, value);
	}
	
	/**
	 * Adds the value as a data atom mapped to the given key
	 * @param key
	 * @param value
	 */
	public void put(String key, String value){
		content.put(key, new DataAtom(value));
	}

	/**
	 * Adds the value as a data atom mapped to the given key
	 * @param key
	 * @param value
	 */
	
	public void put(String key, int value){
		content.put(key, new DataAtom(value));
	}


	/**
	 * Fetch the value corresponding to a given key
	 * @param key
	 * @return
	 */
	public Datum get(String key){
		Datum d = content.get(key);
		if (d == null)
			return new DataAtom("");
		else
			return d;
	}
	
	@Override
	public String toString(){
		return content.toString();
	}
	
	public static DataMap read(DataInput in) throws IOException{
		int size = Tuple.decodeInt(in);
		DataMap ret = new DataMap();
        byte[] b = new byte[1];
               
        for (int i = 0; i < size; i++) {
            in.readFully(b);
            if (b[0]!=ATOM)
            	throw new IOException("Invalid data when reading map from binary file");
            String key = DataAtom.read(in).strval();
            Datum value = Tuple.readDatum(in);
            ret.put(key, value);
        }
        return ret;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.write(MAP);
        Tuple.encodeInt(out, cardinality());
        for (Entry<String, Datum> e: content.entrySet()){
        	DataAtom d = new DataAtom(e.getKey());
        	d.write(out);
        	e.getValue().write(out);
        }
 	}
	
	
	public Datum remove(String key){
		return content.remove(key);
	}
	
	public Set<String> keySet(){
		return content.keySet();
	}
	
	public Map<String, Datum> content(){
		return content;
	}

    @Override
    public long getMemorySize() {
        long used = 0;
        Iterator<Map.Entry<String, Datum> > i = content.entrySet().iterator();
        while (i.hasNext()) {
            Map.Entry<String, Datum> e = i.next();
            used += e.getKey().length() * 2 + OBJECT_SIZE + REF_SIZE;
            used += e.getValue().getMemorySize() + REF_SIZE;
        }

        used += 2 * OBJECT_SIZE + REF_SIZE;
        return used;
    }

}
