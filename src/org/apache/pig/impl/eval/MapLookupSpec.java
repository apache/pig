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
package org.apache.pig.impl.eval;

import java.util.ArrayList;
import java.util.List;

import org.apache.pig.data.DataMap;
import org.apache.pig.data.Datum;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.TupleSchema;


public class MapLookupSpec extends SimpleEvalSpec {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	protected String keyToLookup;
	
	public MapLookupSpec(String keyToLookup){
		this.keyToLookup = keyToLookup;
	}

	@Override
	protected Datum eval(Datum d) {
		if (!(d instanceof DataMap))
			throw new RuntimeException("Attempt to lookup on data of type " + d.getClass().getName());
		return ((DataMap)d).get(keyToLookup);
	}
	
	@Override
	public List<String> getFuncs() {
		return new ArrayList<String>();
	}
	
	@Override
	protected Schema mapInputSchema(Schema schema) {
		//TODO: until we have map schemas
		return new TupleSchema();
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("[");
		sb.append("#'");
		sb.append(keyToLookup);
		sb.append("'");
		sb.append("]");
		return sb.toString();
	}

	@Override
	public void visit(EvalSpecVisitor v) {
		v.visitMapLookup(this);
	}

	public String key() { return keyToLookup; }
    
	
}
