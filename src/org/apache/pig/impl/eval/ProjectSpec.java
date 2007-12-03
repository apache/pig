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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.data.Datum;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.TupleSchema;


public class ProjectSpec extends SimpleEvalSpec {
	private static final long serialVersionUID = 1L;

	protected List<Integer> cols;
	protected boolean wrapInTuple;
	

	public List<Integer> getCols() {
		return cols;
	}

	public ProjectSpec(List<Integer> cols){		
		this.cols = cols;
	}
	
	public ProjectSpec(int col){		
		cols = new ArrayList<Integer>();
		cols.add(col);
	}
		
	@Override
	public List<String> getFuncs() {
		return new ArrayList<String>();
	}

	@Override
	protected Schema mapInputSchema(Schema schema) {
		if (!wrapInTuple && cols.size()==1){
			return maskNullSchema(schema.schemaFor(cols.get(0)));
		}else{
			TupleSchema output = new TupleSchema();
			for (int i: cols){
				output.add(maskNullSchema(schema.schemaFor(i)));
			}
			return output;
		}
	}
	
	private Schema maskNullSchema(Schema s){
		if (s == null)
			return new TupleSchema();
		else
			return s;
		
	}
	
	@Override
	protected Datum eval(Datum d){
		if (!(d instanceof Tuple)){
			throw new RuntimeException("Project operator expected a Tuple, found a " + d.getClass().getSimpleName());
		}
		Tuple t = (Tuple)d;
		
		try{
			if (!wrapInTuple && cols.size() == 1){
				return t.getField(cols.get(0));
			}else{
				Tuple out = new Tuple();
				for (int i: cols){
					out.appendField(t.getField(i));
				}
				return out;
			}
		}catch (IOException e){
			//TODO: Based on a strictness level, insert null values here
				throw new RuntimeException(e);		
		}
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("[");
		sb.append("PROJECT ");
		boolean first = true;
		for (int i: cols){
			if (!first)
				sb.append(",");
			else
				first = false;
			sb.append("$");
			sb.append(i);
		}
		sb.append("]");
		return sb.toString();
	}
    
    public int numCols() {
        return cols.size();
    }
    
    public int getCol(int i) {
        if (i < 0 || i >= cols.size()) 
            throw new RuntimeException("Internal error: improper use of getColumn in " + ProjectSpec.class.getName());
        else return cols.get(i);
    }
	
	public int getCol(){
		if (cols.size()!=1)
			throw new RuntimeException("Internal error: improper use of getColumn in " + ProjectSpec.class.getName());
		return cols.get(0);
	}

	public void setWrapInTuple(boolean wrapInTuple) {
		this.wrapInTuple = wrapInTuple;
	}

	@Override
	public void visit(EvalSpecVisitor v) {
		v.visitProject(this);
	}
    

}
