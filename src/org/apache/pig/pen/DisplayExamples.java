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
package org.apache.pig.pen;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.pig.data.DataAtom;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Datum;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.logicalLayer.OperatorKey;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.TupleSchema;
import org.apache.pig.impl.util.IdentityHashSet;

public class DisplayExamples {
	
	public static StringBuffer Result = new StringBuffer();
	public static final int MAX_DATAATOM_LENGTH = 15;
	
    public static String PrintTabular(LogicalPlan lp, Map<LogicalOperator, DataBag> exampleData) {
    	StringBuffer output = new StringBuffer();
    	
    	LogicalOperator currentOp = lp.getRootOperator();
    	PrintTabular(currentOp, exampleData, output);
    	return output.toString();
    }
    
    static void PrintTabular(LogicalOperator op, Map<LogicalOperator, DataBag> exampleData, StringBuffer output) {
    	DataBag bag = exampleData.get(op);
    	
    	List<OperatorKey> inputs = op.getInputs();
    	for(OperatorKey opKey : inputs) {
    		PrintTabular(op.getOpTable().get(opKey), exampleData, output);
    	}
    	if(op.getAlias() != null) {
    		//PrintTable(op, bag, output);
    		DisplayTable(MakeArray(op, bag), op, bag, output);
    	}
    	
    }
    
    
    static String AddSpaces(int n, boolean printSpace) {
    	StringBuffer str = new StringBuffer();
    	for(int i = 0; i < n; ++i) {
    		if(printSpace)
    			str.append(" ");
    		else
    			str.append("-");
    	}
    	return str.toString();
    }
    
    
    static void DisplayTable(String[][] table, LogicalOperator op, DataBag bag, StringBuffer output) {
    	int cols = op.outputSchema().getFields().size();
    	List<Schema> fields = op.outputSchema().fields;
    	int rows = (int)bag.size();
    	int [] maxColSizes = new int[cols];
    	for(int i = 0; i < cols; ++i) {
    		maxColSizes[i] = fields.get(i).toString().length();
    		if(maxColSizes[i] < 5)	maxColSizes[i] = 5;
    	}
    	int total = 0;
    	int aliasLength = op.getAlias().length() + 4;
    	for(int j = 0; j < cols; ++j) {
    		for(int i = 0; i < rows; ++i) {
    			int length = table[i][j].length();
    			if(length > maxColSizes[j])
    				maxColSizes[j] = length;
    		}
    		total += maxColSizes[j];
    	}
    	
    	//Display the schema first
    	output.append(AddSpaces(total + 3*(cols +1) + aliasLength + 1, false) + "\n");
    	output.append("| " + op.getAlias() + AddSpaces(4, true) + " | ");
    	for(int i = 0; i < cols; ++i) {
    		String field = fields.get(i).toString();
    		output.append(field + AddSpaces(maxColSizes[i] - field.length(), true) + " | ");
    	}
    	output.append("\n" + AddSpaces(total + 3*(cols +1) + aliasLength + 1, false) + "\n");
    	//now start displaying the data
    	for(int i = 0; i < rows; ++i) {
    		output.append("| " + AddSpaces(aliasLength, true) + " | ");
    		for(int j = 0; j < cols; ++j) {
    			String str = table[i][j];
    			output.append(str + AddSpaces(maxColSizes[j] - str.length(), true) + " | ");
    		}
    		output.append("\n");
    	}
    	//now display the finish line
    	output.append(AddSpaces(total + 3*(cols +1) + aliasLength + 1, false) + "\n");
    }
    
    static String[][] MakeArray(LogicalOperator op, DataBag bag) {
    	int rows = (int)bag.size();
    	int cols = op.outputSchema().getFields().size();
    	String[][] table = new String [rows][cols];
    	Iterator<Tuple> it = bag.iterator();
    	for(int i = 0; i < rows; ++i) {
    		Tuple t = it.next();
    		for(int j = 0; j < cols; ++j) {
    			table[i][j] = ShortenField(t.getField(j));
    		}
    	}
    	return table;
    }
    
    
    static String ShortenField(Datum d) {
    	if(d instanceof Tuple) return ShortenField((Tuple)d);
    	else if(d instanceof DataBag) return ShortenField((DataBag)d);
    	else if(d instanceof DataAtom) return ShortenField((DataAtom)d);
    	else {
    		System.out.println("Wrong data-type received!!!");
    		return null;
    	}
    }
    
    static String ShortenField(DataAtom da) {
    	int length = da.toString().length();
    	if(length > MAX_DATAATOM_LENGTH) {
    		return new String(da.toString().substring(0, 4) + " ... " + da.toString().substring(length - 4, length - 1)); 
    	}
    	return da.toString();
    }
    
    static String ShortenField(DataBag bag) {
    	StringBuffer str = new StringBuffer();
    	long size = bag.size();
    	str.append("{");
    	if(size > 3) {
    		Iterator<Tuple> it = bag.iterator();
    		str.append(ShortenField(it.next()));
    		while(it.hasNext()) {
    			Tuple t = it.next();
    			if(!it.hasNext()) {
    				str.append(", ..., " + ShortenField(t));
    			}
    		}
    	} else {
    		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
    			Tuple t = it.next();
    			if(it.hasNext()) {
    				str.append(ShortenField(t) + ", ");
    			} else
    				str.append(ShortenField(t));
    		}
    	}
    	str.append("}");
    	return str.toString();
    }
    
    static String ShortenField(Tuple t) {
    	StringBuffer str = new StringBuffer();
    	int noFields = t.arity();
    	str.append("(");
    	if(noFields > 3) {
    		Datum d = t.getField(0);
			
			str.append(ShortenField(d) + ", ..., ");
			d = t.getField(noFields - 1);
			
			str.append(ShortenField(d));
    		
    	} else {
    		for(int i = 0; i < noFields; ++i) {
    			Datum d;
				d = t.getField(i);
				
				if(i != (noFields - 1)) {
					str.append(ShortenField(d) + ", ");
				} else {
					str.append(ShortenField(d));
				}
    			
    		}
    	}
    	str.append(")");
    	return str.toString();
    }
    	

}
