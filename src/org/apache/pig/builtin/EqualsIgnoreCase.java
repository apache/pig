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

import java.io.IOException;

import org.apache.pig.FilterFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

/**
 * Compares two Strings ignoring case considerations.
 *
 */

public class EqualsIgnoreCase extends FilterFunc {

@Override
public Boolean exec(Tuple input) throws IOException {
	    if (input == null || input.size() == 0) {
	        return null;
	    }
	    try{
	    	String firstStr = input.get(0).toString();
	    	String secondStr = input.get(1).toString();
	    	if (firstStr == null) {
	    		throw new ExecException("First String is NULL");
	    	}
	    	if (secondStr == null) {
	    		throw new ExecException("Second String is NULL");
	    	}
	    	return new Boolean(firstStr.equalsIgnoreCase(secondStr));
	    }	catch(Exception e){
	    	throw new ExecException(e.getMessage());
	 }
	}
}
	
