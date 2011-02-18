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
package org.apache.pig.test.utils;

import java.io.File;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.lang.Boolean;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.pig.FilterFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigInputFormat;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;

/**
 * 
 * define MyFilterSet util.FILTERFROMFILE('/user/pig/filterfile');
 * 
 * A = load 'mydata' using PigStorage() as ( a, b );
 * B = filter A by MyFilterSet(a);
 * 
 */
public class FILTERFROMFILE extends FilterFunc{
	private String FilterFileName = "";
	
	public FILTERFROMFILE(){ 
    }
	
	public FILTERFROMFILE(String FilterFileName){
		this.FilterFileName = FilterFileName;
	}
	
	Map<String, Boolean> lookupTable = null;
	
	
	private void init() throws IOException {
	    
		lookupTable = new HashMap<String, Boolean>();
		
		Properties props = ConfigurationUtil.toProperties(PigMapReduce.sJobConfInternal.get());
		InputStream is = FileLocalizer.openDFSFile(FilterFileName, props);

		BufferedReader reader = new BufferedReader(new InputStreamReader(is));
		
		while (true){
			String line = reader.readLine();

			if (line == null)
				break;

			String FilterField = line.split("\t")[0];
			
			lookupTable.put(FilterField, Boolean.TRUE);
		}
	}
	
	@Override
	public Boolean exec(Tuple input) throws IOException {
	 if (lookupTable == null){
		init();
	  }	
	String s;
        try {
            s = input.get(0).toString();
        } catch (ExecException e) {
            IOException ioe = new IOException("Error getting data");
            ioe.initCause(e);
            throw ioe;
        }
		
	   boolean matched = lookupTable.containsKey(s);

	   return(matched);
    }
}
