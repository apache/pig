/*
 * Licensed to the Apache Software Foundation (ASF) under one or more                  
 * contributor license agreements.  See the NOTICE file distributed with               
 * this work for additional information regarding copyright ownership.                 
 * The ASF licenses this file to You under the Apache License, Version 2.0             
 * (the "License"); you may not use this file except in compliance with                
 * the License.  You may obtain a copy of the License at                               
 *                                                                                     
 *     http://www.apache.org/licenses/LICENSE-2.0                                      
 *                                                                                     
 * Unless required by applicable law or agreed to in writing, software                 
 * distributed under the License is distributed on an "AS IS" BASIS,                   
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.            
 * See the License for the specific language governing permissions and                 
 * limitations under the License.                                                      
 */
 
package org.apache.pig.test.udf.evalfunc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;

public class Udfcachetest extends EvalFunc<String> {

    private String file;

    public Udfcachetest(String f) {
        super();
        file = f;
    }

    public String exec(Tuple input) throws IOException {
        // Read and return the first line of the file
        String f = null;
        try {
            URI uri = new URI(file);
            f = uri.getPath();
        } catch (URISyntaxException use) {
            throw new IOException("Unable to parse URI " + file + 
                ". Cannot ship to distributed cache.", use);
        }
 
        FileReader fr = new FileReader("./foodle");
        BufferedReader d = new BufferedReader(fr);
        String s = d.readLine();
		fr.close();
		return s;
    }
    
    public List<String> getCacheFiles() {
        List<String> list = new ArrayList<String>(1);
        list.add(file);
        log.info("UDF returning " + file);
        return list;
    }
}

