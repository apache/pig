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

package org.apache.pig.backend.hadoop.datastorage;

import java.util.Iterator;
import java.util.Map;
import java.util.Enumeration;

import org.apache.hadoop.conf.Configuration;

import java.util.Properties;


public class HConfiguration extends Properties {
    
    private static final long serialVersionUID = 1L;
        
    public HConfiguration() {
    }

    //
    // TODO: this implementation has a problem: it does not
    //       respect final attributes
    //
    public HConfiguration(Configuration other) {
        if (other != null) {
            Iterator<Map.Entry<String, String>> iter = other.iterator();
            
            while (iter.hasNext()) {
                Map.Entry<String, String> entry = iter.next();
                
                put(entry.getKey(), entry.getValue());
            }
        }
    }

    
    public Configuration getConfiguration() {
        Configuration config = new Configuration();

        Enumeration<Object> iter = keys();
        
        while (iter.hasMoreElements()) {
            String key = (String) iter.nextElement();
            String val = getProperty(key);
           
            config.set(key, val);
        }

        return config;
    }
}



