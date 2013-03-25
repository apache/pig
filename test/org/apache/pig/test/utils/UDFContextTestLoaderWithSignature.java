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

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;

public class UDFContextTestLoaderWithSignature extends PigStorage {
    private String val;
    
    public UDFContextTestLoaderWithSignature(String v1) {
        val = v1;
    }
    
    @Override
    public void setLocation(String location, Job job)
            throws IOException {
        super.setLocation(location, job);
        Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass());
        if (p.get(signature)==null) {
            p.put("test_" + signature, val);
        }
    }
    
    @Override
    public Tuple getNext() throws IOException {
        Tuple t = super.getNext();
        if (t!=null) {
            Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass());
            t.append(p.get("test_" + signature));
        }
        return t;
    }
}
