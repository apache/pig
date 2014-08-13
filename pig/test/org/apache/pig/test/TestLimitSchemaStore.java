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
package org.apache.pig.test;

import java.io.File;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestLimitSchemaStore{
    
    
    private PigServer pigServer;

    @Before
    public void setUp() throws Exception{
        pigServer = new PigServer(ExecType.LOCAL);
    }
    
    
    //PIG-2146
    @Test //end to end test
    public void testLimitStoreSchema1() throws Exception{
        Util.createLocalInputFile("student", new String[]{"joe smith:18:3.5","amy brown:25:2.5","jim fox:20:4.0","leo fu:55:3.0"});
        
        pigServer.registerQuery("a = load 'student' using " + PigStorage.class.getName() + "(':') as (name, age, gpa);");
        pigServer.registerQuery("d = distinct a;");
        pigServer.registerQuery("lim = limit d 1;");
        String outFile = "limitSchemaOut";
        Util.deleteDirectory(new File(outFile));
        pigServer.store("lim", outFile,  "PigStorage('\\t', '-schema')");
        pigServer.dumpSchema("lim");
        
        pigServer.registerQuery("b = LOAD '" + outFile + "' using PigStorage('\\t', '-schema');");
        Schema genSchema = pigServer.dumpSchema("b");
        System.err.println(genSchema);
        Assert.assertNotNull(genSchema);
        
    } 
  
      
}
