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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileWriter;
import java.util.Iterator;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestUDFWithoutParameter {

    static String[] ScriptStatement = { "A = LOAD 'test/org/apache/pig/test/data/passwd' USING PigStorage();",
            "B = FOREACH A GENERATE org.apache.pig.test.utils.MyUDFWithoutParameter();" };

    static File TempScriptFile = null;

    @Before
    public void setUp() throws Exception {
        TempScriptFile = File.createTempFile("temp_jira_753", ".pig");
        FileWriter writer=new FileWriter(TempScriptFile);
        for (String line:ScriptStatement){
            writer.write(line+"\n");
        }
        writer.close();
    }

    @Test
    public void testUDFWithoutParameter() throws Exception {
        PigServer pig = new PigServer(ExecType.LOCAL);
        pig.registerScript(TempScriptFile.getAbsolutePath());

        Iterator<Tuple> iterator=pig.openIterator("B");
        int index=0;
        while(iterator.hasNext()){
            Tuple tuple=iterator.next();
            index++;
            int result=(Integer)tuple.get(0);
            assertEquals(result, index);
        }
    }

    @After
    public void tearDown() throws Exception {
        TempScriptFile.delete();
    }
}
