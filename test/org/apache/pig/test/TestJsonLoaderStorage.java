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
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Iterator;

import junit.framework.Assert;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestJsonLoaderStorage {
    private static PigServer pigServer;
    File jsonFile;
    
    @BeforeClass
    public static void setUp() throws Exception{
        removeOutput();
        pigServer = new PigServer(ExecType.LOCAL);
    }
    
    private static void removeOutput() {
        File outputDir = new File("jsonStorage1.json");
        if (outputDir.exists()) {
            for (File c : outputDir.listFiles())
                c.delete();
            outputDir.delete();
        }
    }
    
    @Test
    public void testJsonStorage1() throws Exception{
        removeOutput();
        
        pigServer.registerScript("test/org/apache/pig/test/data/jsonStorage1.pig");
        
        File resultFile = new File("jsonStorage1.json/part-m-00000");
        
        String result = Util.readFile(resultFile);
        String expected = Util.readFile(new File("test/org/apache/pig/test/data/jsonStorage1.result"));
        Assert.assertTrue(result.equals(expected));
        
        File schemaFile = new File("jsonStorage1.json/.pig_schema");
        result = Util.readFile(schemaFile);
        expected = Util.readFile(new File("test/org/apache/pig/test/data/jsonStorage1.schema"));
        Assert.assertTrue(result.equals(expected));
    }
    
    @Test
    public void testJsonLoader1() throws Exception{
        
        File tmpFile = File.createTempFile("tmp", null);
        tmpFile.delete();
        
        pigServer.registerQuery("a = load 'jsonStorage1.json' using JsonLoader();");
        pigServer.store("a", tmpFile.getCanonicalPath());
        
        String result = Util.readFile(new File(tmpFile.getCanonicalPath()+"/part-m-00000"));
        String expected = Util.readFile(new File("test/org/apache/pig/test/data/jsonStorage1.txt"));
        Assert.assertTrue(result.equals(expected));
    }
    
    @Test
    public void testJsonLoader2() throws Exception{
        
        File tmpFile = File.createTempFile("tmp", null);
        tmpFile.delete();
        
        File schemaFile = new File("test/org/apache/pig/test/data/jsonStorage1.schema");
        
        pigServer.registerQuery("a = load 'jsonStorage1.json' using" + 
        		" JsonLoader('a0:int,a1:{(a10:int,a11:chararray)},a2:(a20:double,a21:bytearray),a3:[chararray]');");
        pigServer.store("a", tmpFile.getCanonicalPath());
        
        String result = Util.readFile(new File(tmpFile.getCanonicalPath()+"/part-m-00000"));
        String expected = Util.readFile(new File("test/org/apache/pig/test/data/jsonStorage1.txt"));
        Assert.assertTrue(result.equals(expected));
    }
    
    @Test
    public void testJsonStorage2() throws Exception{
        
        File inputFile = File.createTempFile("tmp", null);
        PrintWriter pw = new PrintWriter(new FileWriter(inputFile));
        pw.println("\t\t\t");
        pw.close();
        
        File interFile = File.createTempFile("tmp", null);
        interFile.delete();
        
        pigServer.registerQuery("a = load '" + inputFile.getCanonicalPath() + "' as (a0:int, a1:chararray, a2, a3:(a30:int));");
        pigServer.store("a", interFile.getCanonicalPath(), "JsonStorage");
        
        pigServer.registerQuery("b = load '" + interFile.getCanonicalPath() + "' using JsonLoader();");
        Iterator<Tuple> iter = pigServer.openIterator("b");
        
        Tuple t = iter.next();
        
        Assert.assertTrue(t.size()==4);
        Assert.assertTrue(t.get(0)==null);
        Assert.assertTrue(t.get(1)==null);
        Assert.assertTrue(t.get(2)==null);
        Assert.assertTrue(t.get(3)==null);
        
        Assert.assertFalse(iter.hasNext());
    }

    @AfterClass
    public static void tearDown() {
        removeOutput();
    }
}