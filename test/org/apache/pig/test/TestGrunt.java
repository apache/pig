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

import org.junit.Test;
import junit.framework.TestCase;
import junit.framework.AssertionFailedError;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.impl.PigContext;
import org.apache.pig.tools.grunt.Grunt;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;

public class TestGrunt extends TestCase {
    MiniCluster cluster = MiniCluster.buildCluster();

    private final Log log = LogFactory.getLog(getClass());
    
/*    @Test 
    public void testCopyFromLocal() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();
        
        String strCmd = "copyFromLocal /bin/sh . ;";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }*/

    @Test 
    public void testDefine() throws Throwable {
        PigServer server = new PigServer("MAPREDUCE");
        PigContext context = server.getPigContext();
        
        String strCmd = "define myudf org.apache.pig.builtin.AVG();\n";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        try {
            grunt.exec();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Encountered \"define\""));
        }
        assertTrue(null != context.getFuncSpecFromAlias("myudf"));
    }

    @Test 
    public void testBagSchema() throws Throwable {
        PigServer server = new PigServer("MAPREDUCE");
        PigContext context = server.getPigContext();
        
        String strCmd = "a = load 'input1' as (b: bag{t(i: int, c:chararray, f: float)});\n";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test 
    public void testBagSchemaFail() throws Throwable {
        PigServer server = new PigServer("MAPREDUCE");
        PigContext context = server.getPigContext();
        
        String strCmd = "a = load 'input1'as (b: bag{t(i: int, c:chararray, f: float)});\n";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        try {
            grunt.exec();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Encountered \";\""));
        }
    }

    @Test 
    public void testBagConstant() throws Throwable {
        PigServer server = new PigServer("MAPREDUCE");
        PigContext context = server.getPigContext();
        
        String strCmd = "a = load 'input1'; b = foreach a generate {(1, '1', 0.4f),(2, '2', 0.45)};\n";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test 
    public void testBagConstantWithSchema() throws Throwable {
        PigServer server = new PigServer("MAPREDUCE");
        PigContext context = server.getPigContext();
        
        String strCmd = "a = load 'input1'; b = foreach a generate {(1, '1', 0.4f),(2, '2', 0.45)} as b: bag{t(i: int, c:chararray, d: double)};\n";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test 
    public void testBagConstantInForeachBlock() throws Throwable {
        PigServer server = new PigServer("MAPREDUCE");
        PigContext context = server.getPigContext();
        
        String strCmd = "a = load 'input1'; b = foreach a {generate {(1, '1', 0.4f),(2, '2', 0.45)};};\n";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test 
    public void testBagConstantWithSchemaInForeachBlock() throws Throwable {
        PigServer server = new PigServer("MAPREDUCE");
        PigContext context = server.getPigContext();
        
        String strCmd = "a = load 'input1'; b = foreach a {generate {(1, '1', 0.4f),(2, '2', 0.45)} as b: bag{t(i: int, c:chararray, d: double)};};\n";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test 
    public void testParsingAsInForeachBlock() throws Throwable {
        PigServer server = new PigServer("MAPREDUCE");
        PigContext context = server.getPigContext();
        
        String strCmd = "a = load 'foo' as (foo, fast); b = group a by foo; c = foreach b {generate SUM(a.fast) as fast;};\n";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test 
    public void testParsingAsInForeachWithOutBlock() throws Throwable {
        PigServer server = new PigServer("MAPREDUCE");
        PigContext context = server.getPigContext();
        
        String strCmd = "a = load 'foo' as (foo, fast); b = group a by foo; c = foreach b generate SUM(a.fast) as fast;\n";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test 
    public void testParsingWordWithAsInForeachBlock() throws Throwable {
        PigServer server = new PigServer("MAPREDUCE");
        PigContext context = server.getPigContext();
        
        String strCmd = "a = load 'foo' as (foo, fast); b = group a by foo; c = foreach b {generate SUM(a.fast);};\n";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test 
    public void testParsingWordWithAsInForeachWithOutBlock() throws Throwable {
        PigServer server = new PigServer("MAPREDUCE");
        PigContext context = server.getPigContext();
        
        String strCmd = "a = load 'foo' as (foo, fast); b = group a by foo; c = foreach b generate SUM(a.fast);\n";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test 
    public void testParsingWordWithAsInForeachWithOutBlock2() throws Throwable {
        PigServer server = new PigServer("MAPREDUCE");
        PigContext context = server.getPigContext();
        
        String strCmd = "cash = load 'foo' as (foo, fast); b = foreach cash generate fast * 2.0;\n";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }


    @Test 
    public void testParsingGenerateInForeachBlock() throws Throwable {
        PigServer server = new PigServer("MAPREDUCE");
        PigContext context = server.getPigContext();
        
        String strCmd = "a = load 'foo' as (foo, fast, regenerate); b = group a by foo; c = foreach b {generate a.regenerate;};\n";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test 
    public void testParsingGenerateInForeachWithOutBlock() throws Throwable {
        PigServer server = new PigServer("MAPREDUCE");
        PigContext context = server.getPigContext();
        
        String strCmd = "a = load 'foo' as (foo, fast, regenerate); b = group a by foo; c = foreach b generate a.regenerate;\n";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test 
    public void testParsingAsGenerateInForeachBlock() throws Throwable {
        PigServer server = new PigServer("MAPREDUCE");
        PigContext context = server.getPigContext();
        
        String strCmd = "a = load 'foo' as (foo, fast, regenerate); b = group a by foo; c = foreach b {generate {(1, '1', 0.4f),(2, '2', 0.45)} as b: bag{t(i: int, cease:chararray, degenerate: double)}, SUM(a.fast) as fast, a.regenerate as degenerated;};\n";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test 
    public void testParsingAsGenerateInForeachWithOutBlock() throws Throwable {
        PigServer server = new PigServer("MAPREDUCE");
        PigContext context = server.getPigContext();
        
        String strCmd = "a = load 'foo' as (foo, fast, regenerate); b = group a by foo; c = foreach b generate {(1, '1', 0.4f),(2, '2', 0.45)} as b: bag{t(i: int, cease:chararray, degenerate: double)}, SUM(a.fast) as fast, a.regenerate as degenerated;\n";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }
}
