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
package org.apache.pig.penny.test;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStreamReader;
import java.io.PrintStream;

import org.apache.pig.ExecType;
import org.apache.pig.penny.PennyServer;
import org.apache.pig.test.MiniCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * These tests run some of the applications against a minicluster. These constitute a full
 * system test (in a box) for penny.
 */
public class PennyAgentTest {
    static {
        System.setProperty("hadoop.log.dir", "/tmp");
    }
    static MiniCluster cluster = MiniCluster.buildCluster();
    static File script;
    static File output;
    
    static void recursiveDelete(File f) {
        if (f.isDirectory()) {
            for(File c: f.listFiles()) {
                recursiveDelete(c);
            }
        }
        if (f.exists()) {
            f.delete();
        }
    }
    @Test
    public void testDH() throws Exception {
        recursiveDelete(output);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        PrintStream oldOs = System.out;
        try {
            System.setOut(new PrintStream(bos));
            org.apache.pig.penny.apps.dh.Main.main(new String[] { script.getAbsolutePath(), "c", "1", "0", "10000", "10" });
        } finally {
            System.setOut(oldOs);
        }
        int sampleCount = 0;
        ByteArrayInputStream bais = new ByteArrayInputStream(bos.toByteArray());
        BufferedReader br = new BufferedReader(new InputStreamReader(bais));
        String line;
        while((line = br.readLine()) != null) {
            if (line.startsWith("Histogram:")) {
                sampleCount++;
            }
        }
        Assert.assertEquals(1, sampleCount);
    }
    
    @Test
    public void testDS() throws Exception {
        recursiveDelete(output);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        PrintStream oldOs = System.out;
        try {
            System.setOut(new PrintStream(bos));
            org.apache.pig.penny.apps.ds.Main.main(new String[] { script.getAbsolutePath() });
        } finally {
            System.setOut(oldOs);
        }
        int sampleCount = 0;
        ByteArrayInputStream bais = new ByteArrayInputStream(bos.toByteArray());
        BufferedReader br = new BufferedReader(new InputStreamReader(bais));
        String line;
        while((line = br.readLine()) != null) {
            if (line.startsWith("*** ")) {
                sampleCount++;
            }
        }
        Assert.assertEquals(15, sampleCount);
    }
    
    @Test
    public void testRI() throws Exception {
        recursiveDelete(output);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        PrintStream oldOs = System.out;
        try {
            System.setOut(new PrintStream(bos));
            org.apache.pig.penny.apps.ri.Main.main(new String[] { script.getAbsolutePath(), "a", "1" });
        } finally {
            System.setOut(oldOs);
        }
        int sampleCount = 0;
        ByteArrayInputStream bais = new ByteArrayInputStream(bos.toByteArray());
        BufferedReader br = new BufferedReader(new InputStreamReader(bais));
        String line;
        while((line = br.readLine()) != null) {
            if (line.length() > 0) {
                sampleCount++;
            }
        }
        Assert.assertEquals(0, sampleCount);
    }
    
    @BeforeClass
    public static void oneTimeSetUp() throws Exception {
        script = File.createTempFile("script", ".pig");
        script.deleteOnExit();
        File input = File.createTempFile("data", ".dat");
        input.deleteOnExit();
        PrintStream ps = new PrintStream(input);
        for(int i= 0; i < 1024*1024; i++) {
            ps.print(i + "\tval" + ((int)Math.sqrt(i)) + "\n");
        }
        ps.close();
        output = File.createTempFile("output", ".dat");
        output.deleteOnExit();
        ps = new PrintStream(script);
        ps.println("a = load 'file://" + input.getAbsolutePath() + "';");
        ps.println("b = group a by $1;");
        ps.println("c = foreach b generate group, COUNT($1);");
        ps.println("store c into 'file://" + output.getAbsolutePath() + "';");
        ps.close();
        PennyServer.setExecType(ExecType.MAPREDUCE);
        PennyServer.setProperties(cluster.getProperties());
        System.out.println(cluster.getProperties());
    }
    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cluster.shutDown();
    }
}
