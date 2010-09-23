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

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import java.io.BufferedReader;
import java.io.FileReader;


import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.TFileStorage;
import org.apache.pig.impl.io.InterStorage;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;

public class TestTmpFileCompression {
    private PigServer pigServer;
    static MiniCluster cluster = MiniCluster.buildCluster();
    File logFile;

    @Before
    public void setUp() throws Exception {
        pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
    }

    private void resetLog(Class clazz) throws Exception {
        if (logFile != null)
            logFile.delete();
        Logger logger = Logger.getLogger(clazz);
        logger.removeAllAppenders();
        logger.setLevel(Level.DEBUG);
        SimpleLayout layout = new SimpleLayout();
        logFile = File.createTempFile("log", "");
        FileAppender appender = new FileAppender(layout, logFile.toString(),
                        false, false, 0);
        logger.addAppender(appender);
    }

    public boolean checkLogFileMessage(String[] messages) {
        BufferedReader reader = null;

        try {
            reader = new BufferedReader(new FileReader(logFile));
            String logMessage = "";
            String line;
            while ((line = reader.readLine()) != null) {
                logMessage = logMessage + line + "\n";
            }
            for (int i = 0; i < messages.length; i++) {
                if (!logMessage.contains(messages[i])) return false;
            }
            return true;
        }
        catch (IOException e) {
            return false;
        }
    }

    @After
    public void tearDown() throws Exception {
        if (logFile != null)
            logFile.delete();
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cluster.shutDown();
    }

    @Test
    public void testImplicitSplitUncompressed() throws Exception {
        resetLog(InterStorage.class);
        int LOOP_SIZE = 20;
        String[] input = new String[LOOP_SIZE];
        for (int i = 1; i <= LOOP_SIZE; i++) {
            input[i - 1] = Integer.toString(i);
        }
        String inputFileName = "testImplicitSplit-input.txt";
        Util.createInputFile(cluster, inputFileName, input);
        pigServer.registerQuery("A = LOAD '" + inputFileName + "';");
        pigServer.registerQuery("B = filter A by $0<=10;");
        pigServer.registerQuery("C = filter A by $0>10;");
        pigServer.registerQuery("D = union B,C;");
        Iterator<Tuple> iter = pigServer.openIterator("D");
        if (!iter.hasNext()) fail("No Output received");
        int cnt = 0;
        while (iter.hasNext()) {
            Tuple t = iter.next();
            ++cnt;
        }
        assertEquals(20, cnt);
        Util.deleteFile(cluster, inputFileName);
        assertTrue(checkLogFileMessage(new String[] {
            "Pig Internal storage in use"
        }));
    }

    @Test
    public void testImplicitSplitInCoGroupUncompressed() throws Exception {
        // this query is similar to the one reported in JIRA - PIG-537
        // Create input file
        resetLog(InterStorage.class);
        String input1 = "testImplicitSplitInCoGroup-input1.txt";
        String input2 = "testImplicitSplitInCoGroup-input2.txt";
        Util.createInputFile(cluster, input1, new String[] {
                        "a:1", "b:2", "b:20", "c:3", "c:30"
        });
        Util.createInputFile(cluster, input2, new String[] {
                        "a:first", "b:second", "c:third"
        });
        pigServer.registerQuery("a = load '" + input1 + "' using PigStorage(':') as (name:chararray, marks:int);");
        pigServer.registerQuery("b = load '" + input2 + "' using PigStorage(':') as (name:chararray, rank:chararray);");
        pigServer.registerQuery("c = cogroup a by name, b by name;");
        pigServer.registerQuery("d = foreach c generate group, FLATTEN(a.marks) as newmarks;");
        pigServer.registerQuery("e = cogroup a by marks, d by newmarks;");
        pigServer.registerQuery("f = foreach e generate group, flatten(a), flatten(d);");
        HashMap<Integer, Object[]> results = new HashMap<Integer, Object[]>();
        results.put(1, new Object[] {
                        "a", 1, "a", 1
        });
        results.put(2, new Object[] {
                        "b", 2, "b", 2
        });
        results.put(3, new Object[] {
                        "c", 3, "c", 3
        });
        results.put(20, new Object[] {
                        "b", 20, "b", 20
        });
        results.put(30, new Object[] {
                        "c", 30, "c", 30
        });

        Iterator<Tuple> it = pigServer.openIterator("f");
        while (it.hasNext()) {
            Tuple t = it.next();
            System.err.println("Tuple:" + t);
            Integer group = (Integer) t.get(0);
            Object[] groupValues = results.get(group);
            for (int i = 0; i < 4; i++) {
                assertEquals(groupValues[i], t.get(i + 1));
            }
        }
        Util.deleteFile(cluster, input1);
        Util.deleteFile(cluster, input2);
        assertTrue(checkLogFileMessage(new String[] {
            "Pig Internal storage in use"
        }));
    }

    @Test
    public void testImplicitSplit() throws Exception {
        resetLog(TFileStorage.class);
        int LOOP_SIZE = 20;
        String[] input = new String[LOOP_SIZE];
        for (int i = 1; i <= LOOP_SIZE; i++) {
            input[i - 1] = Integer.toString(i);
        }
        String inputFileName = "testImplicitSplit-input.txt";
        Util.createInputFile(cluster, inputFileName, input);
        pigServer.getPigContext().getProperties().setProperty(
                        "pig.tmpfilecompression", "true");
        pigServer.getPigContext().getProperties().setProperty(
                        "pig.tmpfilecompression.codec", "gz");
        pigServer.registerQuery("A = LOAD '" + inputFileName + "';");
        pigServer.registerQuery("B = filter A by $0<=10;");
        pigServer.registerQuery("C = filter A by $0>10;");
        pigServer.registerQuery("D = union B,C;");
        Iterator<Tuple> iter = pigServer.openIterator("D");
        if (!iter.hasNext()) fail("No Output received");
        int cnt = 0;
        while (iter.hasNext()) {
            Tuple t = iter.next();
            ++cnt;
        }
        assertEquals(20, cnt);
        Util.deleteFile(cluster, inputFileName);
        assertTrue(checkLogFileMessage(new String[] {
            "TFile storage in use"
        }));
    }

    @Test
    public void testImplicitSplitInCoGroup() throws Exception {
        // this query is similar to the one reported in JIRA - PIG-537
        // Create input file
        resetLog(TFileStorage.class);
        String input1 = "testImplicitSplitInCoGroup-input1.txt";
        String input2 = "testImplicitSplitInCoGroup-input2.txt";
        Util.createInputFile(cluster, input1, new String[] {
                        "a:1", "b:2", "b:20", "c:3", "c:30"
        });
        Util.createInputFile(cluster, input2, new String[] {
                        "a:first", "b:second", "c:third"
        });
        pigServer.getPigContext().getProperties().setProperty(
                        "pig.tmpfilecompression", "true");
        pigServer.getPigContext().getProperties().setProperty(
                        "pig.tmpfilecompression.codec", "gz");
        pigServer.registerQuery("a = load '" + input1 + "' using PigStorage(':') as (name:chararray, marks:int);");
        pigServer.registerQuery("b = load '" + input2 + "' using PigStorage(':') as (name:chararray, rank:chararray);");
        pigServer.registerQuery("c = cogroup a by name, b by name;");
        pigServer.registerQuery("d = foreach c generate group, FLATTEN(a.marks) as newmarks;");
        pigServer.registerQuery("e = cogroup a by marks, d by newmarks;");
        pigServer.registerQuery("f = foreach e generate group, flatten(a), flatten(d);");
        HashMap<Integer, Object[]> results = new HashMap<Integer, Object[]>();
        results.put(1, new Object[] {
                        "a", 1, "a", 1
        });
        results.put(2, new Object[] {
                        "b", 2, "b", 2
        });
        results.put(3, new Object[] {
                        "c", 3, "c", 3
        });
        results.put(20, new Object[] {
                        "b", 20, "b", 20
        });
        results.put(30, new Object[] {
                        "c", 30, "c", 30
        });

        Iterator<Tuple> it = pigServer.openIterator("f");
        while (it.hasNext()) {
            Tuple t = it.next();
            System.err.println("Tuple:" + t);
            Integer group = (Integer) t.get(0);
            Object[] groupValues = results.get(group);
            for (int i = 0; i < 4; i++) {
                assertEquals(groupValues[i], t.get(i + 1));
            }
        }
        Util.deleteFile(cluster, input1);
        Util.deleteFile(cluster, input2);
        assertTrue(checkLogFileMessage(new String[] {
            "TFile storage in use"
        }));
    }

}
