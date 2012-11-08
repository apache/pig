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
import java.io.FileOutputStream;
import java.io.PrintStream;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class TestSplitStore {
    private PigServer pig;
    private PigContext pigContext;
    private File tmpFile;
    private static MiniCluster cluster = MiniCluster.buildCluster();

    @Before
    public void setUp() throws Exception {
        pig = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        pigContext = pig.getPigContext();
        int LOOP_SIZE = 20;
        tmpFile = File.createTempFile("test", "txt");
        tmpFile.deleteOnExit();
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for(int i = 1; i <= LOOP_SIZE; i++) {
            ps.println(i);
        }
        ps.close();
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cluster.shutDown();
    }

    @Test
    public void test1() throws Exception{
        pig.registerQuery("A = LOAD '"
                + Util.generateURI(tmpFile.toString(), pig.getPigContext()) + "';");
        pig.registerQuery("Split A into A1 if $0<=10, A2 if $0>10;");
        pig.store("A1", "'" + FileLocalizer.getTemporaryPath(pigContext) + "'");
        pig.store("A2", "'" + FileLocalizer.getTemporaryPath(pigContext) + "'");
    }

    @Test
    public void test2() throws Exception{
        pig.registerQuery("A = LOAD '"
                + Util.generateURI(tmpFile.toString(), pig.getPigContext()) + "';");
        pig.registerQuery("Split A into A1 if $0<=10, A2 if $0>10;");
        pig.openIterator("A1");
        pig.store("A2", "'" + FileLocalizer.getTemporaryPath(pigContext) + "'");
    }

    @Test
    public void test3() throws Exception{
        pig.registerQuery("A = LOAD '"
                + Util.generateURI(tmpFile.toString(), pig.getPigContext()) + "';");
        pig.registerQuery("Split A into A1 if $0<=10, A2 if $0>10;");
        pig.openIterator("A2");
        pig.store("A1", "'" + FileLocalizer.getTemporaryPath(pigContext) + "'");
    }

    @Test
    public void test4() throws Exception{
        pig.registerQuery("A = LOAD '"
                + Util.generateURI(tmpFile.toString(), pig.getPigContext()) + "';");
        pig.registerQuery("Split A into A1 if $0<=10, A2 if $0>10;");
        pig.store("A1", "'" + FileLocalizer.getTemporaryPath(pigContext) + "'");
        pig.openIterator("A2");
    }

    @Test
    public void test5() throws Exception{
        pig.registerQuery("A = LOAD '"
                + Util.generateURI(tmpFile.toString(), pig.getPigContext()) + "';");
        pig.registerQuery("Split A into A1 if $0<=10, A2 if $0>10;");
        pig.store("A2", "'" + FileLocalizer.getTemporaryPath(pigContext) + "'");
        pig.openIterator("A1");
    }

    @Test
    public void test6() throws Exception{
        pig.registerQuery("A = LOAD '"
                + Util.generateURI(tmpFile.toString(), pig.getPigContext()) + "';");
        pig.registerQuery("Split A into A1 if $0<=10, A2 if $0>10;");
        pig.openIterator("A1");
        pig.registerQuery("Store A2 into '" + FileLocalizer.getTemporaryPath(pigContext) + "';");
    }

    @Test
    public void test7() throws Exception{
        pig.registerQuery("A = LOAD '"
                + Util.generateURI(tmpFile.toString(), pig.getPigContext()) + "';");
        pig.registerQuery("Split A into A1 if $0<=10, A2 if $0>10;");
        pig.openIterator("A2");
        pig.registerQuery("Store A1 into '" + FileLocalizer.getTemporaryPath(pigContext) + "';");
    }

    @Test
    public void test8() throws Exception{
        pig.registerQuery("A = LOAD '"
                + Util.generateURI(tmpFile.toString(), pig.getPigContext()) + "';");
        pig.registerQuery("Split A into A1 if $0<=10, A2 if $0>10;");
        pig.registerQuery("Store A1 into '" + FileLocalizer.getTemporaryPath(pigContext) + "';");
        pig.openIterator("A2");
    }

    @Test
    public void test9() throws Exception{
        pig.registerQuery("A = LOAD '"
                + Util.generateURI(tmpFile.toString(), pig.getPigContext()) + "';");
        pig.registerQuery("Split A into A1 if $0<=10, A2 if $0>10;");
        pig.registerQuery("Store A2 into '" + FileLocalizer.getTemporaryPath(pigContext) + "';");
        pig.openIterator("A1");
    }
}