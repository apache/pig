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
package org.apache.pig.piggybank.test.evaluation.string;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.data.Tuple;
import org.apache.pig.test.MiniCluster;
import org.apache.pig.test.Util;
import org.junit.Test;

import junit.framework.TestCase;

public class TestLookupInFiles extends TestCase {
    MiniCluster cluster = MiniCluster.buildCluster();
    private PigServer pigServer;
   
    @Override
    public void setUp() throws Exception{
        pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
    }
    @Test
    public void testLookupInFiles() throws Exception {
        File tmpFile = File.createTempFile("test", ".txt");
        PrintStream ps1 = new PrintStream(new FileOutputStream(tmpFile));
        
        ps1.println("one");
        ps1.println("notexist");
        ps1.println("three");
        ps1.close();
        
        File lookupFile1 = File.createTempFile("lookup", ".txt");
        PrintStream lps1 = new PrintStream(new FileOutputStream(lookupFile1));
        
        lps1.println("one");
        lps1.println("two");
        lps1.println("three");
        lps1.close();
        
        File lookupFile2 = File.createTempFile("lookup", "txt");
        PrintStream lps2 = new PrintStream(new FileOutputStream(lookupFile2));
        
        lps2.println("one");
        lps2.println("ten");
        lps2.println("eleven");
        lps2.close();
        
        FileSystem fs = FileSystem.get(ConfigurationUtil.toConfiguration(pigServer.getPigContext().getProperties()));
        fs.copyFromLocalFile(new Path(lookupFile1.toString()), new Path("lookup1"));
        fs.copyFromLocalFile(new Path(lookupFile1.toString()), new Path("lookup2"));
        pigServer.registerQuery("A = LOAD '" + Util.generateURI(tmpFile.toString(), pigServer.getPigContext()) + "' AS (key:chararray);");
        pigServer.registerQuery("B = FOREACH A GENERATE org.apache.pig.piggybank.evaluation.string.LookupInFiles(key, 'lookup1', 'lookup2');");
        Iterator<Tuple> iter = pigServer.openIterator("B");
        
        int r = (Integer)iter.next().get(0);
        assertTrue(r==1);
        r = (Integer)iter.next().get(0);
        assertTrue(r==0);
    }
}
