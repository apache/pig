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
import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestSplitIndex {
    private PigServer pigServer;
    File inputDir;
    @Before
    public void setUp() throws Exception{
        pigServer = new PigServer(ExecType.LOCAL, new Properties());
        inputDir = File.createTempFile("tmp", "");
        inputDir.delete();
        inputDir.mkdir();
        Util.createLocalInputFile(inputDir.getAbsolutePath()+"/1", new String[] {"1\t2"});
        Util.createLocalInputFile(inputDir.getAbsolutePath()+"/2", new String[] {"3\t4"});
    }
    
    @Test
    public void testSplitIndex() throws Exception {
        pigServer.registerQuery("a = load '" + Util.encodeEscape(inputDir.toString()) + "' using " + SplitSensitiveLoadFunc.class.getName() + "();");
        Iterator<Tuple> iter = pigServer.openIterator("a");
        
        boolean file1exist=false, file2exist=false;
        Tuple t = iter.next();
        if (t.get(2).toString().endsWith("/1"))
            file1exist = true;
        if (t.get(2).toString().endsWith("/2"))
            file2exist = true;
        t = iter.next();
        if (t.get(2).toString().endsWith("/1"))
            file1exist = true;
        if (t.get(2).toString().endsWith("/2"))
            file2exist = true;
        if (!file1exist || !file2exist)
            Assert.fail();
    }
    
    @Test
    public void testSplitIndexNoCombine() throws Exception {
        pigServer.getPigContext().getProperties().setProperty("pig.splitCombination", "false");
        pigServer.registerQuery("a = load '" + Util.encodeEscape(inputDir.toString()) + "' using " + SplitSensitiveLoadFunc.class.getName() + "();");
        Iterator<Tuple> iter = pigServer.openIterator("a");
        
        boolean file1exist=false, file2exist=false;
        Tuple t = iter.next();
        if (t.get(2).toString().endsWith("/1"))
            file1exist = true;
        if (t.get(2).toString().endsWith("/2"))
            file2exist = true;
        t = iter.next();
        if (t.get(2).toString().endsWith("/1"))
            file1exist = true;
        if (t.get(2).toString().endsWith("/2"))
            file2exist = true;
        if (!file1exist || !file2exist)
            Assert.fail();
    }
    
    public static class SplitSensitiveLoadFunc extends PigStorage {
        Path path = null;
        public SplitSensitiveLoadFunc() {
            super();
        }
        @Override
        public void prepareToRead(RecordReader reader, PigSplit split) {
            in = reader;
            path = ((FileSplit)split.getWrappedSplit()).getPath();
        }
        
        @Override
        public Tuple getNext() throws IOException {
            Tuple myTuple = super.getNext();
            if (myTuple != null)
                myTuple.append(path.toString());
            return myTuple;
        }
    }
}
