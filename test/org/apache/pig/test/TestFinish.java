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
import java.io.IOException;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.EvalFunc;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.FileLocalizer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestFinish extends TestCase {

    private PigServer pigServer;

    TupleFactory mTf = TupleFactory.getInstance();
    BagFactory mBf = BagFactory.getInstance();
    File f1;
    
    public static int gCount = 0;
    MiniCluster cluster = MiniCluster.buildCluster();
    
    static public class MyEvalFunction extends EvalFunc<Tuple>{
        int count = 0;
        
        String execType;
        String expectedFileName;
        /**
         * 
         */
        public MyEvalFunction(String execType, String expectedFileName) {
            this.execType = execType;
            this.expectedFileName = expectedFileName;
        }
        
        @Override
        public Tuple exec(Tuple input) throws IOException {
            ++count;
            return input;
        }

        @Override
        public void finish() {
            gCount = count;
            if(execType.equals("MAPREDUCE")) {
                try {
                    FileSystem fs = FileSystem.get(PigMapReduce.sJobConf);
                    fs.create(new Path(expectedFileName));
                } catch (IOException e) {
                    throw new RuntimeException("Unable to create file:" + expectedFileName);
                }
            } else if(execType.equals("LOCAL")){
                // XXX: FIXME need to handle this as part of fixing local mode for load-store-redesign
                // local mode will soon be hadoop local mode - if so - can the above code just be 
                // used as is and it would create a local file?
            }
            System.err.println("XXX: In finish, gCount is now:" + gCount);
        }
    }
    
    private String setUp(ExecType execType) throws Exception{
        String inputFileName;
        if(execType == ExecType.LOCAL) {
            pigServer = new PigServer(ExecType.LOCAL);
            f1 = File.createTempFile("test", "txt");
            f1.deleteOnExit();
            inputFileName = f1.getAbsolutePath();
            PrintStream ps = new PrintStream(new FileOutputStream(f1));
            for(int i = 0; i < 3; i++) {
                ps.println('a'+i + ":1");
            }
            ps.close();
        } else {
            pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
            f1 = File.createTempFile("test", "txt");
            f1.deleteOnExit();
            inputFileName = f1.getAbsolutePath();
            String input[] = new String[3];
            for(int i = 0; i < 3; i++) {
                input[i] = ('a'+i + ":1");
            }
            Util.createInputFile(cluster, inputFileName, input);
        }
        return inputFileName;
    }
    
    @Test
    public void testFinishInMapMR() throws Exception{
        gCount = 0;
        String inputFileName = setUp(ExecType.MAPREDUCE);
        // this file will be created on the cluster if finish() is called
        String expectedFileName = "testFinishInMapMR-finish.txt";
        pigServer.registerQuery("define MYUDF " + MyEvalFunction.class.getName() + "('MAPREDUCE','" + expectedFileName + "');");
        pigServer.registerQuery("a = load '" + inputFileName + "' using " + PigStorage.class.getName() + "(':');");
        pigServer.registerQuery("b = foreach a generate MYUDF" + "(*);");
        Iterator<Tuple> iter = pigServer.openIterator("b");
        int count = 0;
        while(iter.hasNext()){
            ++count;
            iter.next();
        }
        
        FileSystem fs = FileSystem.get(ConfigurationUtil.toConfiguration(cluster.getProperties()));
        assertTrue(fs.exists(new Path(expectedFileName)));
        Util.deleteFile(cluster, inputFileName);
        Util.deleteFile(cluster, expectedFileName);
    }
    
    @Test
    public void testFinishInReduceMR() throws Exception{
        gCount = 0;
        String inputFileName = setUp(ExecType.MAPREDUCE);
        // this file will be created on the cluster if finish() is called
        String expectedFileName = "testFinishInReduceMR-finish.txt";
        pigServer.registerQuery("define MYUDF " + MyEvalFunction.class.getName() + "('MAPREDUCE','" + expectedFileName + "');");
        pigServer.registerQuery("a = load '" + inputFileName + "' using " + PigStorage.class.getName() + "(':');");
        pigServer.registerQuery("a1 = group a by $1;");
        pigServer.registerQuery("b = foreach a1 generate MYUDF" + "(*);");
        Iterator<Tuple> iter = pigServer.openIterator("b");
        int count = 0;
        while(iter.hasNext()){
            ++count;
            iter.next();
        }
        
        FileSystem fs = FileSystem.get(ConfigurationUtil.toConfiguration(cluster.getProperties()));
        assertTrue(fs.exists(new Path(expectedFileName)));
        Util.deleteFile(cluster, inputFileName);
        Util.deleteFile(cluster, expectedFileName);
    }
    
    @Test
    public void testFinishInMapLoc() throws Exception{
        String inputFileName = setUp(ExecType.LOCAL);
        gCount = 0;
        // this file will be created on the cluster if finish() is called
        String expectedFileName = "testFinishInMapLoc-finish.txt";
        pigServer.registerQuery("define MYUDF " + MyEvalFunction.class.getName() + "('LOCAL','" + expectedFileName + "');");
        pigServer.registerQuery("a = load '" + inputFileName + "' using " + PigStorage.class.getName() + "(':');");
        pigServer.registerQuery("b = foreach a generate MYUDF" + "(*);");
        pigServer.openIterator("b");
        assertEquals(true, gCount==3);
    }
    
    @Test
    public void testFinishInReduceLoc() throws Exception{
        String inputFileName = setUp(ExecType.LOCAL);
        gCount = 0;
        // this file will be created on the cluster if finish() is called
        String expectedFileName = "testFinishInReduceLoc-finish.txt";
        pigServer.registerQuery("define MYUDF " + MyEvalFunction.class.getName() + "('LOCAL','" + expectedFileName + "');");
        pigServer.registerQuery("a = load '" + inputFileName + "' using " + PigStorage.class.getName() + "(':');");
        pigServer.registerQuery("a1 = group a by $1;");
        pigServer.registerQuery("b = foreach a1 generate MYUDF" + "(*);");
        pigServer.openIterator("b");
        assertEquals(true, gCount==1);
    }

    @After
    public void tearDown() throws Exception {
    }

}
