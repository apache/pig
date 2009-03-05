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

import org.apache.pig.EvalFunc;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
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
    
    static public class MyEvalFunction extends EvalFunc<Tuple>{
        int count = 0;
        
        @Override
        public Tuple exec(Tuple input) throws IOException {
            ++count;
            return input;
        }

        @Override
        public void finish() {
            gCount = count;
        }
    }
    
    @Before
    @Override
    public void setUp() throws Exception{
        pigServer = new PigServer(ExecType.MAPREDUCE);
        f1 = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(f1));
        for(int i = 0; i < 3; i++) {
            ps.println('a'+i + ":1");
        }
        ps.close();
    }
    
    @Test
    public void testFinishInMapMR() throws Exception{
        gCount = 0;
        pigServer.registerQuery("a = load '" + Util.generateURI(f1.toString()) + "' using " + PigStorage.class.getName() + "(':');");
        pigServer.registerQuery("b = foreach a generate " + MyEvalFunction.class.getName() + "(*);");
        Iterator<Tuple> iter = pigServer.openIterator("b");
        int count = 0;
        while(iter.hasNext()){
            ++count;
            iter.next();
        }
        
        System.out.println(count + ", " + gCount);
        assertEquals(true, gCount==3);
    }
    
    @Test
    public void testFinishInReduceMR() throws Exception{
        gCount = 0;
        pigServer.registerQuery("a = load '" + Util.generateURI(f1.toString()) + "' using " + PigStorage.class.getName() + "(':');");
        pigServer.registerQuery("a1 = group a by $1;");
        pigServer.registerQuery("b = foreach a1 generate " + MyEvalFunction.class.getName() + "(*);");
        Iterator<Tuple> iter = pigServer.openIterator("b");
        int count = 0;
        while(iter.hasNext()){
            ++count;
            iter.next();
        }
        
        System.out.println(count + ", " + gCount);
        assertEquals(true, gCount==1);
    }
    
    @Test
    public void testFinishInMapLoc() throws Exception{
        pigServer = new PigServer(ExecType.LOCAL);
        gCount = 0;
        pigServer.registerQuery("a = load '" + Util.generateURI(f1.toString()) + "' using " + PigStorage.class.getName() + "(':');");
        pigServer.registerQuery("b = foreach a generate " + MyEvalFunction.class.getName() + "(*);");
        pigServer.openIterator("b");
        assertEquals(true, gCount==3);
    }
    
    @Test
    public void testFinishInReduceLoc() throws Exception{
        pigServer = new PigServer(ExecType.LOCAL);
        gCount = 0;
        pigServer.registerQuery("a = load '" + Util.generateURI(f1.toString()) + "' using " + PigStorage.class.getName() + "(':');");
        pigServer.registerQuery("a1 = group a by $1;");
        pigServer.registerQuery("b = foreach a1 generate " + MyEvalFunction.class.getName() + "(*);");
        pigServer.openIterator("b");
        assertEquals(true, gCount==1);
    }

    @After
    public void tearDown() throws Exception {
    }

}
