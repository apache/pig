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
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.BinStorage;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.*;
import org.apache.pig.ExecType;
import org.apache.pig.impl.io.PigFile;
import org.apache.pig.impl.PigContext;

public class TestPigFile extends TestCase {

    private final Log log = LogFactory.getLog(getClass());

    DataBag bag          = BagFactory.getInstance().newDefaultBag();
    Random rand = new Random();
    
    @Override
    @Before
    protected void setUp() throws Exception {

        log.info("Generating PigFile test data...");

        Random rand = new Random();
        byte[] r = new byte[10];

        Tuple t = TupleFactory.getInstance().newTuple(10);
        for (int i = 0, j = 0; i < 10000; i++, j++) {
            rand.nextBytes(r);
            if (j == 10) {
                bag.add(t);
                t = TupleFactory.getInstance().newTuple(10);
                j = 0;
            }
            t.set(j, new DataByteArray(r));

        }
        log.info("Done.");
    }

    @Override
    @After
    protected void tearDown() throws Exception {
    }

    @Test
    public void testStoreAndLoadText() throws IOException {
        PigContext pigContext = new PigContext(ExecType.LOCAL);
        
        log.info("Running Store...");
        String initialdata = File.createTempFile("pig-tmp", "").getAbsolutePath();
        PigFile store = new PigFile(initialdata);
        store.store(bag, new PigStorage(), pigContext);
        log.info("Done.");

        log.info("Running Load...");
        PigFile load = new PigFile(initialdata);
        DataBag loaded = load.load(new PigStorage(), pigContext);
        log.info("Done.");

        assertTrue(bag.size() == loaded.size());

        Iterator<Tuple> it1 = bag.iterator();
        Iterator<Tuple> it2 = loaded.iterator();
        while (it1.hasNext() && it2.hasNext()) {
            Tuple f1 = it1.next();
            Tuple f2 = it2.next();
            assertTrue(f1.equals(f2));
        }
        assertFalse(it1.hasNext() || it2.hasNext());
        new File(initialdata).delete();
    }

    private Object getRandomDatum(int nestingLevel) throws IOException{
        if (nestingLevel>3)
            return getRandomDataAtom();
        int i = rand.nextInt(4);
        switch(i){
        case 0: return getRandomDataAtom();
        case 1: return getRandomTuple(nestingLevel);
        case 2: return getRandomBag(20,nestingLevel);
        case 3: return getRandomMap(nestingLevel);
        }
        
        throw new RuntimeException("Shouldn't reach here.");
    }

    private char[] letters = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j',
        'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 
        'x', 'y', 'z'};
    
    private DataByteArray getRandomDataAtom(){
        /*
        byte[] bytes = new byte[10];
        rand.nextBytes(bytes);
        //return new DataByteArray(bytes);
        return new DataByteArray("Abc");
        */
        String s = new String();
        for (int i = 0; i < 10; i++) {
            s += letters[rand.nextInt(26)];
        }
        return new DataByteArray(s);

    }
    
    private Tuple getRandomTuple(int nestingLevel) throws IOException{
        
        try {
            int cardinality = rand.nextInt(2)+1;
            Tuple t = TupleFactory.getInstance().newTuple(cardinality);
            for (int i=0; i<cardinality; i++)
                t.set(i, getRandomDatum(nestingLevel+1));
            return t;
        } catch (ExecException ee) {
            IOException ioe = new IOException(ee.getMessage());
            ioe.initCause(ee);
            throw ioe;
        }
    }
    
    private DataBag getRandomBag(int maxCardinality, int nestingLevel) throws IOException{
        int cardinality = rand.nextInt(maxCardinality)+1;
        DataBag b = BagFactory.getInstance().newDefaultBag();
        for (int i=0; i<cardinality; i++){
            Tuple t = getRandomTuple(nestingLevel+1); 
            b.add(t);
        }
        return b;
        
    }
    
    private Map<Object, Object> getRandomMap(int nestingLevel) throws IOException{
        int cardinality = rand.nextInt(2)+1;
        Map<Object, Object> m = new HashMap<Object, Object>();
        for (int i=0; i<cardinality; i++){
            m.put(getRandomDataAtom().toString(),getRandomDatum(nestingLevel+1));
        }
        return m;
    }

    @Test
    public void testStoreAndLoadBin() throws IOException {
        log.info("Generating Data ...");
        bag = getRandomBag(5000,0);
        log.info("Done.");
        
        PigContext pigContext = new PigContext(ExecType.LOCAL);
        
        log.info("Running Store...");
        String storeFile = File.createTempFile("pig-tmp", "").getAbsolutePath();
        PigFile store = new PigFile(storeFile);
        store.store(bag, new BinStorage(), pigContext);
        log.info("Done.");

        log.info("Running Load...");
        PigFile load = new PigFile(storeFile);
        DataBag loaded = load.load(new BinStorage(), pigContext);
        log.info("Done.");

        assertTrue(bag.size() == loaded.size());

        Iterator<Tuple> it1 = bag.iterator();
        Iterator<Tuple> it2 = loaded.iterator();
        while (it1.hasNext() && it2.hasNext()) {
            Tuple f1 = it1.next();
            Tuple f2 = it2.next();
            assertTrue(f1.equals(f2));
        }
        assertFalse(it1.hasNext() || it2.hasNext());
        new File(storeFile).delete();
    }


    public void testLocalStore() throws Throwable {
        PigServer pig = new PigServer("local");
        File tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for(int i = 0; i < 10; i++) {
            ps.println(i);
        }
        ps.close();
        pig.registerQuery("a = load 'file:" + tmpFile+"';");
        pig.store("a", "/tmp/abc/xyz");
        
        tmpFile.delete();
        tmpFile = new File("/tmp/abc/xyz");
        tmpFile.delete();
        tmpFile = new File("/tmp/abc");
        tmpFile.delete();
        
    }
    

}
