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

import static org.apache.pig.PigServer.ExecType.MAPREDUCE;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.Properties;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.EvalFunc;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigServer;
import org.apache.pig.StoreFunc;
import org.apache.pig.backend.datastorage.ElementDescriptor;
import org.apache.pig.builtin.COUNT;
import org.apache.pig.data.DataAtom;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.io.FileLocalizer;
import org.junit.Before;
import org.junit.Test;

public class TestMapReduce extends TestCase {

    private Log log = LogFactory.getLog(getClass());
    
    MiniCluster cluster = MiniCluster.buildCluster();

    private PigServer pig;
    
    @Before
    @Override
    protected void setUp() throws Exception {
        pig = new PigServer(MAPREDUCE, cluster.getProperties());
    }

    @Test
    public void testBigGroupAll() throws Throwable {
        int LOOP_COUNT = 4*1024;
        File tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for(int i = 0; i < LOOP_COUNT; i++) {
            ps.println(i);
        }
        ps.close();
        String query = "foreach (group (load 'file:" + Util.encodeEscape(tmpFile.toString()) + "') all) generate " + COUNT.class.getName() + "($1) ;";
        System.out.println(query);
        pig.registerQuery("asdf_id = " + query);
        Iterator it = pig.openIterator("asdf_id");
        tmpFile.delete();
        Tuple t = (Tuple)it.next();
        Double count = t.getAtomField(0).numval();
        assertEquals(count, (double)LOOP_COUNT);
    }
    
    static public class MyApply extends EvalFunc<DataBag> {
    	String field0 = "Got";
    	public MyApply() {}
    	public MyApply(String field0) {
    		this.field0 = field0;
    	}
        @Override
		public void exec(Tuple input, DataBag output) throws IOException {
            Iterator<Tuple> it = (input.getBagField(0)).iterator();
            while(it.hasNext()) {
                Tuple t = it.next();
                Tuple newT = new Tuple(2);
                newT.setField(0, field0);
                newT.setField(1, t.getField(0).toString());
                output.add(newT);
            }
          
        }
    }
    static public class MyGroup extends EvalFunc<Tuple> {
        @Override
        public void exec(Tuple input, Tuple output) throws IOException{
            output.appendField(new DataAtom("g"));
        }
    }
    static public class MyStorage implements LoadFunc, StoreFunc {
        final static int COUNT = 10;
        int count = 0;
		public void bindTo(String fileName, BufferedPositionedInputStream is, long offset, long end) throws IOException {
        }
		public Tuple getNext() throws IOException {
            if (count < COUNT) {
                Tuple t = new Tuple(Integer.toString(count++));
                return t;
            }
            return null;
        }
        OutputStream os;
		public void bindTo(OutputStream os) throws IOException {
			this.os = os;
		}
		public void finish() throws IOException {
			
		}
		public void putNext(Tuple f) throws IOException {
			os.write((f.toDelimitedString("-")+"\n").getBytes());			
		}
    }
    @Test
    public void testStoreFunction() throws Throwable {
        File tmpFile = File.createTempFile("test", ".txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for(int i = 0; i < 10; i++) {
            ps.println(i+"\t"+i);
        }
        ps.close();
        String query = "foreach (load 'file:"+Util.encodeEscape(tmpFile.toString())+"') generate $0,$1;";
        System.out.println(query);
        pig.registerQuery("asdf_id = " + query);
        try {
        	pig.deleteFile("frog");
        } catch(Exception e) {}
        pig.store("asdf_id", "frog", MyStorage.class.getName()+"()");
        InputStream is = FileLocalizer.open("frog", pig.getPigContext());
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        String line;
        int i = 0;
        while((line = br.readLine()) != null) {
        	assertEquals(line, Integer.toString(i) + '-' + Integer.toString(i));
        	i++;
        }
        br.close();
        pig.deleteFile("frog");
    }
    @Test
    public void testQualifiedFuncions() throws Throwable {
        File tmpFile = File.createTempFile("test", ".txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for(int i = 0; i < 1; i++) {
            ps.println(i);
        }
        ps.close();
        String query = "foreach (group (load 'file:"+Util.encodeEscape(tmpFile.toString())+"' using " + MyStorage.class.getName() + "()) by " + MyGroup.class.getName() + "('all')) generate flatten(" + MyApply.class.getName() + "($1)) ;";
        System.out.println(query);
        pig.registerQuery("asdf_id = " + query);
        Iterator it = pig.openIterator("asdf_id");
        tmpFile.delete();
        Tuple t;
        int count = 0;
        while(it.hasNext()) {
            t = (Tuple) it.next();
            assertEquals(t.getField(0).toString(), "Got");
            Integer.parseInt(t.getField(1).toString());
            count++;
        }
        assertEquals(count, MyStorage.COUNT);
    }
    
    @Test
    public void testDefinedFunctions() throws Throwable {
        File tmpFile = File.createTempFile("test", ".txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for(int i = 0; i < 1; i++) {
            ps.println(i);
        }
        ps.close();
        pig.registerFunction("foo", MyApply.class.getName()+"('foo')");
        String query = "foreach (group (load 'file:"+Util.encodeEscape(tmpFile.toString())+"' using " + MyStorage.class.getName() + "()) by " + MyGroup.class.getName() + "('all')) generate flatten(foo($1)) ;";
        System.out.println(query);
        pig.registerQuery("asdf_id = " + query);
        Iterator it = pig.openIterator("asdf_id");
        tmpFile.delete();
        Tuple t;
        int count = 0;
        while(it.hasNext()) {
            t = (Tuple) it.next();
            assertEquals("foo", t.getField(0).toString());
            Integer.parseInt(t.getField(1).toString());
            count++;
        }
        assertEquals(count, MyStorage.COUNT);
    }

    @Test
    public void testPigServer() throws Throwable {
        log.debug("creating pig server");
        PigContext pigContext = new PigContext(MAPREDUCE, cluster.getProperties());
        PigServer pig = new PigServer(pigContext);
        System.out.println("testing capacity");
        long capacity = pig.capacity();
        assertTrue(capacity > 0);
        String sampleFileName = "/tmp/fileTest";
        if (!pig.existsFile(sampleFileName)) {
            ElementDescriptor path = pigContext.getDfs().asElement(sampleFileName);
            OutputStream os = path.create();
            os.write("Ben was here!".getBytes());
            os.close();
        }
        long length = pig.fileSize(sampleFileName);
        assertTrue(length > 0);
    }
}
