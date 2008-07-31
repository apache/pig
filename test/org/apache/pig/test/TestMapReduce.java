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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import junit.framework.TestCase;

import org.junit.Test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigServer;
import org.apache.pig.StoreFunc;
import org.apache.pig.ExecType;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.COUNT;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.backend.datastorage.ElementDescriptor;
import org.junit.Before;

public class TestMapReduce extends TestCase {

    private Log log = LogFactory.getLog(getClass());
    
    MiniCluster cluster = MiniCluster.buildCluster();

    private PigServer pig;
    
    @Before
    @Override
    protected void setUp() throws Exception {
        pig = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
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
        String query = "foreach (group (load 'file:" + tmpFile + "') all) generate " + COUNT.class.getName() + "($1) ;";
        System.out.println(query);
        pig.registerQuery("asdf_id = " + query);
        Iterator it = pig.openIterator("asdf_id");
        tmpFile.delete();
        Tuple t = (Tuple)it.next();
        Double count = DataType.toDouble(t.get(0));
        assertEquals(count, (double)LOOP_COUNT);
    }
    
    static public class MyApply extends EvalFunc<DataBag> {
        String field0 = "Got";
        public MyApply() {}
        public MyApply(String field0) {
            this.field0 = field0;
        }
        @Override
        public DataBag exec(Tuple input) throws IOException {
            try {
                DataBag output = BagFactory.getInstance().newDefaultBag();
                Iterator<Tuple> it = (DataType.toBag(input.get(0))).iterator();
                while(it.hasNext()) {
                    Tuple t = it.next();
                    Tuple newT = TupleFactory.getInstance().newTuple(2);
                    newT.set(0, field0);
                    newT.set(1, t.get(0).toString());
                    output.add(newT);
                }

                return output;
            } catch (ExecException ee) {
                IOException ioe = new IOException(ee.getMessage());
                ioe.initCause(ee);
                throw ioe;
            }
        }
    }
    static public class MyGroup extends EvalFunc<Tuple> {
        @Override
        public Tuple exec(Tuple input) throws IOException{
            try {
                Tuple output = TupleFactory.getInstance().newTuple(1);
                output.set(0, new String("g"));
                return output;
            } catch (ExecException ee) {
                IOException ioe = new IOException(ee.getMessage());
                ioe.initCause(ee);
                throw ioe;
            }
        }
    }
    static public class MyStorage implements LoadFunc, StoreFunc {
        final static int COUNT = 10;
        int count = 0;
        public void bindTo(String fileName, BufferedPositionedInputStream is, long offset, long end) throws IOException {
        }
        public Tuple getNext() throws IOException {
            if (count < COUNT) {
                Tuple t = TupleFactory.getInstance().newTuple(Integer.toString(count++));
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
            try {
                os.write((f.toDelimitedString("-")+"\n").getBytes());            
            } catch (ExecException ee) {
                IOException ioe = new IOException(ee.getMessage());
                ioe.initCause(ee);
                throw ioe;
            }
        }

        public Boolean bytesToBoolean(byte[] b) throws IOException {
            return false;
        }
    
        public Integer bytesToInteger(byte[] b) throws IOException {
            return 0;
        }

        public Long bytesToLong(byte[] b) throws IOException {
            return 0L;
        }

        public Float bytesToFloat(byte[] b) throws IOException {
            return 0.0f;
        }

        public Double bytesToDouble(byte[] b) throws IOException {
            return 0.0;
        }

        public String bytesToCharArray(byte[] b) throws IOException {
            return "";
        }

        public Map<Object, Object> bytesToMap(byte[] b) throws IOException {
            return new HashMap<Object, Object>();
        }

        public Tuple bytesToTuple(byte[] b) throws IOException {
            return null;
        }

        public DataBag bytesToBag(byte[] b) throws IOException {
            return null;
        }

        public void fieldsToRead(Schema schema) {}

        public Schema determineSchema(URL fileName) throws IOException {
            return null;
        }

	    public byte[] toBytes(DataBag bag) throws IOException {
	        return null;
	    }
	
	    public byte[] toBytes(String s) throws IOException {
	        return s.getBytes();
	    }
	
	    public byte[] toBytes(Double d) throws IOException {
	        return d.toString().getBytes();
	    }
	
	    public byte[] toBytes(Float f) throws IOException {
	        return f.toString().getBytes();
	    }
	
	    public byte[] toBytes(Integer i) throws IOException {
	        return i.toString().getBytes();
	    }
	
	    public byte[] toBytes(Long l) throws IOException {
	        return l.toString().getBytes();
	    }
	
	    public byte[] toBytes(Map<Object, Object> m) throws IOException {
	        return m.toString().getBytes();
	    }
	
	    public byte[] toBytes(Tuple t) throws IOException {
	        return null;
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
        String query = "foreach (load 'file:"+tmpFile+"') generate $0,$1;";
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
        String query = "foreach (group (load 'file:"+tmpFile+"' using " + MyStorage.class.getName() + "()) by " + MyGroup.class.getName() + "('all')) generate flatten(" + MyApply.class.getName() + "($1)) ;";
        System.out.println(query);
        pig.registerQuery("asdf_id = " + query);
        Iterator it = pig.openIterator("asdf_id");
        tmpFile.delete();
        Tuple t;
        int count = 0;
        while(it.hasNext()) {
            t = (Tuple) it.next();
            assertEquals(t.get(0).toString(), "Got");
            Integer.parseInt(t.get(1).toString());
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
        pig.registerFunction("foo",
            new FuncSpec(MyApply.class.getName()+"('foo')"));
        String query = "foreach (group (load 'file:"+tmpFile+"' using " + MyStorage.class.getName() + "()) by " + MyGroup.class.getName() + "('all')) generate flatten(foo($1)) ;";
        System.out.println(query);
        pig.registerQuery("asdf_id = " + query);
        Iterator it = pig.openIterator("asdf_id");
        tmpFile.delete();
        Tuple t;
        int count = 0;
        while(it.hasNext()) {
            t = (Tuple) it.next();
            assertEquals("foo", t.get(0).toString());
            Integer.parseInt(t.get(1).toString());
            count++;
        }
        assertEquals(count, MyStorage.COUNT);
    }

    @Test
    public void testPigServer() throws Throwable {
        log.debug("creating pig server");
        PigContext pigContext = new PigContext(ExecType.MAPREDUCE, cluster.getProperties());
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
