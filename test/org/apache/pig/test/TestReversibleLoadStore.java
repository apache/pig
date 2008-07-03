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
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map; 
import java.util.HashMap;

import junit.framework.TestCase;

import org.apache.pig.LoadFunc;
import org.apache.pig.PigServer;
import org.apache.pig.StoreFunc;
import org.apache.pig.ReversibleLoadStoreFunc;
import org.apache.pig.PigServer.ExecType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.BufferedPositionedInputStream;

public class TestReversibleLoadStore extends PigExecTestCase {

    static List<Tuple> _storedTuples = new ArrayList<Tuple>();
    
    public void testNoReuse() throws Exception {
        
        DummyLoadFunc.readCounterMap = null ;
        DummyStoreFunc.writeCounter = 0 ;     
        
        File tmpFile = createTempFile() ;
        
        PigServer pig = new PigServer(ExecType.LOCAL);
        pig.registerQuery("A = LOAD 'file:" + Util.encodeEscape(tmpFile.getAbsolutePath().toString()) + "' USING "
                        + DummyLoadFunc.class.getName() + "();");

        String file1 = "/tmp/testPigOutput" ;
        if (pig.existsFile(file1)) {
            pig.deleteFile(file1) ;
        }
        
        pig.store("A", file1, DummyStoreFunc.class.getName() + "()");
        
        String file2 = "/tmp/testPigOutput2" ;
        if (pig.existsFile(file2)) {
            pig.deleteFile(file2) ;
        }
        pig.store("A", file2, DummyStoreFunc.class.getName() + "()");
        
        // for this test the plan will not be reused so:-
        // - initial temp file has to be read 10 times 
        // - DummyLoadStoreFunc has to be written 10 times
        
        assertEquals(10, DummyLoadFunc.readCounterMap.get("file:"+tmpFile.getAbsolutePath()).intValue()) ;
        assertEquals(10, DummyStoreFunc.writeCounter) ;
        
        pig.deleteFile(file1) ;
        pig.deleteFile(file2) ;
        
    }
    
    public void testReuse() throws Exception {
        
        DummyLoadStoreFunc.readCounterMap = null ;
        DummyLoadStoreFunc.writeCounter = 0 ;     
        
        File tmpFile = createTempFile() ;
        
        PigServer pig = new PigServer(ExecType.LOCAL);
        pig.registerQuery("A = LOAD 'file:" + Util.encodeEscape(tmpFile.getAbsolutePath().toString()) + "' USING "
                        + DummyLoadStoreFunc.class.getName() + "();");

        String file1 = "/tmp/testPigOutput" ;
        if (pig.existsFile(file1)) {
            pig.deleteFile(file1) ;
        }
        
        pig.store("A", file1, DummyLoadStoreFunc.class.getName() + "()");
        
        String file2 = "/tmp/testPigOutput2" ;
        if (pig.existsFile(file2)) {
            pig.deleteFile(file2) ;
        }
        pig.store("A", file2, DummyLoadStoreFunc.class.getName() + "()");
        
        // for this test the plan will be reused so:-
        // - initial temp file has to be read 5 times 
        // - the output of the first execution has to be read 5 times
        // - DummyLoadStoreFunc has to be written 10 times
        
        assertEquals(5, DummyLoadStoreFunc.readCounterMap.get("file:"+tmpFile.getAbsolutePath()).intValue()) ;
        assertEquals(5, DummyLoadStoreFunc.readCounterMap.get("/tmp/testPigOutput").intValue()) ;
        assertEquals(10, DummyLoadStoreFunc.writeCounter) ;
        
        
        pig.deleteFile(file1) ;
        pig.deleteFile(file2) ;
        
    }
    
    private File createTempFile() throws Exception {
        File tmpFile =  File.createTempFile("test", ".txt");
        if (tmpFile.exists()) {
            tmpFile.delete() ;
        }
        PrintWriter pw = new PrintWriter(tmpFile) ;
        pw.println("1,11,111,1111") ;
        pw.println("2,22,222,2222") ;
        pw.println("3,33,333,3333") ;
        pw.println("4,4,444,4444") ;
        pw.println("5,55,555,5555") ;
        pw.close() ;
        tmpFile.deleteOnExit() ;
        return tmpFile ;
    }
    
    public static class DummyLoadStoreFunc implements ReversibleLoadStoreFunc {
        
       public static Map<String,Integer> readCounterMap = null ;
        
        protected BufferedPositionedInputStream in = null;
        private String fileName = null ;
        
        public void bindTo(String inputfileName, BufferedPositionedInputStream is,
                long offset, long end) throws IOException {
            in = is ;
            fileName = inputfileName ;
        }

        public Tuple getNext() throws IOException {
            String line = in.readLine(Charset.forName("UTF8"), (byte) '\n') ;
            if (line == null) {
                return null ;
            }
            // else
            
            if (readCounterMap == null) {
                readCounterMap = new HashMap<String,Integer>() ;
            }
            
            if (readCounterMap.get(fileName) == null) {
                readCounterMap.put(fileName, 1) ;
            }
            else {
                readCounterMap.put(fileName, readCounterMap.get(fileName) + 1) ;
            }
            
            return new Tuple(line, ",");
        }
        
        public static int writeCounter = 0 ;
        private PrintWriter pw = null ;
        
        public void bindTo(OutputStream os) throws IOException {
            pw = new PrintWriter(os) ;
        }

        public void finish() throws IOException {
            pw.close() ;
        }

        public void putNext(Tuple tuple) throws IOException {
            writeCounter++ ;
            pw.println(tuple.toDelimitedString(","));            
        }
        
    }
    
    public static class DummyLoadFunc implements LoadFunc {

        public static Map<String,Integer> readCounterMap = null ;
        
        protected BufferedPositionedInputStream in = null;
        private String fileName = null ;
        
        public void bindTo(String inputfileName, BufferedPositionedInputStream is,
                long offset, long end) throws IOException {
            in = is ;
            fileName = inputfileName ;
        }

        public Tuple getNext() throws IOException {
            String line = in.readLine(Charset.forName("UTF8"), (byte) '\n') ;
            if (line == null) {
                return null ;
            }
            // else
            
            if (readCounterMap == null) {
                readCounterMap = new HashMap<String,Integer>() ;
            }
            
            if (readCounterMap.get(fileName) == null) {
                readCounterMap.put(fileName, 1) ;
            }
            else {
                readCounterMap.put(fileName, readCounterMap.get(fileName) + 1) ;
            }
            
            return new Tuple(line, ",");
        }

    }
    
    public static class DummyStoreFunc implements StoreFunc {
        
        public static int writeCounter = 0 ;
        private PrintWriter pw = null ;
        
        public void bindTo(OutputStream os) throws IOException {
            pw = new PrintWriter(os) ;
        }

        public void finish() throws IOException {
            pw.close() ;
        }

        public void putNext(Tuple tuple) throws IOException {
            writeCounter++ ;
            pw.println(tuple.toDelimitedString(","));            
        }
    }

}
