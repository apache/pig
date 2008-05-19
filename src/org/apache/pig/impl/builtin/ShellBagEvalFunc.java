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
package org.apache.pig.impl.builtin;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DefaultAbstractBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.backend.executionengine.ExecException;


public class ShellBagEvalFunc extends EvalFunc<DataBag> {
    private final Log log = LogFactory.getLog(getClass());
    byte groupDelim = '\n';
    byte recordDelim = '\n';
    byte fieldDelim = '\t';
    String fieldDelimString = "\t";
    OutputStream os;
    InputStream is;
    InputStream es;
    String cmd;
    Thread processThread;
    BagFactory mBagFactory = BagFactory.getInstance();
    TupleFactory mTupleFactory = TupleFactory.getInstance();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    private ArrayList<Object> mProtoTuple = new ArrayList<Object>();
    
    LinkedBlockingQueue<DataBag> bags = new LinkedBlockingQueue<DataBag>();
    
    
    public ShellBagEvalFunc(String cmd) {
        this.cmd = cmd;
    }

    private class EndOfQueue extends DefaultAbstractBag {
        @Override
        public void add(Tuple t){}

        // To satisfy abstract functions in DataBag.
        public boolean isSorted() { return false; }
        public boolean isDistinct() { return false; }
        public Iterator<Tuple> iterator() { return null; }
        public long spill() { return 0; }
    }
    
    private void startProcess() throws IOException {
        Process p = Runtime.getRuntime().exec(cmd);
        is = p.getInputStream();
        os = p.getOutputStream();
        es = p.getErrorStream();
        
        
        new Thread() {
            @Override
            public void run() {
                byte b[] = new byte[256];
                int rc;
                try {
                    while((rc = es.read(b)) > 0) {
                        System.err.write(b, 0, rc);
                    }
                } catch(Exception e) {
                    log.error(e);
                }
            }
        }.start();
        
        
        processThread = new Thread() {
            @Override
            public void run() {
                while(true){
                    DataBag bag;
                    try{
                        bag = bags.take();
                    }catch(InterruptedException e){
                        continue;
                    }
                    if (bag instanceof EndOfQueue)
                        break;
                    try {
                        readBag(bag);
                    } catch (IOException e) {
                        log.error(e);
                    }
                }
            }
        };
        
        processThread.start();
    }
    
    @Override
    public DataBag exec(Tuple input) throws IOException {
        DataBag output = mBagFactory.newDefaultBag();
        if (os == null) {
            startProcess();
        }
        try {
            os.write(input.toDelimitedString(fieldDelimString).getBytes());
            os.write(recordDelim);
            os.write(groupDelim);
            os.flush();
        } catch (ExecException ee) {
            IOException ioe = new IOException(ee.getMessage());
            ioe.initCause(ee);
            throw ioe;
        }

        try{
            bags.put(output);
        }catch(InterruptedException e){}
        
        //Since returning before ensuring that output is present
        output.markStale(true);

        return output;
        
    }
    
    @Override
    public void finish(){
        try{
            os.close();
            try{
                bags.put(new EndOfQueue());
            }catch(InterruptedException e){}
        }catch(IOException e){
            log.error(e);
        }
        while(true){
            try{
                processThread.join();
                break;
            }catch (InterruptedException e){}
        }
    }

    @Override
    public boolean isAsynchronous() {
        return true;
    }
    
    private void readBag(DataBag output) throws IOException {
        baos.reset();
        boolean inRecord = false;
        int c;
        while((c = is.read()) != -1) {
            System.out.print(((char)c));
            if ((inRecord == false) && (c == groupDelim)) {
                output.markStale(false);
                return;
            }
            inRecord = true;
            if (c == recordDelim) {
                inRecord = false;
                readField();
                Tuple t =  mTupleFactory.newTuple(mProtoTuple);
                mProtoTuple.clear();
                output.add(t);
                continue;
            } else if (c == fieldDelim) {
                readField();
            }
            baos.write(c);
        }
    }

    private void readField() {
        if (baos.size() == 0) {
            // NULL value
            mProtoTuple.add(null);
        } else {
            // TODO, once this can take schemas, we need to figure out
            // if the user requested this to be viewed as a certain
            // type, and if so, then construct it appropriately.
            mProtoTuple.add(new DataByteArray(baos.toByteArray()));
        }
        baos.reset();
    }
}
