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
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Datum;
import org.apache.pig.data.Tuple;


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
    
    LinkedBlockingQueue<DataBag> bags = new LinkedBlockingQueue<DataBag>();
    
    
    public ShellBagEvalFunc(String cmd) {
        this.cmd = cmd;
    }

    private class EndOfQueue extends DataBag {
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
    public void exec(Tuple input, DataBag output) throws IOException {
        if (os == null) {
            startProcess();
        }
        os.write(input.toDelimitedString(fieldDelimString).getBytes());
        os.write(recordDelim);
        os.write(groupDelim);
        os.flush();
        try{
            bags.put(output);
        }catch(InterruptedException e){}
        
        //Since returning before ensuring that output is present
        output.markStale(true);
        
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
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
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
                Tuple t = new Tuple(baos.toString(), fieldDelimString);
                // log.error(Thread.currentThread().getName() + ": Adding tuple " + t + " to collector " + output);
                output.add(t);
                baos = new ByteArrayOutputStream();
                continue;
            }
            baos.write(c);
        }
    }
}
