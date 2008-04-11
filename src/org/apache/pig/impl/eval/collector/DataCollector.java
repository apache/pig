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
package org.apache.pig.impl.eval.collector;

import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Datum;
import org.apache.pig.data.Tuple;



/**
 * This interface is used to represent tuple collectors.
 * It may be an in memory DataBag, an input to another function,
 * or a file.
 */
public abstract class DataCollector {
    private static final Log LOG = 
        LogFactory.getLog(DataCollector.class.getName());
    
    Integer staleCount = 0;
    protected boolean inTheMiddleOfBag = false;
    
    protected DataCollector successor = null;
    
    public DataCollector(DataCollector successor){
        this.successor = successor;
    }
    
    /**
     * Add a tuple to the collector.
     */
    public abstract void add(Datum d);
    
    private boolean needsFlattening(){
        if (needFlatteningLocally() || (successor!=null && successor.needsFlattening()))
            return true;
        else
            return false;
    }
    
    /*
     * Whether this collector needs flattened bags to operate
     */
    protected boolean needFlatteningLocally(){
        return false;
    }
    
    
    protected boolean checkDelimiter(Datum d){
        if (d instanceof DataBag.BagDelimiterTuple){
            if (d instanceof DataBag.StartBag){
                if (inTheMiddleOfBag)
                    throw new RuntimeException("Internal error: Found a flattened bag inside another");
                else
                    inTheMiddleOfBag = true;
            }else{
                if (!(d instanceof DataBag.EndBag))
                    throw new RuntimeException("Internal error: Unknown bag delimiter type");
                if (!inTheMiddleOfBag)
                    throw new RuntimeException("Internal error: Improper nesting of bag delimiter tuples");
                inTheMiddleOfBag = false;
            }
            return true;
        }
        return false;
    }

    protected void addToSuccessor(Datum d){
        if (d instanceof DataBag && !inTheMiddleOfBag && successor!=null && successor.needsFlattening()){
            DataBag bag = (DataBag)d;
            //flatten the bag and send it through the pipeline
            successor.add(DataBag.startBag);
            Iterator<Tuple> iter = bag.iterator();
            while(iter.hasNext())
                successor.add(iter.next());
            successor.add(DataBag.endBag);
        }else{
            //simply add the datum
            successor.add(d);
        }
    }
    
    public void markStale(boolean stale){
        synchronized(staleCount){
            if (stale){
                staleCount++;
            }else{
                if (staleCount > 0){
                    synchronized(this){
                        staleCount--;
                        notifyAll();
                    }
                }
            }
        }
        if (successor!=null)
            successor.markStale(stale);
    }
    
    public void setSuccessor(DataCollector output){
        this.successor = output;
    }
    
    public boolean isStale(){
        synchronized(staleCount){
            return staleCount>0;
        }
    }
    
    public final void finishPipe() {
        try {
            finish();
            
            if (successor != null) {
                successor.finishPipe();
            //    successor = null;
            }
        } catch (Exception e) {
            try {
                if (successor != null) {
                    successor.finishPipe();
                } 
            } catch (Exception ignored) {
                // Ignore this exception since the original is more relevant
                LOG.debug(ignored);
            }
            //successor = null;
            throw new RuntimeException(e);
        }
    }
    
    protected void finish(){}
    
}

