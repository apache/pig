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

package org.apache.pig.backend.hadoop.executionengine;

import java.io.OutputStream;
import java.util.Iterator;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.io.ReadToEndLoader;
import org.apache.pig.tools.pigstats.PigStats;


public class HJob implements ExecJob {

    private final Log log = LogFactory.getLog(getClass());
    
    protected JOB_STATUS status;
    protected PigContext pigContext;
    protected FileSpec outFileSpec;
    protected Exception backendException;
    protected String alias;
    protected POStore poStore;
    private PigStats stats;
    
    public HJob(JOB_STATUS status,
                PigContext pigContext,
                POStore store,
                String alias) {
        this.status = status;
        this.pigContext = pigContext;
        this.poStore = store;
        this.outFileSpec = poStore.getSFile();
        this.alias = alias;
    }
    
    public HJob(JOB_STATUS status,
            PigContext pigContext,
            POStore store,
            String alias,
            PigStats stats) {
        this.status = status;
        this.pigContext = pigContext;
        this.poStore = store;
        this.outFileSpec = poStore.getSFile();
        this.alias = alias;
        this.stats = stats;
    }
    
    public JOB_STATUS getStatus() {
        return status;
    }
    
    public boolean hasCompleted() throws ExecException {
        return true;
    }
    
    public Iterator<Tuple> getResults() throws ExecException {
        final LoadFunc p;
        
        try{
             LoadFunc originalLoadFunc = 
                 (LoadFunc)PigContext.instantiateFuncFromSpec(
                         outFileSpec.getFuncSpec());
             
             p = (LoadFunc) new ReadToEndLoader(originalLoadFunc, 
                     ConfigurationUtil.toConfiguration(
                     pigContext.getProperties()), outFileSpec.getFileName(), 0, pigContext);

        }catch (Exception e){
            int errCode = 2088;
            String msg = "Unable to get results for: " + outFileSpec;
            throw new ExecException(msg, errCode, PigException.BUG, e);
        }
        
        return new Iterator<Tuple>() {
            Tuple   t;
            boolean atEnd;

            public boolean hasNext() {
                if (atEnd)
                    return false;
                try {
                    if (t == null)
                        t = p.getNext();
                    if (t == null)
                        atEnd = true;
                } catch (Exception e) {
                    log.error(e);
                    t = null;
                    atEnd = true;
                    throw new Error(e);
                }
                return !atEnd;
            }

            public Tuple next() {
                Tuple next = t;
                if (next != null) {
                    t = null;
                    return next;
                }
                try {
                    next = p.getNext();
                } catch (Exception e) {
                    log.error(e);
                }
                if (next == null)
                    atEnd = true;
                return next;
            }

            public void remove() {
                throw new RuntimeException("Removal not supported");
            }

        };
    }

    public Properties getConfiguration() {
        return pigContext.getProperties();
    }

    public PigStats getStatistics() {
        //throw new UnsupportedOperationException();
        return stats;
    }

    public void completionNotification(Object cookie) {
        throw new UnsupportedOperationException();
    }
    
    public void kill() throws ExecException {
        throw new UnsupportedOperationException();
    }
    
    public void getLogs(OutputStream log) throws ExecException {
        throw new UnsupportedOperationException();
    }
    
    public void getSTDOut(OutputStream out) throws ExecException {
        throw new UnsupportedOperationException();
    }
    
    public void getSTDError(OutputStream error) throws ExecException {
        throw new UnsupportedOperationException();
    }

    public void setException(Exception e) {
        backendException = e;
    }

    public Exception getException() {
        return backendException;
    }

    @Override
    public String getAlias() throws ExecException {
        return alias;
    }

    /**
     * @return the poStore
     */
    @Override
    public POStore getPOStore() {
        return poStore;
    }
}
