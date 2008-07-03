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
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.LoadFunc;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.BufferedPositionedInputStream;


public class HJob implements ExecJob {

    private final Log log = LogFactory.getLog(getClass());
    
    protected JOB_STATUS status;
    protected PigContext pigContext;
    protected FileSpec outFileSpec;
    
    public HJob(JOB_STATUS status,
                PigContext pigContext,
                FileSpec outFileSpec) {
        this.status = status;
        this.pigContext = pigContext;
        this.outFileSpec = outFileSpec;
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
             p = (LoadFunc)PigContext.instantiateFuncFromSpec(outFileSpec.getFuncSpec());

             InputStream is = FileLocalizer.open(outFileSpec.getFileName(), pigContext);

             p.bindTo(outFileSpec.getFileName(), new BufferedPositionedInputStream(is), 0, Long.MAX_VALUE);

        }catch (Exception e){
            throw new ExecException("Unable to get results for " + outFileSpec, e);
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

    public Properties getContiguration() {
        Properties props = new Properties();
        return props;
    }

    public Map<String, Object> getStatistics() {
        throw new UnsupportedOperationException();
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
}
