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

package org.apache.pig.backend.local.executionengine;

import java.io.OutputStream;
import java.util.Iterator;
import java.util.Properties;
import java.util.Map;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataBag;

public class LocalJob implements ExecJob {

    protected DataBag results;
    protected JOB_STATUS status;
    
    public LocalJob(DataBag results, JOB_STATUS status) {
        this.results = results;
        this.status = status;
    }
    
    public JOB_STATUS getStatus() {
        return status;
    }
    
    public boolean hasCompleted() throws ExecException {
        return true;
    }
    
    public Iterator<Tuple> getResults() throws ExecException {
        return this.results.content();
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
