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

package org.apache.pig.tools.pigstats;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.ReadToEndLoader;

/**
 * This class encapsulates the runtime statistics of an user specified output.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class OutputStats {

    private String name;
    private String location;
    private long bytes;
    private long records;

    private boolean success;

    private POStore store = null;
    
    private Configuration conf;

    private static final Log LOG = LogFactory.getLog(OutputStats.class);
    
    public OutputStats(String location, long bytes, long records, boolean success) {
        this.location = location;
        this.bytes = bytes;
        this.records = records;        
        this.success = success;
        try {
            this.name = new Path(location).getName();
        } catch (Exception e) {
            // location is a mal formatted URL 
            this.name = location;
        }
    }

    public String getName() {
        return name;
    }

    public String getLocation() {
        return location;
    }

    public long getBytes() {
        return bytes;
    }

    public long getNumberRecords() {
        return records;
    }

    public String getFunctionName() {
        return (store == null) ? null : store.getSFile().getFuncSpec()
                .getClassName();
    }

    public boolean isSuccessful() {
        return success;
    }

    public String getAlias() {
        return (store == null) ? null : store.getAlias();
    }

    public POStore getPOStore() {
        return store;
    }

    public Configuration getConf() {
        return conf;
    }
    
    public String getDisplayString(boolean local) {
        StringBuilder sb = new StringBuilder();
        if (success) {
            sb.append("Successfully stored ");
            if (!local && records >= 0) {
                sb.append(records).append(" records ");
            } else {
                sb.append("records ");
            }
            if (bytes > 0) {
                sb.append("(").append(bytes).append(" bytes) ");
            }
            sb.append("in: \"").append(location).append("\"\n");
        } else {
            sb.append("Failed to produce result in \"").append(location)
                    .append("\"\n");
        }
        return sb.toString();
    }

    public void setPOStore(POStore store) {
        this.store = store;
    }
    
    public void setConf(Configuration conf) {
        this.conf = conf;
    }
    
    public Iterator<Tuple> iterator() throws IOException {
        final LoadFunc p;
        PigContext pigContext = ScriptState.get().getPigContext();
        if (pigContext == null || store == null) {
            throw new IllegalArgumentException();
        }
        try {
            LoadFunc originalLoadFunc = (LoadFunc) PigContext
                    .instantiateFuncFromSpec(store.getSFile().getFuncSpec());

            p = (LoadFunc) new ReadToEndLoader(originalLoadFunc,
                    ConfigurationUtil.toConfiguration(pigContext
                            .getProperties()), store.getSFile().getFileName(),
                    0);

        } catch (Exception e) {
            int errCode = 2088;
            String msg = "Unable to get results for: " + store.getSFile();
            throw new ExecException(msg, errCode, PigException.BUG, e);
        }
        
        return new Iterator<Tuple>() {        
            Tuple   t;
            boolean atEnd;

            public boolean hasNext() {
                if (atEnd) return false;
                try {
                    if (t == null) t = p.getNext();
                    if (t == null) atEnd = true;
                } catch (Exception e) {
                    LOG.error(e);
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
                    LOG.error(e);
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
}
