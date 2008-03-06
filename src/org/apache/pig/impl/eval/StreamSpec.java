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
package org.apache.pig.impl.eval;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.LoadFunc;
import org.apache.pig.StoreFunc;
import org.apache.pig.impl.streaming.PigExecutableManager;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Datum;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.eval.collector.DataCollector;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class StreamSpec extends EvalSpec {
    private static final long serialVersionUID = 1L;

    private static final Log LOG = 
        LogFactory.getLog(StreamSpec.class.getName());

    private String streamingCommand;                       // Actual command to be run
    private String deserializer;                           // LoadFunc to be used
    private String serializer;                             // StoreFunc to be used

    public StreamSpec(String streamingCommand) {
        this.streamingCommand = streamingCommand;
        this.deserializer = PigStorage.class.getName();
        this.serializer = PigStorage.class.getName();
    }

    public StreamSpec(String streamingCommand, 
                      String deserializer, String serializer) {
        this.streamingCommand = streamingCommand;
        this.deserializer = deserializer;
        this.serializer = serializer;
    }
    
    @Override
    public List<String> getFuncs() {
        // No user-defined functions here
        return new ArrayList<String>();
    }

    protected Schema mapInputSchema(Schema schema) {
        // EvalSpec _has_ to have the schema if there is one...
        return null;
    }

    protected DataCollector setupDefaultPipe(DataCollector endOfPipe) {
        return new StreamDataCollector(streamingCommand,
                                       (deserializer == null) ? new PigStorage() :
                                         (LoadFunc)PigContext.instantiateFuncFromSpec(
                                                                        deserializer),            
                                       (serializer == null) ? new PigStorage() :
                                         (StoreFunc)PigContext.instantiateFuncFromSpec(
                                                                        serializer),
                                      endOfPipe);
    }

    public void visit(EvalSpecVisitor v) {
        v.visitStream(this);
    }

    /**
     * A simple {@link DataCollector} which wraps a {@link PigExecutableManager}
     * and lets it handle the input and the output to the managed executable.
     */
    private static class StreamDataCollector extends DataCollector {
        PigExecutableManager executable;                          //Executable manager
        
        public StreamDataCollector(String streamingCommand,
                                   LoadFunc deserializer, StoreFunc serializer,
                                   DataCollector endOfPipe) {
            super(endOfPipe);

            DataCollector successor = 
                new DataCollector(endOfPipe) {
                public void add(Datum d) {
                    // Just forward the data to the next EvalSpec in the pipeline
                    addToSuccessor(d);
                }
            };

            try {
                // Create the PigExecutableManager
                executable = new PigExecutableManager(streamingCommand, 
                                                      deserializer, serializer, 
                                                      successor);
                
                executable.configure();
                
                // Start the executable
                executable.run();
            } catch (Exception e) {
                LOG.fatal("Failed to create/start PigExecutableManager with: " + e);
                throw new RuntimeException(e);
            }
        }

        public void add(Datum d) {
            try {
                executable.add(d);
            } catch (IOException ioe) {
                LOG.fatal("executable.add(" + d + ") failed with: " + ioe);
                throw new RuntimeException(ioe);
            }
        }

        protected void finish() {
            try {
                executable.close();
            }
            catch (Exception e) {
                LOG.fatal("Failed to close PigExecutableManager with: " + e);
                throw new RuntimeException(e);
            }
        }
    }
}
