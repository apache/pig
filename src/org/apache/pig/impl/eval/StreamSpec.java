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
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.streaming.ExecutableManager;
import org.apache.pig.impl.streaming.StreamingCommand;
import org.apache.pig.data.Datum;
import org.apache.pig.impl.eval.collector.DataCollector;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class StreamSpec extends EvalSpec {
    private static final long serialVersionUID = 1L;

    private static final Log LOG = 
        LogFactory.getLog(StreamSpec.class.getName());

    private String executableManager;               // ExecutableManager to use
    private StreamingCommand command;               // Actual command to be run

    public StreamSpec(ExecutableManager executableManager, 
                      StreamingCommand command) {
        this.executableManager = executableManager.getClass().getName();
        this.command = command;

        // Setup streaming-specific properties
        if (command.getShipFiles()) {
            parseShipCacheSpecs(command.getShipSpecs(), 
                                properties, "pig.streaming.ship.files");
        }
        parseShipCacheSpecs(command.getCacheSpecs(), 
                            properties, "pig.streaming.cache.files");
    }
    
    private static void parseShipCacheSpecs(List<String> specs, 
            Properties properties, String property) {
        if (specs == null || specs.size() == 0) {
            return;
        }
        
        // Setup streaming-specific properties
        StringBuffer sb = new StringBuffer();
        Iterator<String> i = specs.iterator();
        while (i.hasNext()) {
            sb.append(i.next());
            if (i.hasNext()) {
                sb.append(", ");
            }
        }
        properties.setProperty(property, sb.toString());        
    }

    /**
     * Get the {@link StreamingCommand} for this <code>StreamSpec</code>.
     * @return
     */
    public StreamingCommand getCommand() {
        return command;
    }
    
    public List<String> getFuncs() {
        // No user-defined functions here
        return new ArrayList<String>();
    }

    protected Schema mapInputSchema(Schema schema) {
        return schema;
    }

    protected DataCollector setupDefaultPipe(Properties properties,
                                             DataCollector endOfPipe) {
        return new StreamDataCollector(properties,
                                       (ExecutableManager)PigContext.instantiateFuncFromSpec(executableManager), 
                                       command, endOfPipe);
    }

    public void visit(EvalSpecVisitor v) {
        v.visitStream(this);
    }

    /**
     * A simple {@link DataCollector} which wraps a {@link ExecutableManager}
     * and lets it handle the input and the output to the managed executable.
     */
    private static class StreamDataCollector extends DataCollector {
        ExecutableManager executableManager;            //Executable manager
        
        public StreamDataCollector(Properties properties,
                                   ExecutableManager executableManager,
                                   StreamingCommand command,
                                   DataCollector endOfPipe) {
            super(endOfPipe);
            this.executableManager = executableManager;

            DataCollector successor = 
                new DataCollector(endOfPipe) {
                public void add(Datum d) {
                    // Just forward the data to the next EvalSpec in the pipeline
                    addToSuccessor(d);
                }
            };

            try {
                // Setup the ExecutableManager
                this.executableManager.configure(properties, command, successor);
                
                // Start the executable
                this.executableManager.run();
            } catch (Exception e) {
                LOG.fatal("Failed to create/start ExecutableManager with: " + 
                          e);
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }

        public void add(Datum d) {
            try {
                executableManager.add(d);
            } catch (IOException ioe) {
                LOG.fatal("ExecutableManager.add(" + d + ") failed with: " + 
                          ioe);
                throw new RuntimeException(ioe);
            }
        }

        protected void finish() {
            try {
                executableManager.close();
            }
            catch (Exception e) {
                LOG.fatal("Failed to close ExecutableManager with: " + e);
                throw new RuntimeException(e);
            }
        }
    }
}
