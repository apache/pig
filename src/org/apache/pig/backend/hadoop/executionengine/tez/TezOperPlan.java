/**
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
package org.apache.pig.backend.hadoop.executionengine.tez;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URL;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.plan.VisitorException;

/**
 * A Plan used to create the plan of Tez operators which can be converted into
 * the Job Control object. This is necessary to capture the dependencies among
 * jobs.
 */
public class TezOperPlan extends OperatorPlan<TezOperator> {

    private static final long serialVersionUID = 1L;

    public TezOperPlan() {
    }

    @Override
    public String toString() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        TezPrinter printer = new TezPrinter(ps, this);
        printer.setVerbose(true);
        try {
            printer.visit();
        } catch (VisitorException e) {
            throw new RuntimeException(e);
        }
        return baos.toString();
    }
    
    public Map<String, LocalResource> getLocalExtraResources() throws IOException {
        Set<URL> jarLists = new HashSet<URL>();
        // TODO: Add script jars/pig-misc jars
        return TezResourceManager.getTezResources(jarLists);
    }
}

