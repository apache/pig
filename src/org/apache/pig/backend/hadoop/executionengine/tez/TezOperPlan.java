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
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.plan.VisitorException;

/**
 * A Plan used to create the plan of Tez operators which can be converted into
 * the Job Control object. This is necessary to capture the dependencies among
 * jobs.
 */
public class TezOperPlan extends OperatorPlan<TezOperator> {

    private static final long serialVersionUID = 1L;

    private Map<String, Path> extraResources = new HashMap<String, Path>();

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

    // Add extra plan-specific local resources from the source FS
    public void addExtraResource(URL url) throws IOException {
        Path resourcePath = new Path(url.getFile());
        String resourceName = resourcePath.getName();

        if (!extraResources.containsKey(resourceName)) {
            Path remoteFsPath;
            try {
                remoteFsPath = TezResourceManager.getInstance().addTezResource(url.toURI());
            } catch (URISyntaxException e) {
                throw new IOException(e);
            }
            extraResources.put(resourceName, remoteFsPath);
        }
    }

    // Add extra plan-specific local resources already present in the remote FS
    public void addExtraResource(String resourceName, Path remoteFsPath) throws IOException {
        if (!extraResources.containsKey(resourceName)) {
            TezResourceManager.getInstance().addTezResource(resourceName, remoteFsPath);
            extraResources.put(resourceName, remoteFsPath);
        }
    }

    // Get the plan-specific resources
    public Map<String, LocalResource> getExtraResources() throws Exception {
        // In a STREAM add the files specified in SHIP and CACHE
        // as local resources for the plan.
        TezPOStreamVisitor streamVisitor = new TezPOStreamVisitor(this);
        streamVisitor.visit();

        addShipResources(streamVisitor.getShipFiles());
        addCacheResources(streamVisitor.getCacheFiles());

        // In a UDF add the files specified by getCacheFiles()
        // as local resources for the plan.
        TezPOUserFuncVisitor udfVisitor = new TezPOUserFuncVisitor(this);
        udfVisitor.visit();

        addCacheResources(udfVisitor.getCacheFiles());

        return TezResourceManager.getInstance().getTezResources(extraResources.keySet());
    }

    // In the statement "SHIP('/home/foo')" we'll map the resource name foo to
    // the file that has been copied to the staging directory in the remote FS.
    private void addShipResources(Set<String> fileNames) throws IOException {
        for (String fileName : fileNames) {
            fileName = fileName.trim();
            if (fileName.length() > 0) {
                URL url = new File(fileName).toURI().toURL();
                addExtraResource(url);
            }
        }
    }

    // In the statement "CACHE('/input/data.txt#alias.txt')" we'll map the
    // resource name alias.txt to the actual resource path in the remote FS
    // at '/input/data.txt'.
    private void addCacheResources(Set<String> fileNames) throws Exception {
        for (String fileName : fileNames) {
            fileName = fileName.trim();
            if (fileName.length() > 0) {
                URI resourceURI = new URI(fileName);
                String fragment = resourceURI.getFragment();

                Path remoteFsPath = new Path(resourceURI.getPath());
                String resourceName = (fragment != null && fragment.length() > 0) ? fragment : remoteFsPath.getName();

                addExtraResource(resourceName, remoteFsPath);
            }
        }
    }

    @Override
    public void remove(TezOperator op) {
        // The remove method does not replace output and input keys in TezInput
        // and TezOutput. That has to be handled separately.
        for (OperatorKey opKey : op.outEdges.keySet()) {
            getOperator(opKey).inEdges.remove(op.getOperatorKey());
        }
        for (OperatorKey opKey : op.inEdges.keySet()) {
            getOperator(opKey).outEdges.remove(op.getOperatorKey());
        }
        super.remove(op);
    }

    @Override
    public boolean disconnect(TezOperator from, TezOperator to) {
        from.outEdges.remove(to.getOperatorKey());
        to.inEdges.remove(from.getOperatorKey());
        return super.disconnect(from, to);
    }

}

