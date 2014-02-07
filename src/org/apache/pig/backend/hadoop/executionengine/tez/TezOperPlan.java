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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.plan.VisitorException;

/**
 * A Plan used to create the plan of Tez operators which can be converted into
 * the Job Control object. This is necessary to capture the dependencies among
 * jobs.
 */
public class TezOperPlan extends OperatorPlan<TezOperator> {

    private static final long serialVersionUID = 1L;

    private Map<URL, Path> extraResources = new HashMap<URL, Path>();

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

    // Add extra plan-specific local resources to HDFS
    public void addExtraResource(URL url) throws IOException {
        if (!extraResources.containsKey(url)) {
            Path pathInHDFS = TezResourceManager.addLocalResource(url);
            extraResources.put(url, pathInHDFS);
        }
    }

    // Add extra plan-specific local resources already present in HDFS
    public void addExtraResource(URL url, Path pathInHDFS) throws IOException {
        if (!extraResources.containsKey(url)) {
            TezResourceManager.addLocalResource(url, pathInHDFS);
            extraResources.put(url, pathInHDFS);
        }
    }

    // Get the plan-specific resources
    public Map<String, LocalResource> getLocalExtraResources() throws Exception {
        TezPOStreamVisitor streamVisitor = new TezPOStreamVisitor(this);
        streamVisitor.visit();

        // In a STREAM add the files specified in SHIP and CACHE
        // as local resources for the plan.
        addFileResources(streamVisitor.getShipFiles());
        addHdfsResources(streamVisitor.getCacheFiles());

        Set<URL> resourceUrls = extraResources.keySet();
        return TezResourceManager.getTezResources(resourceUrls);
    }

    private void addFileResources(Set<String> fileNames) throws IOException {
        Set<URL> fileUrls = new HashSet<URL>();

        for (String fileName : fileNames) {
            fileName = fileName.trim();
            if (fileName.length() > 0) {
                fileUrls.add(ConverterUtils.getYarnUrlFromURI(new File(fileName).toURI()));
            }
        }

        for (URL url : fileUrls) {
            addExtraResource(url);
        }
    }

    // In the statement "CACHE('/input/data.txt#alias.txt')" we'll map the
    // URL 'hdfs:/input/data.txt#alias.txt' to the actual resource path in
    // HDFS at '/input/data.txt'.
    private void addHdfsResources(Set<String> fileNames) throws Exception {
        Map<URL, Path> resourceMap = new HashMap<URL, Path>();

        for (String fileName : fileNames) {
            fileName = fileName.trim();
            if (fileName.length() > 0) {
                URL urlOnHDFS = ConverterUtils.getYarnUrlFromURI(new File(fileName).toURI());
                urlOnHDFS.setScheme("hdfs");

                // Get the path on HDFS without a fragment at the end
                int aliasIndex = fileName.indexOf("#");
                String path = (aliasIndex == -1) ? fileName : fileName.substring(0, aliasIndex);
                Path pathOnHDFS = new Path(path);
                resourceMap.put(urlOnHDFS, pathOnHDFS);
            }
        }

        for (Map.Entry<URL, Path> entry : resourceMap.entrySet()) {
            addExtraResource(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void remove(TezOperator op) {
        //TODO Cleanup outEdges of predecessors and inEdges of successors
        //TezDAGBuilder would not create the edge. So low priority
        super.remove(op);
    }


}

