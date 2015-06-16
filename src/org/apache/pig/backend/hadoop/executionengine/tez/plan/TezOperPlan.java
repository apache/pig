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
package org.apache.pig.backend.hadoop.executionengine.tez.plan;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.pig.backend.hadoop.executionengine.tez.TezResourceManager;
import org.apache.pig.backend.hadoop.executionengine.tez.util.TezCompilerUtil;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.Pair;

/**
 * A Plan used to create the plan of Tez operators which can be converted into
 * the Job Control object. This is necessary to capture the dependencies among
 * jobs.
 */
public class TezOperPlan extends OperatorPlan<TezOperator> {

    private static final long serialVersionUID = 1L;

    private transient Map<String, Path> extraResources = new HashMap<String, Path>();

    private int estimatedTotalParallelism = -1;

    private transient Credentials creds;

    public TezOperPlan() {
        creds = new Credentials();
    }

    public Credentials getCredentials() {
        return creds;
    }

    public int getEstimatedTotalParallelism() {
        return estimatedTotalParallelism;
    }

    public void setEstimatedParallelism(int estimatedTotalParallelism) {
        this.estimatedTotalParallelism = estimatedTotalParallelism;
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

        addShipResources(udfVisitor.getShipFiles());
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


    /**
     * Move everything below a given operator to the new operator plan.  The specified operator will
     * be moved and will be the root of the new operator plan
     * @param root Operator to move everything under including the root operator
     * @param newPlan new operator plan to move things into
     * @throws PlanException
     */
    public void moveTree(TezOperator root, TezOperPlan newPlan) throws PlanException {
        List<TezOperator> list = new ArrayList<TezOperator>();
        list.add(root);
        int prevSize = 0;
        int pos = 0;
        while (list.size() > prevSize) {
            prevSize = list.size();
            TezOperator node = list.get(pos);
            if (getSuccessors(node)!=null) {
                for (TezOperator succ : getSuccessors(node)) {
                    if (!list.contains(succ)) {
                        list.add(succ);
                    }
                }
            }
            if (getPredecessors(node)!=null) {
                for (TezOperator pred : getPredecessors(node)) {
                    if (!list.contains(pred)) {
                        list.add(pred);
                    }
                }
            }
            pos++;
        }

        for (TezOperator node: list) {
            newPlan.add(node);
        }

        Set<Pair<TezOperator, TezOperator>> toReconnect = new HashSet<Pair<TezOperator, TezOperator>>();
        for (TezOperator from : mFromEdges.keySet()) {
            List<TezOperator> tos = mFromEdges.get(from);
            for (TezOperator to : tos) {
                if (list.contains(from) || list.contains(to)) {
                    toReconnect.add(new Pair<TezOperator, TezOperator>(from, to));
                }
            }
        }

        for (Pair<TezOperator, TezOperator> pair : toReconnect) {
            if (list.contains(pair.first) && list.contains(pair.second)) {
                // Need to reconnect in newPlan
                TezEdgeDescriptor edge = pair.second.inEdges.get(pair.first.getOperatorKey());
                TezCompilerUtil.connect(newPlan, pair.first, pair.second, edge);
            }
        }

        for (TezOperator node : list) {
            // Simply remove from plan, don't deal with inEdges/outEdges
            super.remove(node);
        }
    }

    // This method is used in PigGraceShuffleVertexManager to get a list of grandparents. 
    // Also need to exclude grandparents which also a parent (a is both parent and grandparent in the diagram below)
    //    a   ->    c
    //      \  b  /
    // 
    public static List<TezOperator> getGrandParentsForGraceParallelism(TezOperPlan tezPlan, TezOperator op) {
        List<TezOperator> grandParents = new ArrayList<TezOperator>();
        List<TezOperator> preds = tezPlan.getPredecessors(op);
        if (preds != null) {
            for (TezOperator pred : preds) {
                if (pred.isVertexGroup()) {
                    grandParents.clear();
                    return grandParents;
                }
                List<TezOperator> predPreds = tezPlan.getPredecessors(pred);
                if (predPreds!=null) {
                    for (TezOperator predPred : predPreds) {
                        if (predPred.isVertexGroup()) {
                            grandParents.clear();
                            return grandParents;
                        }
                        if (!grandParents.contains(predPred)) {
                            grandParents.add(predPred);
                        }
                    }
                } else {
                    grandParents.clear();
                    break;
                }
            }

            if (!grandParents.isEmpty()) {
                for (TezOperator pred : preds) {
                    if (grandParents.contains(pred)) {
                        grandParents.remove(pred);
                    }
                }
            }
        }
        return grandParents;
    }
}

