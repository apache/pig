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

package org.apache.pig.test.utils.dotGraph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/***
 * This represents graph structure in DOT format
 */
public class DotGraph extends DotNode {
    public boolean topLevel = false;
    public Set<DotEdge> edges = new HashSet<DotEdge>();
    public Set<DotNode> nodes = new HashSet<DotNode>();
    public Map<String, String> edgeAttributes = new HashMap<String, String>();
    public Map<String, String> nodeAttributes = new HashMap<String, String>();

    public DotGraph(String name) {
        super(name);
    }

    @Override
    public String toString() {
        String graphType = topLevel ? "digraph " : "subgraph ";
        StringBuilder sb = new StringBuilder(graphType);
        sb.append(name);
        sb.append("{\n");

        for (Map.Entry<String, String> attr : attributes.entrySet())
            sb.append(attr.getKey() + "=" + attr.getValue() + ";\n");

        if (nodeAttributes.size() > 0) {
            int index = 0;
            sb.append("node [");
            for (Map.Entry<String, String> attr : nodeAttributes.entrySet()) {
                sb.append(attr.getKey() + "=" + attr.getValue());
                if (index < nodeAttributes.size() - 1)
                    sb.append(", ");
                index++;
            }
            sb.append("];\n");
        }

        if (edgeAttributes.size() > 0) {
            int index = 0;
            sb.append("edge [");
            for (Map.Entry<String, String> attr : edgeAttributes.entrySet()) {
                sb.append(attr.getKey() + "=" + attr.getValue());
                if (index < edgeAttributes.size() - 1)
                    sb.append(", ");
                index++;
            }
            sb.append("];\n");
        }

        for (DotNode node : nodes)
            sb.append(node.toString() + ";\n");

        for (DotEdge edge : edges)
            sb.append(edge.toString() + ";\n");

        sb.append("}");
        return sb.toString();
    }

    @Override
    public boolean equals(Object other) {
        if (other != null && other instanceof DotGraph) {
            DotGraph graph = (DotGraph) other;
            return graph.getLabel().equals(getLabel()) && edges.equals(graph.edges) && nodes.equals(graph.nodes);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return getLabel().hashCode() * edgeAttributes.hashCode() * nodeAttributes.hashCode();
    }

    private Set<DotNode> getRootNodes() {
        Set<DotNode> roots = new HashSet<DotNode>(nodes);

        for (DotEdge edge : edges)
            roots.remove(edge.toNode);

        return roots;
    }

    public boolean isomorphic(DotNode other) {
        if (other instanceof DotGraph) {
            DotGraph graph = (DotGraph) other;
            return graph.getLabel().equals(getLabel()) && graph.getCanonicalName().equals(getCanonicalName());
        }
        return false;
    }

    @Override
    public String getCanonicalName() {
        StringBuilder sb = new StringBuilder("");
        ArrayList<String> children = new ArrayList<>();

        Set<DotNode> roots = getRootNodes();

        for (DotNode root : roots)
            children.add(root.getCanonicalName());

        Collections.sort(children);
        for (String nodeName : children)
            sb.append(nodeName);

        sb.insert(0, '#');
        sb.insert(sb.length(), '#');
        return sb.toString();
    }

    @Override
    public String getLabel() {
        StringBuilder label = new StringBuilder(super.getLabel());
        ArrayList<String> children = new ArrayList<>();

        Set<DotNode> roots = getRootNodes();

        for (DotNode root : roots)
            children.add(root.getLabel());

        Collections.sort(children);
        for (String nodeLabel : children)
            label.append(nodeLabel);

        label.insert(0, '\"');
        label.insert(label.length(), '\"');
        return label.toString();
    }
}