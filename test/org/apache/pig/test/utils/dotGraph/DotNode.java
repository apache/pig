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
 * This represents a node in DOT format
 */
public class DotNode {
    public String name;
    public Map<String, String> attributes = new HashMap<String, String>();
    public Set<DotNode> edgeTo = new HashSet<DotNode>();

    public DotNode(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(name);
        if (attributes.size() > 0) {
            int index = 0;
            sb.append(" [");
            for (Map.Entry<String, String> attr : attributes.entrySet()) {
                sb.append(attr.getKey() + "=" + attr.getValue());
                if (index < attributes.size() - 1)
                    sb.append(", ");
                index++;
            }
            sb.append("]");
        }
        return sb.toString();
    }

    public String getLabel() {
        String label = "";
        for (Map.Entry<String, String> attr : attributes.entrySet()) {
            if (attr.getKey().equals("label")) {
                label = attr.getValue();
                break;
            }
        }
        return label;
    }

    public boolean isInvisStyle() {
        for (Map.Entry<String, String> attr : attributes.entrySet())
            if (attr.getKey().equals("style") && attr.getValue().equals("invis"))
                return true;

        return false;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof DotNode && !(other instanceof DotGraph)) {
            DotNode node = (DotNode) other;
            return name.equals(node.name);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    public String getCanonicalName() {
        StringBuilder sb = new StringBuilder("");
        ArrayList<String> canonicalNames = new ArrayList<>();

        for (DotNode node : edgeTo)
            if (!node.isInvisStyle())
                canonicalNames.add(node.getCanonicalName());

        Collections.sort(canonicalNames);
        for (String nodeName : canonicalNames)
            sb.append(nodeName);

        sb.insert(0, '0');
        sb.insert(sb.length(), '1');
        return sb.toString();
    }
}