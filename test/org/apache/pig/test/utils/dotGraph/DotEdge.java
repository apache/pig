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

import java.util.HashMap;
import java.util.Map;

/**
 * This represents an edge in DOT format.
 * An edge in DOT can have attributes but we're not interested
 */
public class DotEdge {
    public DotNode fromNode;
    public DotNode toNode;
    public Map<String, String> attributes = new HashMap<String, String>();

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(fromNode.name + " -> " + toNode.name);
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

    @Override
    public boolean equals(Object other) {
        if (other instanceof DotEdge) {
            DotEdge edge = (DotEdge) other;
            return fromNode.equals(edge.fromNode) && toNode.equals(edge.toNode);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return fromNode.hashCode() * toNode.hashCode();
    }
}