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
package org.apache.pig.parser;

import org.antlr.runtime.Token;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.Tree;

public class PigParserNode extends CommonTree {
    
    // the script file this node belongs to
    private String fileName = null;
    
    public PigParserNode(Token t, String fileName) {
        super(t);
        this.fileName = fileName;
    }

    public PigParserNode(PigParserNode node) {
        super(node);
        this.fileName = node.getFileName();
    }


    public Tree dupNode() {
        return new PigParserNode(this);
    }

    public String getFileName() {
        return fileName;
    }

}
