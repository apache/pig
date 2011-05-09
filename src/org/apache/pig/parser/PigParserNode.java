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

import java.util.List;

import org.antlr.runtime.Token;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.Tree;

public class PigParserNode extends CommonTree {
    
    // the script file this node belongs to
    private String fileName = null;
    
    // macro body starting line number
    private int startLine = 0;
    
    // stack of macro invocations
    private List<InvocationPoint> invokStack;
    
    public PigParserNode(Token t, String fileName, int lineOffset) {
        super(t);
        if (t != null && lineOffset > 0) {
            t.setLine(t.getLine() + lineOffset);
        }
        this.fileName = fileName;
    }
    
    public PigParserNode(Token t, String fileName, Token start) {
        this(t, fileName, 0);
        this.startLine = start.getLine();
    }

    public PigParserNode(PigParserNode node) {
        super(node);
        this.fileName = node.getFileName();
        this.invokStack = node.invokStack;
        this.startLine = node.startLine;
    }

    public Tree dupNode() {
        return new PigParserNode(this);
    }

    public String getFileName() {
        return fileName;
    }
    
    public int getStartLine() {
        return startLine;
    }
    
    public void setInvocationStack(List<InvocationPoint> stack) {
        invokStack = stack;
    }
    
    public List<InvocationPoint> getInvocationStack() {
        return invokStack;
    }
    
    public InvocationPoint getNextInvocationPoint() {
        if (invokStack == null || invokStack.isEmpty()) return null;
        return invokStack.remove(0);
    }
    
    // 'macro' is invoked at 'line' of 'file'
    public static class InvocationPoint {
        private int line;
        private String file;
        private String macro;
        
        public InvocationPoint(int line, String file, String macro) {
            this.line = line;
            this.file = file;
            this.macro = macro;
        }

        public int getLine() {
            return line;
        }

        public String getFile() {
            return file;
        }
        
        public String getMacro() {
            return macro;
        }               
    }

}
