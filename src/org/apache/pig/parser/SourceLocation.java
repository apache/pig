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

public class SourceLocation {
    private int line = -1; // line number, -1 if unknown.
    private int offset = -1; // offset, -f if unknown.
    
    public SourceLocation(int line, int offset) {
        this.line = line;
        this.offset = offset;
    }
    
    public SourceLocation(int line) {
        this.line = line;
    }
    
    public SourceLocation(Token token) {
        this.line = token.getLine();
        this.offset = token.getCharPositionInLine();
    }
    
    public SourceLocation(CommonTree tree) {
        this.line = tree.getLine();
        this.offset = tree.getCharPositionInLine();
    }
    
    public int line() {
        return line;
    }
    
    public int offset() {
        return offset;
    }
    
}
