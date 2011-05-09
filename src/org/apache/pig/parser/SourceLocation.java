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

import org.apache.pig.parser.PigParserNode.InvocationPoint;

public class SourceLocation {
    private String file = null; // Name of the source, null if unknown.
    private int line = -1; // line number, -1 if unknown.
    private int offset = -1; // offset, -f if unknown.
    
    private PigParserNode node; // corresponding parser tree node
    
    public SourceLocation() {
    }
    
    public SourceLocation(String filename, int line, int offset) {
    	this.file = filename;
    	this.line = line;
    	this.offset = offset;
    }
    
    public SourceLocation(PigParserNode tree) {
        this.file = tree.getFileName();
        this.line = tree.getLine();
        this.offset = tree.getCharPositionInLine();
        this.node = tree;
    }
    
    public SourceLocation(SourceLocation location) {
        this.file = location.file;
        this.line = location.line;
        this.offset = location.offset;
        this.node = location.node;
    }
    
    public String file() {
        return file;
    }

    public int line() {
        return line;
    }
    
    public int offset() {
        return offset;
    }
    
    public PigParserNode node() {
        return node;
    }
    
    @Override
    public String toString() {
        if( line == -1 )
            return "";
        
        StringBuilder sb = new StringBuilder();
        if (node != null) {
            InvocationPoint pt = node.getNextInvocationPoint();
            while (pt != null) {
                sb.append("\n");
                sb.append("at expanding macro '" + pt.getMacro() + "' ("
                        + pt.getFile() + ":" + pt.getLine() + ")");
                pt = node.getNextInvocationPoint();
            }
            sb.append("\n");
        }
        sb.append( "<" );
        if( file != null )
            sb.append( "file " + file + ", " );
        sb.append( "line " + line +", column " + offset + "> " );
        
        return sb.toString();
    }
    
}
