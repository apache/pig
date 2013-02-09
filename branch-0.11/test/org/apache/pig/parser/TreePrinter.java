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

import org.antlr.runtime.tree.CommonTree;

public class TreePrinter {
    
    public static void printTree(CommonTree tree, int indent) {
        if( tree != null ) {
            StringBuilder sb = new StringBuilder();
            for ( int i = 0; i < indent; i++ )
                sb = sb.append( "   " );

            System.out.println( sb + tree.getText() );

            for ( int i = 0; i < tree.getChildCount(); i++ ) {
                printTree( (CommonTree)tree.getChild( i ), indent + 1 );
            }
        }
    }
    
}
