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

lexer grammar MacroImport;

options { 
    filter=true; 
}

@header {
package org.apache.pig.parser;

import java.util.HashSet;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;

}

@members {
    private HashSet<String> memory = new HashSet<String>();
    private StringBuilder sb = new StringBuilder();
    
    public String getResultString() { return sb.toString(); }
}

IMPORT
    : 'import' WS+ name=QUOTEDSTRING ';'
        {
            String fp = $name.text;

            // remove quotes
            fp = fp.substring(1, fp.length()-1);

            // check file name duplication
            if (memory.contains(fp)) {
                throw new RuntimeException("Macro file name duplication: " + fp);
            }
            try {
                BufferedReader in
                    = ParserUtil.getImportScriptAsReader(fp);
                String line = in.readLine();
                while (line != null) {
                    sb.append(line).append("\n");
                    line = in.readLine();
                }
            } catch (IOException e) {
                throw new RuntimeException("Macro import error", e);
            }
            memory.add(fp);
        } 
;

C
: c=. { sb.append((char)c); }
;

fragment WS : (' '|'\n'|'\r')+
;

fragment QUOTEDSTRING :  '\'' (   ( ~ ( '\'' | '\\' | '\n' | '\r' ) )
                       | ( '\\' ( ( 'N' | 'T' | 'B' | 'R' | 'F' | '\\' | '\'' ) ) )
                       | ( '\\u' ( '0'..'9' | 'A'..'F' )
                                 ( '0'..'9' | 'A'..'F' )
                                 ( '0'..'9' | 'A'..'F' )
                                 ( '0'..'9' | 'A'..'F' )  )
                     )*
                '\''
;

