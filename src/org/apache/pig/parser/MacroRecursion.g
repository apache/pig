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

lexer grammar MacroRecursion;

options { 
    filter=true;
}

@header {
package org.apache.pig.parser;

import java.util.HashMap;
import java.util.List;
import org.apache.pig.parser.PigMacro;
}

@members {
    private Map<String, PigMacro> memory = new HashMap<String, PigMacro>();
    private StringBuilder sb = new StringBuilder();
    private boolean isExpanded = false;
 
    public String getResultString() { return sb.toString(); } 

    public void setMacros(Map<String, PigMacro> macros) {
        memory = macros;
    }
    
    public boolean isExpanded() { return isExpanded; }
}

INLINE
    : rets+=ALIAS (',' WS* rets+=ALIAS)* WS '=' WS name=ALIAS WS? '(' ( params+=PARAMETER (',' WS* params+=PARAMETER)* )? ');'
        {
            String mn = $name.text;
            PigMacro macro = memory.get(mn);
            if (macro == null) {
                throw new RuntimeException("Pig macro '" + mn + "' must be defined before being invoked");
            }
            String[] inputs = null;
            if ($params != null) {
                inputs = new String[$params.size()]; 
                int i = 0;
                for (Object param : $params) {
                    inputs[i++] = ((Token)param).getText();
                }
            }   
            String[] outputs = null;
            if ($rets != null) {
                outputs = new String[$rets.size()];
                int i = 0;
                for (Object ret : $rets) {
                    outputs[i++] = ((Token)ret).getText();
                }
            }           
            String s = macro.substituteParams(inputs, outputs);
            sb.append(s);
            
            isExpanded = true;
        }
;

C
: c=. { sb.append((char)c); }
;

fragment WS : (' '|'\n'|'\r')+  
;

fragment DIGIT : '0'..'9'
;

fragment LETTER : ('A'..'Z' | 'a'..'z')
;
    
fragment SPECIALCHAR : '_'
;

fragment ALIAS 
    : LETTER ( DIGIT | LETTER | SPECIALCHAR )*
    | '$'  LETTER ( DIGIT | LETTER | SPECIALCHAR )*
;

fragment INTEGER: ( DIGIT )+
;

fragment PERIOD : '.'
;
    
fragment FLOATINGPOINT : (INTEGER PERIOD INTEGER  | PERIOD INTEGER) 
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

fragment PARAMETER : (ALIAS | INTEGER | FLOATINGPOINT | QUOTEDSTRING)
;

