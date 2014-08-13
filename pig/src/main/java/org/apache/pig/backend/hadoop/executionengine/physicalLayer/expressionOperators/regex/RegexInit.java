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
package org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.regex;

import java.io.Serializable;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.PORegexp;

public class RegexInit implements RegexImpl, Serializable {

    private static final long serialVersionUID = 1L;
    // Intersection and subtraction ( subtraction cannot be used w/o intersection )
    // ,reluctant and possesive quantifiers 
    // is only possible in java.util.regex
    private static final String[] javaRegexOnly = { "&&", "??", "*?", "+?", "}?",
            "?+", "*+", "++", "}+", "^", "$", "(?" };

    PORegexp regexop = null;
    int side = -1;
    boolean rhsConstant = false;
    
    public RegexInit(PORegexp regexoperator ) {
        this.regexop = regexoperator;
    }
    
    public void setConstExpr( boolean rhsConstant ) {
        this.rhsConstant = rhsConstant;
    }
  
    /**
     * This function determines the type of pattern we are working with
     * The return value of the function determines the type we are expecting
     * @param pattern
     * @return int, 0 means this is java.util.regex,
     * 1 means this is dk.brics.automaton
     */
    private int determineBestRegexMethod( String pattern ) {

        for( int i = 0; i < javaRegexOnly.length; i++ ) {
            for( int j = pattern.length(); j > 0; ) {
                j = pattern.lastIndexOf(javaRegexOnly[i], j );
                if( j > 0 ) {
                    int precedingEsc = precedingEscapes(pattern, j);
                    if( precedingEsc %2 == 0 ) {
                        return 0;
                    }
                    j = j - precedingEsc;
                } else if ( j == 0 ) {
                    return 0;
                }
            }
        }

        // Determine if there are any complex unions in pattern
        // Complex unions are [a-m[n-z]]
        int index = pattern.indexOf('[');
        if( index >= 0 ) {
            int precedingEsc = precedingEscapes(pattern, index);
            if( index != 0 ) {
                while( precedingEsc %2 == 1 ) {
                    index = pattern.indexOf('[', index + 1);
                    precedingEsc = precedingEscapes(pattern, index);
                }
            }
            int index2 = 0;
            int index3 = 0;
            while( index != -1 && index < pattern.length() ) {
                index2 = pattern.indexOf(']', index );
                if( index2 == -1 ) {
                    break;
                }
                precedingEsc = precedingEscapes(pattern, index2);
                // Find the next ']' which is not '\\]'
                while( precedingEsc %2 == 1 ) {
                    index2 = pattern.indexOf(']', index2 + 1);
                    precedingEsc = precedingEscapes(pattern, index2);
                }                
                if( index2 == -1 ) {
                    break;
                }
                index3 = pattern.indexOf('[', index + 1 );
                precedingEsc = precedingEscapes(pattern, index3);
                if( index3 == -1 ) {
                    break;
                }
                // Find the next '[' which is not '\\['
                while( precedingEsc %2 == 1 ) {
                    index3 = pattern.indexOf('[', index3 + 1);
                    precedingEsc = precedingEscapes(pattern, index3);
                }
                if( index3 == -1 ) {
                    break;
                }
                if( index3 < index2 ) {
                    return 0;
                }
                index = index3;
            }
        }

        index = pattern.lastIndexOf('\\');
        if( index > -1 ) {
            int precedingEsc = precedingEscapes(pattern, index);
            // This is the case where we have complex regexes
            // e.g. \d, \D, \s...etc
            while( index != -1 ) {
                if( precedingEsc %2 == 0 && (index + 1 ) < pattern.length() ) {
                    char index_1 = pattern.charAt(index + 1 );
                    if( index_1 == '1' || index_1 == '2' ||
                            index_1 == '3' || index_1 == '4' ||
                            index_1 == '5' || index_1 == '6' ||
                            index_1 == '7' || index_1 == '8' ||
                            index_1 == '9' ||
                            index_1 == 'a' || index_1 == 'e' ||
                            index_1 == '0' || index_1 == 'x' ||
                            index_1 == 'u' ||
                            index_1 == 'c' ||
                            index_1 == 'Q' ||
                            index_1 == 'w' || index_1 == 'W' ||
                            index_1 == 'd' || index_1 == 'D' ||
                            index_1 == 's' || index_1 == 'S' ||
                            index_1 == 'p' || index_1 == 'P' ||
                            index_1 == 'b' || index_1 == 'B' ||
                            index_1 == 'A' || index_1 == 'G' ||
                            index_1 == 'z' || index_1 == 'Z'   
                    ) {
                        return 0; 
                    }
                }

                // We skip past all the escapes
                index = index - ( precedingEsc + 1 );
                precedingEsc = -1;
                if( index >= 0 ){
                    index = pattern.lastIndexOf('\\',index);
                    precedingEsc = precedingEscapes(pattern, index);
                }
            }
        }
        return 1;
    }    

    private int precedingEscapes( String pattern, int startIndex ) {
        if( startIndex > 0 ) {
            // This is the case when there are an odd number of escapes '//'
            int precedingEscapes = 0;
            for(int j = startIndex - 1; j >= 0; j-- ) {
                if( pattern.charAt(j) == '\\' ) {
                    precedingEscapes++;
                } else {
                    break;
                }
            }
            return precedingEscapes;
        } else if ( startIndex == 0 ) {
            return 0;
        }
        return -1;
    }

    private RegexImpl compile( String pattern ) {
        RegexImpl impl = null;
        int regexMethod = determineBestRegexMethod(pattern);
        switch( regexMethod ) {
        case 0:
            impl = new CompiledRegex(Pattern.compile(pattern));
            break;
        case 1:
            try {
                impl = new CompiledAutomaton(pattern);
            } catch( IllegalArgumentException e ) {
                Log log = LogFactory.getLog(getClass());
                log.debug("Got an IllegalArgumentException for Pattern: " + pattern );
                log.debug(e.getMessage());
                log.debug("Switching to java.util.regex" );
                impl = new CompiledRegex(Pattern.compile(pattern));
            }
            break;
        default:
            break;
        }
        return impl;
    }

    @Override
    public boolean match(String lhs, String rhs) {
        RegexImpl impl = null;
        if( rhsConstant ) {
            impl = compile( rhs );
        } else {
            impl = new NonConstantRegex();
        }
        this.regexop.setImplementation( impl );
        return impl.match(lhs, rhs);
    }      

}
