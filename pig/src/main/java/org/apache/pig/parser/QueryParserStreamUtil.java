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

import org.antlr.runtime.CharStream;

public class QueryParserStreamUtil {

    /**
     * Refer to ANTLRStringStream class for definitions of n, p, and data.
     * @param i look ahead number
     */
    public static int LA(int i, int n, int p, char[] data) {
        if( i == 0 ) {
            return 0; // undefined
        }
        
        if ( i < 0 ) {
            i++; // e.g., translate LA(-1) to use offset 0
        }

        if( p + i - 1 >= n ) {
            return CharStream.EOF;
        }
        
        return Character.toUpperCase( data[ p + i - 1 ] );
    }

}
