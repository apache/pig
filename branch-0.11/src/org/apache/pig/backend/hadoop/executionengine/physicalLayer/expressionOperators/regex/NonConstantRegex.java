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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NonConstantRegex implements RegexImpl {

    private Pattern pattern = null;
    
    private String oldString = null;
    
    private Matcher matcher = null;

    @Override
    public boolean match(String lhs, String rhs) {
        // We first check for length so the comparison is faster
        // and then we directly check for difference.
        // I havent used equals as first two comparisons,
        // same Object and isInstanceOf does not apply in this case.
        if( oldString == null
                || rhs.length() != oldString.length() 
                || rhs.compareTo(oldString) != 0 ) {
            oldString = rhs;
            pattern = Pattern.compile(oldString);
            matcher = pattern.matcher(lhs);
        } else {
            matcher.reset( lhs );
        }
        return matcher.matches();
    }

}
