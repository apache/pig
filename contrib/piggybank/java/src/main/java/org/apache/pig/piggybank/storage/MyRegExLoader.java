/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the
 * NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.pig.piggybank.storage;

import java.util.regex.Pattern;

/*
 * MyRegExLoader extends RegExLoader, allowing regular expressions to be passed by argument through pig latin
 * via a line like
 * 
 * A = LOAD 'file:test.txt' USING org.apache.pig.piggybank.storage.MyRegExLoader('(\\d+)!+(\\w+)~+(\\w+)');
 * 
 * which would parse lines like
 * 
 * 1!!!one~i 2!!two~~ii 3!three~~~iii
 * 
 * into arrays like
 * 
 * {1, "one", "i"}, {2, "two", "ii"}, {3, "three", "iii"}
 */

public class MyRegExLoader extends RegExLoader {
    Pattern pattern = null;

    public MyRegExLoader(String pattern) {
      pattern = pattern.replace("\\\\","\\");
      this.pattern = Pattern.compile(pattern);
    }

    @Override
    public Pattern getPattern() {
        return pattern;
    }
}
