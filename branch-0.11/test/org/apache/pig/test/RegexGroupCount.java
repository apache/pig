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
/**
 * A test udf to test that users can pass in escaped dot
 * as part of a regex to udf's argument.
 * Example: In perl the regex would be "www\.abc\.com" - 
 * the user's intent is to supply this as a regex pattern 
 * where dot (.) is escaped. As a java
 * string this would be "www\\.abc\\.com" - the parser should
 * eventually give this java string to the udf. In pig script too
 * the user would give this as 'www\\.abc\\.com'
 */
package org.apache.pig.test;

import java.io.IOException;

import java.util.regex.Matcher;

import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;

import org.apache.pig.data.Tuple;

public class RegexGroupCount extends EvalFunc<Integer> {

    private final Pattern pattern_;

    public RegexGroupCount(String patternStr) {

       System.out.println("My pattern supplied is "+patternStr);

       System.out.println("Equality test "+patternStr.equals("www\\.xyz\\.com/sports"));
        
       pattern_ = Pattern.compile(patternStr, Pattern.DOTALL|Pattern.CASE_INSENSITIVE);  

    } 

    //@Override

    public Integer exec(Tuple input)  throws IOException {

              int i = 9999;

              if (input == null || input.size() == 0) {   return 8888;  }

              String istr = (String) input.get(0);

              System.out.println("My input is: "+istr);

                try {

                    i = 0;

                    Matcher matcher = pattern_.matcher(istr);

                    while (matcher.find()) {

                               i++;

                    }

                } catch (NullPointerException e) {

                    i = 7777;

                }  catch (Exception e) {

                    i = 6666;

                    throw new IOException("Caught exception processing RegexGroupCount", e);

                }

                return i;

    }

}
