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
package org.apache.pig.test;

import java.lang.reflect.Method;
import java.util.Random;
import java.util.regex.Pattern;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.PORegexp;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.regex.NonConstantRegex;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.regex.CompiledAutomaton;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.regex.CompiledRegex;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.regex.RegexInit;
import org.apache.pig.impl.plan.OperatorKey;
import org.junit.Before;
import org.junit.Test;
import junit.framework.TestCase;

public class TestPORegexp extends TestCase {

    static Random r = new Random();
    
    @Before
    @Override
    protected void setUp() throws Exception {
        
    }
    
    @Test
    public void testOrdering() {
        try {
            CompiledAutomaton auto2 = new CompiledAutomaton("[a-z]{3}");
            assertEquals(false, auto2.match("1234", "abc") );
            assertEquals(true, auto2.match("abc", "1234") );
            
            auto2 = new CompiledAutomaton(".*#c");
            assertEquals(true, auto2.match("ab#c", "dummy"));

            auto2 = new CompiledAutomaton(".*@.*");
            assertEquals(true, auto2.match("ab@c", "dummy"));

            auto2 = new CompiledAutomaton("abc&def");
            assertEquals(true, auto2.match("abc&def", "dummy"));

            auto2 = new CompiledAutomaton("abc~[ab]");
            assertEquals(true, auto2.match("abc~a", "dummy"));
            
            CompiledRegex regex2 = new CompiledRegex(Pattern.compile("[a-z]{3}"));
            assertEquals(false, regex2.match("1234", "abc") );
            assertEquals(true, regex2.match("abc", "1234") );
            
            NonConstantRegex ncr = new NonConstantRegex();
            assertEquals(false, ncr.match("1234", "abc"));
            assertEquals(false, ncr.match("abc", "1234"));
            assertEquals(true, ncr.match("1234", "\\d\\d\\d\\d"));
            assertEquals(true, ncr.match("abc", "[a-z]{3}"));

            
        } catch( Exception e ){ 
            fail();
        }
    }
    
    @Test
    public void testRegexDetermination() {
        try {
            Method m = RegexInit.class.getDeclaredMethod
            ("determineBestRegexMethod", String.class);
            m.setAccessible(true);
            
            RegexInit regex = new RegexInit(new PORegexp(new OperatorKey()));
            
            assertEquals(1, m.invoke(regex, "abc") );
            
            assertEquals(1, m.invoke(regex, "\\\\abc") );
            
            assertEquals(1,  m.invoke(regex, "abc.*"));
            
            assertEquals(1,  m.invoke(regex, ".*abc"));
            
            assertEquals(1,  m.invoke(regex, ".*abc.*"));
            
            assertEquals(1,  m.invoke(regex, ".*abc\\.*"));
            
            assertEquals(1,  m.invoke(regex, ".*abc\\\\"));
            
            assertEquals(0,  m.invoke(regex, ".*abc\\d"));
            
            assertEquals(0,  m.invoke(regex, ".*abc\\s"));
            
            assertEquals(0,  m.invoke(regex, ".*abc\\Sw"));
            
            assertEquals(0,  m.invoke(regex, "abc\\Sw"));

            assertEquals(0,  m.invoke(regex, "a\\Q"));
            
            assertEquals(0,  m.invoke(regex, "\\QThis is something"));
            
            assertEquals(0,  m.invoke(regex, "(\\w)*\\s\\1"));

            assertEquals(0,  m.invoke(regex, "[^a]bc"));
            
            assertEquals(0,  m.invoke(regex, "\\p{Alpha}hi"));
            
            assertEquals(0,  m.invoke(regex, "\\d{1,2}hi"));
            
            assertEquals(0,  m.invoke(regex, "^abc.*"));
            
            assertEquals(1,  m.invoke(regex, ".*[A-F]{2,3}.*"));            
            
            assertEquals(0,  m.invoke(regex, "\\d+"));
            
            assertEquals(0,  m.invoke(regex, "\\d{2,3}"));
            
            assertEquals(0,  m.invoke(regex, "\\\\\\d{2,3}"));
            
            assertEquals(0,  m.invoke(regex, ".*\\d{2,3}.*"));
            
            assertEquals(0,  m.invoke(regex, "\\d\\.0\\d"));
            
            assertEquals(0,  m.invoke(regex, "[^f]ed.*"));
            
            assertEquals(0,  m.invoke(regex, "[a-m[n-z]]"));
            
            assertEquals(0,  m.invoke(regex, "[a-z&&[def]]"));

            assertEquals(0,  m.invoke(regex, "[a-z&&[^abc]]"));
            
            assertEquals(1,  m.invoke(regex, "[a-m\\[n-z\\]"));

            assertEquals(1,  m.invoke(regex, "[a-m\\\\\\[n-z\\\\\\]]"));

            assertEquals(0,  m.invoke(regex, "[a-m\\\\\\[n-z\\\\\\][0-9]]"));
            
            assertEquals(0,  m.invoke(regex, "[a-m\\\\[n-z]]"));
            
            assertEquals(0,  m.invoke(regex, "\\\\\\[[a-m\\\\\\[n-z\\\\\\][0-9]]"));
            
            assertEquals(0,  m.invoke(regex, "[a-z]??" ));
            
            assertEquals(0,  m.invoke(regex, "[a-z]*?" ));

            assertEquals(0,  m.invoke(regex, "[a-z]+?" ));
            
            assertEquals(0,  m.invoke(regex, "[a-z]{4}?" ));

            assertEquals(0,  m.invoke(regex, "[a-z]{2,4}?" ));
            
            assertEquals(1,  m.invoke(regex, "[a-z]\\??" ));
            
            assertEquals(1,  m.invoke(regex, "[a-z]\\*?" ));

            assertEquals(1,  m.invoke(regex, "[a-z]\\+?" ));
            
            assertEquals(1,  m.invoke(regex, "[a-z]{4\\}?" ));

            assertEquals(1,  m.invoke(regex, "[a-z]{2,4\\}?" ));
            
            assertEquals(0,  m.invoke(regex, "[a-z]?+" ));
            
            assertEquals(0,  m.invoke(regex, "[a-z]*+" ));

            assertEquals(0,  m.invoke(regex, "[a-z]++" ));
            
            assertEquals(0,  m.invoke(regex, "[a-z]{4}+" ));

            assertEquals(0,  m.invoke(regex, "[a-z]{2,4}+" ));
            
            assertEquals(1,  m.invoke(regex, "[a-z]\\?+" ));
            
            assertEquals(1,  m.invoke(regex, "[a-z]\\*+" ));

            assertEquals(1,  m.invoke(regex, "[a-z]\\++" ));
            
            assertEquals(1,  m.invoke(regex, "[a-z]{4\\}+" ));

            assertEquals(1,  m.invoke(regex, "[a-z]{2,4\\}+" ));

            assertEquals(1,  m.invoke(regex, "[a-m\\[n-z\\]]" ));
            
            assertEquals(0,  m.invoke(regex, "\\0101" ));
            
            assertEquals(0,  m.invoke(regex, "\\x0A" ));

            assertEquals(0,  m.invoke(regex, "\\u000A" ));

            assertEquals(0,  m.invoke(regex, "&&" ));

            assertEquals(1,  m.invoke(regex, "\\&&asdkfjalsdf" ));

            assertEquals(0,  m.invoke(regex, "&&asdf\\&&" ));

            assertEquals(0,  m.invoke(regex, "&&asdf\\&&asdfasdf" ));

            assertEquals(0,  m.invoke(regex, "&&asdfas\\&&asdfasdfa\\&&" ));

            assertEquals(0,  m.invoke(regex, "&&asdflj&&" ));

            assertEquals(0,  m.invoke(regex, "\\\\&&asdfasdf" ));

            assertEquals(1,  m.invoke(regex, "\\\\\\&&asdfasdf" ));
            
            assertEquals(0,  m.invoke(regex, "\\\\&&asdfasdf&&" ));

            assertEquals(0,  m.invoke(regex, "\\&&asdfasdf\\\\&&" ));

            assertEquals(0,  m.invoke(regex, "\\&&asd&&fasdf\\\\\\&&" ));
            
            assertEquals(0,  m.invoke(regex, "\\dasdfasdf" ));

            assertEquals(1,  m.invoke(regex, "\\\\dasdfasdf" ));

            assertEquals(0,  m.invoke(regex, "\\\\dasdfasdf\\d" ));

            assertEquals(0,  m.invoke(regex, "\\\\dasdf\\dasdf\\\\d" ));

            assertEquals(0,  m.invoke(regex, "\\\\dasd\\\\dfasdf\\d" ));

            assertEquals(1,  m.invoke(regex, "\\\\dasdfasdf\\" ));

            assertEquals(0,  m.invoke(regex, "\\dasase\\\\dfasdf\\" ));

            assertEquals(1,  m.invoke(regex, "\\\\dasdfasdf\\\\" ));

            assertEquals(1, m.invoke(regex, "xyz#abc") );


        } catch( Exception e ) {
            System.err.println(e.getMessage());
            fail();
        }
    }
}
