
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

package org.apache.hadoop.owl.common;

import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.common.OwlUtil;
import org.apache.hadoop.owl.OwlTestCase;
import org.junit.Test;

public class TestOwlUtil extends OwlTestCase {

    @Test
    public static void testencodeURICompatible(){
        String blah = "Hello World! Plus(+) Space( ) Percent(%) Ampersand(&) DoubleQuote(\")";
        String encoded = null;
        try {
            encoded = OwlUtil.encodeURICompatible(blah);
        } catch (OwlException e) {
            e.printStackTrace();
            assertNull(e); // exception was not expected - we turn it into an assertion
        }
        System.out.println(encoded);
        assertNotNull(encoded);
        assertEquals(encoded,
                "Hello%20World%21%20Plus%28%2B%29%20Space%28%20%29%20Percent%28%25%29%20Ampersand%28%26%29%20DoubleQuote%28%22%29"        
        );
    }


    /**
     * Generate long string.
     * 
     * @param length the length
     * @return the string
     */
    public static String generateLongString(int length){
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i <= length+1; i++){
            sb.append('a');
        }
        return sb.toString();
    }

}
