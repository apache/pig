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

package org.apache.pig.impl.util;

import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class StringUtils {
    
    public static String unescapeInputString(String input)  {

         if(input == null){
            return "";
         }
         // Needed variables
         // preset the size so our StringBuilders don't have to grow
         int inputlength = input.length();       
         StringBuilder unicode = new StringBuilder(4);
         StringBuilder output = new StringBuilder(inputlength) ;
         boolean hadSlash = false;
         boolean inUnicode = false;
         
         // The main loop
         for (int i = 0; i < inputlength; i++) {
             char ch = input.charAt(i);
             // currently doing unicode mode
             if (inUnicode) {
                 unicode.append(ch);
                 if (unicode.length() == 4) {
                     // unicode now contains the four hex digits
                     try {
                         int value = Integer.parseInt(unicode.toString(), 0x10);
                         output.append((char) value) ;
                         // reuse the StringBuilder
                         unicode.setLength(0);
                         inUnicode = false;
                         hadSlash = false;
                     } catch (NumberFormatException nfe) {
                         throw new RuntimeException("Unable to parse unicode value: " + unicode, nfe);
                     }
                 }
                 continue;
             }
             if (hadSlash) {
                 // handle an escaped value
                 hadSlash = false;
                 switch (ch) {
                     case '\\':
                         output.append('\\');
                         break;
                     case '\'':
                         output.append('\'');
                         break;
                     case 'r':
                         output.append('\r');
                         break;
                     case 'f':
                         output.append('\f');
                         break;
                     case 't':
                         output.append('\t');
                         break;
                     case 'n':
                         output.append('\n');
                         break;
                     case 'b':
                         output.append('\b');
                         break;
                     case 'u':
                         {
                             // switch to unicode mode
                             inUnicode = true;
                             break;
                         }
                     default :
                         output.append(ch);
                         break;
                 }
                 continue;
             } else if (ch == '\\') {
                 hadSlash = true;
                 continue;
             }
             output.append(ch);
         }
         
         return output.toString() ;
     }
     
     public static String join(AbstractCollection<String> s, String delimiter) {
         if (s.isEmpty()) return "";
         Iterator<String> iter = s.iterator();
         StringBuffer buffer = new StringBuffer(iter.next());
         while (iter.hasNext()) {
             buffer.append(delimiter);
             buffer.append(iter.next());
         }
         return buffer.toString();
     }
     
         
     public static String[] getPathStrings(String commaSeparatedPaths) {
         int length = commaSeparatedPaths.length();
         int curlyOpen = 0;
         int pathStart = 0;
         boolean globPattern = false;
         List<String> pathStrings = new ArrayList<String>();
     
         for (int i=0; i<length; i++) {
             char ch = commaSeparatedPaths.charAt(i);
             switch(ch) {
                 case '{' : {
                     curlyOpen++;
                     if (!globPattern) {
                         globPattern = true;
                     }
                     break;
                 }
                 case '}' : {
                     curlyOpen--;
                     if (curlyOpen == 0 && globPattern) {
                         globPattern = false;
                     }
                     break;
                 }
                 case ',' : {
                     if (!globPattern) {
                         pathStrings.add(commaSeparatedPaths.substring(pathStart, i));
                         pathStart = i + 1 ;
                     }
                     break;
                 }
             }
         }
         pathStrings.add(commaSeparatedPaths.substring(pathStart, length));
     
         return pathStrings.toArray(new String[0]);
     }
}
