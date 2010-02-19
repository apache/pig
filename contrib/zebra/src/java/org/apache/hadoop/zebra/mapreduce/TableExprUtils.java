/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.zebra.mapreduce;

import java.io.IOException;
import java.io.StringReader;

/**
 * Utility methods for people interested in writing their own TableExpr classes.
 */
class TableExprUtils {
  private TableExprUtils() {
    // prevent instantiation
  }

  /**
   * Encode a string into a StringBuilder.
   * 
   * @param out
   *          output string builder.
   * @param s
   *          String to be encoded
   */
  public static void encodeString(StringBuilder out, String s) {
    if (s == null) {
      encodeInt(out, null);
      return;
    }
    
    encodeInt(out, s.length());
    out.append(s);
  }
  
  /**
   * Decode a string previously encoded through encodeString.
   * 
   * @param in
   *          The input StringReader.
   * @return The decoded string object.
   * @throws IOException
   */
  public static String decodeString(StringReader in) throws IOException {
    Integer len = decodeInt(in);
    if (len == null) {
      return null;
    }

    char[] chars = new char[len];
    in.read(chars);
    return new String(chars);
  }
  
  public static void encodeLong(StringBuilder out, Long l) {
    if (l != null) {
      out.append(l);
    }
    out.append(':');
  }

  public static Long decodeLong(StringReader in) throws IOException {
    int c = in.read();
    if (c == ':') {
      return null;
    }
    if (Character.isDigit(c) == false) {
      throw new IllegalArgumentException("Bad encoded string");
    }
    StringBuilder sb = new StringBuilder();
    sb.append((char) c);

    boolean colonSeen = false;
    while ((c = in.read()) != -1) {
      if (c == ':') {
        colonSeen = true;
        break;
      }
      if (!Character.isDigit(c)) {
        break;
      }
      sb.append((char) c);
    }
    if (colonSeen == false) {
      throw new IllegalArgumentException("Bad encoded string");
    }
    return new Long(sb.toString());
  }
  
  public static void encodeInt(StringBuilder out, Integer i) {
    if (i == null) {
      encodeLong(out, null);
    }
    else {
      encodeLong(out, (long) i.intValue());
    }
  }

  public static Integer decodeInt(StringReader in) throws IOException {
    Long l = decodeLong(in);
    if (l == null) return null;
    return (int) l.longValue();
  }
}
