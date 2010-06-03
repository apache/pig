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
package org.apache.hadoop.zebra.pig.comparator;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

/**
 * Extended from ByteArrayOutputStream with direct access to the underlying byte
 * array and explicit capacity expansion.
 * 
 * Also adding the capability of escaping:
 * 
 * <code>
 * el - escape level for 0x00, valid value 0-252
 * cel - escape level for 0xff, valid value 0-252
 *                  escaped
 * el       0x00            0x01        others
 * 0        AS-IS           AS-IS       AS-IS
 * >0       0x01 0x01+el    0x01FD      AS-IS
 *                  escaped             
 * cel      0xFF            0xFE        others
 * 0        AS-IS           AS-IS       AS-IS
 * >0       0xFE 0xFE-cel   0xFE02      AS-IS
 * </code>
 */
class EncodingOutputStream extends ByteArrayOutputStream {
  int escapeLevel = 0;
  int comescLevel = 0;
  boolean complement = false;

  public EncodingOutputStream() {
    super();
  }

  public EncodingOutputStream(int size) {
    super(size);
  }

  public byte[] get() {
    return buf;
  }

  public void ensureAvailable(int len) {
    int newcount = count + len;
    if (newcount > buf.length) {
      buf = Arrays.copyOf(buf, Math.max(buf.length << 1, newcount));
    }
  }

  public void setEscapeParams(int el, int cel, boolean c) {
    escapeLevel = el;
    comescLevel = cel;
    complement = c;
  }
  
  public int getEscapeLevel() {
    return escapeLevel;
  }

  public int getComescLevel() {
    return comescLevel;
  }

  public boolean getComplement() {
    return complement;
  }

  void writeEscaped(int v, boolean c) {
    ensureAvailable(2);
    buf[count] = 0x01;
    buf[count + 1] = (byte) v;
    if (c) {
      buf[count] = (byte) (~buf[count]);
      buf[count + 1] = (byte) (~buf[count + 1]);
    }
    count += 2;
  }

  /**
   * Write an escaped 0x00.
   */
  void escape00() {
    writeEscaped(escapeLevel + 1, complement);
  }

  /**
   * Write an escaped 0x01.
   */
  void escape01() {
    writeEscaped(0xFD, complement);
  }

  /**
   * Write an escaped 0xFE
   */
  void escapeFE() {
    writeEscaped(0xFD, !complement);
  }

  /**
   * write an escaped 0xFF
   */
  void escapeFF() {
    writeEscaped(comescLevel + 1, !complement);
  }

  void complement(byte b[], int begin, int end) {
    if (begin >= end) return;
    ensureAvailable(end - begin);
    if (!complement) {
      System.arraycopy(b, begin, buf, count, end - begin);
      count += (end - begin);
    } else {
      for (int i = begin; i < end; ++i) {
        buf[count++] = (byte) ~b[i];
      }
    }
  }

  void escape(int b) {
    switch (b) {
      case 0:
        escape00();
        break;
      case 1:
        escape01();
        break;
      case 0xfe:
        escapeFE();
        break;
      case 0xff:
        escapeFF();
        break;
    }
  }

  public void write(int b) {
    if (!shouldEscape(b, escapeLevel > 0, comescLevel > 0)) {
      ensureAvailable(1);
      if (complement) {
        buf[count++] = (byte) ~b;
      } else {
        buf[count++] = (byte) b;
      }
    } else {
      escape(b);
    }
  }

  public void write(byte b[]) {
    write(b, 0, b.length);
  }

  static boolean shouldEscape(int b, boolean checkLow, boolean checkHigh) {
    if (checkLow && b < 0x2) return true;
    if (checkHigh && b > 0xfd) return true;
    return false;
  }

  public void write(byte b[], int off, int len) {
    if ((escapeLevel > 0) || (comescLevel > 0)) {
      ensureAvailable(len);
      int begin = off;
      int next = begin;
      int end = off + len;
      for (; begin < end; begin = next) {
        while ((next < end)
            && (!shouldEscape(b[next] & 0xff, escapeLevel > 0, comescLevel > 0))) {
          ++next;
        }
        complement(b, begin, next);
        if (next < end) {
          escape(b[next] & 0xff);
          ++next;
        }
      }
    } else {
      complement(b, off, off + len);
    }
  }
}

/**
 * Generating binary keys for algorithmic comparators. A user may construct an
 * algorithmic comparator by creating a ComparatorExpr object (through various
 * static methods in this class). She could then create a KeyGenerator object
 * and use it to create binary keys for tuple. The KeyGenerator object can be
 * reused for different tuples that conform to the same schema. Sorting the
 * tuples by the binary key yields the same ordering as sorting by the
 * algorithmic comparator.
 * 
 * Basic idea (without optimization):
 * <ul>
 * <li>define two operations: escape and complement, that takes in a byte array,
 * and outputs a byte array:
 * 
 * <pre>
 * escape(byte[] bytes) {
 *   for (byte b : bytes) {
 *     if (b == 0)
 *       emit(0x1, 0x0);
 *     else if (b == 1)
 *       emit(0x1, 0x2);
 *     else emit(b);
 *   }
 * }
 * 
 * complement(byte[] bytes) {
 *   for (byte b : bytes) {
 *     emit(&tilde;b);
 *   }
 * }
 * </pre>
 * 
 * <li>find ways to convert primitive types to bytes that compares in the same
 * order as those objects.
 * <li>operations:
 * <ul>
 * <li>negate(byte[] bytes) == complement(escape(bytes) + 0x0);
 * <li>tuple(byte[] bytes1, byte[] bytes2) == escape(bytes1) + 0x0 +
 * escape(bytes2)
 * <li>bag(byte[] bytes1, byte[] bytes2, ... ) = escape(bytes1) + 0x0 +
 * escape(bytes2) + ...
 * </ul>
 * <li>optimizations:
 * <ul>
 * <li>negate(negate(bytes)) == bytes;
 * <li>tuple(a) == a;
 * <li>tuple(a, tuple(b, c)) == tuple(a, b, c)
 * <li>the actual output would be a concatenation of f1(o1), f2(o2), ..., where
 * o1, o2, are leaf datums in the tuple or 0x0, and fi(oi) is a nested function
 * of escape() and complement() calls.
 * <li>The invariance we want to preserve is that escape(0x1) >
 * escape(escape(0x0)) > escape(0x0) > 0x0. In the basic algorithm, these are
 * escaped as 0x0102, 0x010100, 0x0100, 0x00, and are thus variable length. We
 * can actually collapse nested consecutive calls of
 * escape(escape(...escape(0))...) to escape(i, 0), where i is the level of
 * nesting, and fi may be represented as nested inter-leaved calling of
 * complement(bytes) and escape(i, bytes), where escape (i, 0x0) == 0x01 +
 * (0x01+i) and escape(i, 1) == 0x010xFD. We do limit the total nesting depth by
 * 252, which should be plenty.
 * <li>we can further optimize fi as either escape(i, j, bytes) or
 * complement(escape(i, j, bytes), where i is the level of nesting for escaping
 * 0x0 and 0x1, and j the level of nesting for escaping 0xff and 0xfe.
 * <li>If the binary keys being generated from a certain comparator either
 * compare equal or differ at some byte position, but never the case where one
 * is a prefix of another, then we do not need to add padding 0x0 for negate()
 * or tuple(). This is captured by the method implicitBound() in ComparatorExpr.
 * <li>We figure out how datums should be extracted from a tuple and being
 * escaped only once for any expression, and write to a modified
 * ByteArrayOutputStream in one pass.
 * </ul>
 * </ul>
 * 
 * TODO Remove the strong dependency with Pig by adding a DatumExtractor
 * interface that allow applications to extract leaf datum from user objects,
 * something like the following:
 * 
 * <pre>
 * interface DatumExtractor {
 *   Object extract(Object o);
 * }
 * </pre>
 * 
 * And user may do something like this:
 * 
 * <pre>
 * class MyObject {
 *  int a;
 *  String b;
 * }
 * 
 * ComparatorExpr expr = KeyBuilder.createLeafExpr(new DatumExtractor {
 *  Object extract(Object o) {
 *      MyObject obj = (MyObject)o;
 *      return obj.b;
 *  } }, DataType.CHARARRAY);
 * </pre>
 * 
 * TODO Change BagExpr to IteratorExpr, so that it may be used in more general
 * context (any Java collection).
 * 
 * TODO Add an ArrayExpr (for Java []).
 */
public class KeyGenerator {
  private EncodingOutputStream out;
  private List<LeafGenerator> list;

  /**
   * Create a key builder that can generate binary keys for the input key
   * expression.
   * 
   * @param expr
   *          comparator expression
   */
  public KeyGenerator(ComparatorExpr expr) {
    out = new EncodingOutputStream();
    list = new ArrayList<LeafGenerator>();
    expr.appendLeafGenerator(list, 0, 0, false, false);
    // illustrate(System.out);
  }

  /**
   * Reset the key builder for a new expression.
   * 
   * @param expr
   *          comparator expression
   */
  public void reset(ComparatorExpr expr) {
    list.clear();
    expr.appendLeafGenerator(list, 0, 0, false, false);
  }

  /**
   * Generate the binary key for the input tuple
   * 
   * @param t
   *          input tuple
   * @return A {@link BytesWritable} containing the binary sorting key for the
   *         input tuple.
   * @throws ExecException
   */
  public BytesWritable generateKey(Tuple t) throws ExecException {
    out.reset();
    for (Iterator<LeafGenerator> it = list.iterator(); it.hasNext();) {
      LeafGenerator e = it.next();
      e.append(out, t);
    }
    BytesWritable ret = new BytesWritable();
    ret.set(out.get(), 0, out.size());
    return ret;
  }

  /**
   * Illustrate how the key would be generated from source.
   * 
   * @param ps
   *          The output print stream.
   */
  public void illustrate(PrintStream ps) {
    for (Iterator<LeafGenerator> it = list.iterator(); it.hasNext();) {
      LeafGenerator e = it.next();
      e.illustrate(ps);
    }
    ps.print("\n");
  }
}
