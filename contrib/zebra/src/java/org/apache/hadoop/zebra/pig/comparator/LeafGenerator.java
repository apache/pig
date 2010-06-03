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

import java.io.PrintStream;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

/**
 * Generate partial binary key for a leaf expression.
 */
public final class LeafGenerator {
  final LeafExpr leaf; // may be null to represent adding '0'
  final int escapeLevel;
  final int comescLevel;
  final boolean complement;

  /**
   * @param leaf
   *          The leaf expression
   * @param el
   *          escape level
   * @param cel
   *          complement escape level
   * @param complement
   *          complement the value
   */
  public LeafGenerator(LeafExpr leaf, int el, int cel, boolean complement) {
    this.leaf = leaf;
    this.escapeLevel = el;
    this.comescLevel = cel;
    this.complement = complement;
  }

  void append(EncodingOutputStream out, Tuple tuple) throws ExecException {
    out.setEscapeParams(escapeLevel, comescLevel, complement);
    if (leaf == null) { // add a '\0'
      out.write(0x0);
    } else {
      leaf.append(out, tuple);
    }
  }

  void illustrate(PrintStream out) {
    if (leaf == null) {
      out.printf("(%s, NA, %d, %d, %b)", "NULL", escapeLevel, comescLevel,
          complement);
    } else {
      leaf.illustrate(out, escapeLevel, comescLevel, complement);
    }
  }
}
