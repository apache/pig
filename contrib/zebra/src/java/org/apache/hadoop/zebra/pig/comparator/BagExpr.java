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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

public class BagExpr extends LeafExpr {
  protected List<LeafGenerator> list;
  protected final ComparatorExpr expr;
  protected int el, cel;
  protected boolean c;

  public BagExpr(int index, ComparatorExpr expr) {
    super(index);
    this.expr = expr;
  }

  @Override
  protected
  void appendObject(EncodingOutputStream out, Object object)
      throws ExecException {

    if (list == null) {
      // This is the first time we get called. build the execution plan.
      el = out.getEscapeLevel();
      cel = out.getComescLevel();
      c = out.getComplement();
      list = new ArrayList<LeafGenerator>();
      // requiring the individual items to be explicitly bounded.
      expr.appendLeafGenerator(list, el, cel, c, true);
    }

    DataBag bag = (DataBag) object;
    for (Iterator<Tuple> it = bag.iterator(); it.hasNext();) {
      Tuple t = it.next();
      for (Iterator<LeafGenerator> it2 = list.iterator(); it2.hasNext();) {
        LeafGenerator g = it2.next();
        g.append(out, t);
      }
    }
  }

  @Override
  protected
  boolean implicitBound() {
    return true;
  }

  public void illustrate(PrintStream out, int escapeLevel, int comescLevel,
      boolean complement) {
    List<LeafGenerator> l = new ArrayList<LeafGenerator>();
    expr.appendLeafGenerator(l, escapeLevel, comescLevel, complement, true);
    out.printf("(%s, %d, [", getType(), index);
    for (Iterator<LeafGenerator> it = l.iterator(); it.hasNext();) {
      LeafGenerator leaf = it.next();
      leaf.illustrate(out);
    }
    out.print("])");
  }

  @Override
  protected
  String getType() {
    return "Bag";
  }

  @Override
  protected
  void toString(PrintStream out) {
    out.printf("%s(%d, ", getType(), index);
    expr.toString(out);
    out.print(")");
  }
}
