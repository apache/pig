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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class TupleExpr extends ComparatorExpr {
  protected final List<ComparatorExpr> exprs;

  protected TupleExpr(List<ComparatorExpr> exprs) {
    this.exprs = exprs;
  }

  @Override
  protected void appendLeafGenerator(List<LeafGenerator> list, int el, int cel,
      boolean c, boolean explicitBound) {
    int length = exprs.size();
    for (int i = 0; i < length - 1; ++i) {
      ComparatorExpr e = exprs.get(i);
      e.appendLeafGenerator(list, el, cel, c, true);
    }
    exprs.get(length - 1).appendLeafGenerator(list, el, cel, c, explicitBound);
  }

  /**
   * Get the children expressions.
   * 
   * @return The children expressions.
   */
  public List<ComparatorExpr> childrenExpr() {
    return exprs;
  }

  @Override
  protected void toString(PrintStream out) {
    out.print("Tuple");
    String sep = "(";
    for (Iterator<ComparatorExpr> it = exprs.iterator(); it.hasNext();) {
      out.printf(sep);
      it.next().toString(out);
      sep = ", ";
    }

    out.print(")");
  }
  
  /**
   * Make a tuple expression
   * 
   * @param exprs
   *          children expressions
   * @return a comparator expression
   */
  public static ComparatorExpr makeTupleExpr(ComparatorExpr... exprs) {
    if (exprs.length == 0)
      throw new IllegalArgumentException("Zero-length expression list");
    if (exprs.length == 1) return exprs[0];
    List<ComparatorExpr> aggr = new ArrayList<ComparatorExpr>(exprs.length);
    for (ComparatorExpr e : exprs) {
      if (e instanceof TupleExpr) {
        aggr.addAll(((TupleExpr) e).childrenExpr());
      } else {
        aggr.add(e);
      }
    }
    return new TupleExpr(aggr);
  }
  
  /**
   * Make a tuple expression
   * 
   * @param exprs
   *          children expressions
   * @return a comparator expression
   */
  public static ComparatorExpr makeTupleComparator(
      Collection<? extends ComparatorExpr> exprs) {
    if (exprs.size() == 0)
      throw new IllegalArgumentException("Zero-length expression list");
    if (exprs.size() == 1) return exprs.iterator().next();
    List<ComparatorExpr> aggr = new ArrayList<ComparatorExpr>(exprs.size());
    for (Iterator<? extends ComparatorExpr> it = exprs.iterator(); it.hasNext();) {
      ComparatorExpr e = it.next();
      if (e instanceof TupleExpr) {
        aggr.addAll(((TupleExpr) e).childrenExpr());
      } else {
        aggr.add(e);
      }
    }
    return new TupleExpr(aggr);
  }

}
