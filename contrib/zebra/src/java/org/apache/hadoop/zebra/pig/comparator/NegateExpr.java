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
import java.util.List;

/**
 * Negate expression
 */
public class NegateExpr extends ComparatorExpr {
  protected final ComparatorExpr expr;

  /**
   * constructor
   * 
   * @param expr
   */
  protected NegateExpr(ComparatorExpr expr) {
    this.expr = expr;
  }

  @Override
  protected void appendLeafGenerator(List<LeafGenerator> list, int el, int cel,
      boolean c, boolean explicitBound) {
    expr.appendLeafGenerator(list, cel, el, !c, true);
  }

  /**
   * @return the child expression
   */
  public ComparatorExpr childExpr() {
    return expr;
  }

  @Override
  protected void toString(PrintStream out) {
    out.print("Negate(");
    expr.toString(out);
    out.print(")");
  }
  
  /**
   * Make a negate expression
   */
  public static ComparatorExpr makeNegateExpr(ComparatorExpr expr) {
    if (expr instanceof NegateExpr) {
      NegateExpr negateExpr = (NegateExpr) expr;
      return negateExpr.childExpr();
    }

    return new NegateExpr(expr);
  }
}
