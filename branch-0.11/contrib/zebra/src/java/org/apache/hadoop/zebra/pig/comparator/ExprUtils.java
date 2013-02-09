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
import java.util.Collection;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;

public class ExprUtils {

  /**
   * Make a bag comparator expression.
   * 
   * @param index
   *          the index of datum in the source tuple that is a {@link DataBag}
   *          object.
   * @param expr
   *          a comparator expression that corresponds to the member tuples in
   *          the bag.
   * @return A comparator expression.
   * 
   *         <p>
   *         Example: suppose we have tuple schema as follows: <code>
   * Tuple {
   *    int a;
   *    String b;
   *    Bag {
   *        Tuple {
   *            Bytes c;
   *            int d;
   *            String e;
   *        }
   *     } f
   * }
   * </code>
   * 
   *         We would like to sort by
   *         <code>Tuple(b, Bag(Negate(Tuple(e, c))))</code>, we can construct
   *         the ComparatorExpr as follows; <code>
   * ComparatorExpr expr = tupleComparator(
   *    leafComparator(1, DataType.CHARARRAY), 
   *    bagComparator(2, 
   *        negateComparator(
   *            tupleComparator(
   *                leafCmparator(2, DataType.CHARARRAY),
   *                leafComparator(0, DataType.BYTEARRAY)))))
   * </code>
   */
  public static ComparatorExpr bagComparator(int index, ComparatorExpr expr) {
    return new BagExpr(index, expr);
  }

  /**
   * Converting an expression to a string.
   * 
   * @param expr
   * @return A string representation of the comparator expression.
   */
  public static String exprToString(ComparatorExpr expr) {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(bout);
    expr.toString(out);
    out.close();
    return new String(bout.toByteArray());
  }

  /**
   * Comparator for primitive types.
   * 
   * @param index
   *          Index in the source tuple.
   * @param type
   *          One of the constants defined in {@link DataType} for primitive
   *          types.
   * @return Comparator expression.
   */
  public static ComparatorExpr primitiveComparator(int index, int type) {
    switch (type) {
      case DataType.BOOLEAN:
        return new BooleanExpr(index);
      case DataType.BYTE:
        return new ByteExpr(index);
      case DataType.BYTEARRAY:
        return new BytesExpr(index);
      case DataType.CHARARRAY:
        return new StringExpr(index);
      case DataType.DOUBLE:
        return new DoubleExpr(index);
      case DataType.FLOAT:
        return new FloatExpr(index);
      case DataType.INTEGER:
        return new IntExpr(index);
      case DataType.LONG:
        return new LongExpr(index);
      case DataType.DATETIME:
        return new DateTimeExpr(index);
      default:
        throw new RuntimeException("Not a prmitive PIG type");
    }
  }

  /**
   * Negate comparator
   * 
   * @param expr
   *          expression to perform negation on.
   * @return A comparator expression.
   */
  public static ComparatorExpr negationComparator(ComparatorExpr expr) {
    return NegateExpr.makeNegateExpr(expr);
  }

  /**
   * Make a Tuple comparator expression.
   * 
   * @param exprs
   *          member comparator expressions.
   * @return A comparator expression.
   */
  public static ComparatorExpr tupleComparator(
      Collection<? extends ComparatorExpr> exprs) {
    return TupleExpr.makeTupleComparator(exprs);
  }

  /**
   * Make a Tuple comparator expression.
   * 
   * @param exprs
   *          member comparator expressions.
   * @return A comparator expression.
   */
  public static ComparatorExpr tupleComparator(ComparatorExpr... exprs) {
    return TupleExpr.makeTupleExpr(exprs);
  }

}
