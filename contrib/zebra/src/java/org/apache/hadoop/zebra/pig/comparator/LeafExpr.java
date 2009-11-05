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

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

/**
 * Base class of comparator expressions that are the leaves of the expression
 * tree.
 */
public abstract class LeafExpr extends ComparatorExpr {
  int index;

  /**
   * Constructor
   * 
   * @param index
   *          tuple position index
   */
  protected LeafExpr(int index) {
    this.index = index;
  }

  /**
   * Default illustrator for leaf expressions.
   * 
   * @param out
   * @param escapeLevel
   * @param comescLevel
   * @param complement
   */
  protected void illustrate(PrintStream out, int escapeLevel, int comescLevel,
      boolean complement) {
    out.printf("(%s, %d, %d, %d, %b)", getType(), index, escapeLevel,
        comescLevel, complement);
  }

  protected void append(EncodingOutputStream out, Tuple tuple)
      throws ExecException {
	  
		if(index >= tuple.size())
			return;
	    Object o = tuple.get(index);
	    if (o == null) {    
	      out.write(0x0);
	      return;
	    }
	    appendObject(out, o);
  }

  protected abstract void appendObject(EncodingOutputStream out, Object object)
      throws ExecException;

  protected abstract String getType();

  protected abstract boolean implicitBound();

  protected void appendLeafGenerator(List<LeafGenerator> list, int el, int cel,
      boolean c, boolean explicitBound) {
    if (explicitBound && implicitBound()) {
      // requiring explicit bound, while the leaf does not already have it, so
      // we do escaping and add a bound at end.
      list.add(new LeafGenerator(this, el + 1, cel, c));
      list.add(new LeafGenerator(null, el, cel, c));
    } else {
      list.add(new LeafGenerator(this, el, cel, c));
    }
  }

  @Override
  protected
  void toString(PrintStream out) {
    out.printf("%s(%d)", getType(), index);
  }
}
