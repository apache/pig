/**
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

package org.apache.hadoop.zebra.types;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.data.DefaultTuple;
import org.apache.pig.impl.util.TupleFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Zebra's implementation of Pig's Tuple.
 * It's derived from Pig's DefaultTuple implementation.
 * 
 */
public class ZebraTuple extends DefaultTuple {
  protected static final Log LOG = LogFactory.getLog(TupleFormat.class);
  
  private static final long serialVersionUID = 1L;
  
  /**
   * Default constructor.  This constructor is public so that hadoop can call
   * it directly.  However, inside pig you should never be calling this
   * function.  Use TupleFactory instead.
   */
  public ZebraTuple() {
    super();
  }
  
  /** 
   * Construct a tuple with a known number of fields.  Package level so
   * that callers cannot directly invoke it.
   * @param size Number of fields to allocate in the tuple.
   */
  ZebraTuple(int size) {
    mFields = new ArrayList<Object>(size);
    for (int i = 0; i < size; i++) mFields.add(null);
  }   

  /** 
   * Construct a tuple from an existing list of objects.  Package
   * level so that callers cannot directly invoke it.
   * @param c List of objects to turn into a tuple.
   */
  ZebraTuple(List<Object> c) {
    mFields = new ArrayList<Object>(c.size());

    Iterator<Object> i = c.iterator();
    int field;
    for (field = 0; i.hasNext(); field++) mFields.add(field, i.next());
  }

  /**
   * Construct a tuple from an existing list of objects.  Package
   * level so that callers cannot directly invoke it.
   * @param c List of objects to turn into a tuple.  This list will be kept
   * as part of the tuple.
   * @param junk Just used to differentiate from the constructor above that
   * copies the list.
   */
  ZebraTuple(List<Object> c, int junk) {
    mFields = c;
  }

  @Override
  public String toString() {
    CsvZebraTupleOutput csvZebraTupleOutput = CsvZebraTupleOutput.createCsvZebraTupleOutput();
    csvZebraTupleOutput.writeTuple(this);
    
    return csvZebraTupleOutput.toString();
  }
}