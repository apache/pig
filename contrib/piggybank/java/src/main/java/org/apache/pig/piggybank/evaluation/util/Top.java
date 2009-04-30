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
package org.apache.pig.piggybank.evaluation.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * TopN UDF accepts a bag of tuples and returns top-n tuples depending upon the
 * tuple field value of type long. Both n and field number needs to be provided
 * to the UDF. The UDF iterates through the input bag and just retains top-n
 * tuples by storing them in a priority queue of size n+1 where priority is the
 * long field. This is efficient as priority queue provides constant time - O(1)
 * removal of the least element and O(log n) time for heap restructuring. The
 * UDF is especially helpful for turning the nested grouping operation inside
 * out and retaining top-n in a nested group. 
 * 
 * Sample usage: 
 * A = LOAD 'test.tsv' as (first: chararray, second: chararray); 
 * B = GROUP A BY (first, second);
 * C = FOREACH B generate FLATTEN(group), COUNT(*) as count;
 * D = GROUP C BY first; // again group by first 
 * topResults = FOREACH D { 
 *          result = Top(10, 2, C); // and retain top 10 occurrences of 'second' in first 
 *          GENERATE FLATTEN(result); 
 *  }
 */
public class Top extends EvalFunc<DataBag> {

  BagFactory mBagFactory = BagFactory.getInstance();

  static class TupleComparator implements Comparator<Tuple> {
    private int fieldNum;

    public TupleComparator(int fieldNum) {
      this.fieldNum = fieldNum;
    }

    @Override
    public int compare(Tuple o1, Tuple o2) {
      if (o1 == null)
        return -1;
      if (o2 == null)
        return 1;
      int retValue = 1;
      try {
        long count1 = (Long) o1.get(fieldNum);
        long count2 = (Long) o2.get(fieldNum);
        retValue = (count1 > count2) ? 1 : ((count1 == count2) ? 0 : -1);
      } catch (ExecException e) {
        throw new RuntimeException("Error while comparing o1:" + o1
            + " and o2:" + o2, e);
      }
      return retValue;
    }
  }

  @Override
  public DataBag exec(Tuple tuple) throws IOException {
    if (tuple == null || tuple.size() < 3) {
      return null;
    }
    try {
      int n = (Integer) tuple.get(0);
      int fieldNum = (Integer) tuple.get(1);
      DataBag inputBag = (DataBag) tuple.get(2);
      PriorityQueue<Tuple> store = new PriorityQueue<Tuple>(n + 1,
          new TupleComparator(fieldNum));
      Iterator<Tuple> itr = inputBag.iterator();
      while (itr.hasNext()) {
        Tuple t = itr.next();
        store.add(t);
        if (store.size() > n)
          store.poll();
      }
      DataBag outputBag = mBagFactory.newDefaultBag();
      for (Tuple t : store) {
        outputBag.add(t);
      }
      return outputBag;
    } catch (ExecException e) {
      throw new RuntimeException("ExecException executing function: ", e);
    } catch (Exception e) {
      throw new RuntimeException("General Exception executing function: " + e);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.pig.EvalFunc#getArgToFuncMapping()
   */
  @Override
  public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
    List<FuncSpec> funcList = new ArrayList<FuncSpec>();
    funcList.add(new FuncSpec(this.getClass().getName(), new Schema(
        new Schema.FieldSchema(null, DataType.INTEGER))));
    funcList.add(new FuncSpec(this.getClass().getName(), new Schema(
        new Schema.FieldSchema(null, DataType.INTEGER))));
    funcList.add(new FuncSpec(this.getClass().getName(), new Schema(
        new Schema.FieldSchema(null, DataType.BAG))));
    return funcList;
  }

  @Override
  public Schema outputSchema(Schema input) {
    try {
      if (input.size() < 3) {
        return null;
      }
      Schema.FieldSchema bagFs = new Schema.FieldSchema("bag_of_input_tuples",
          input.getField(2).schema, DataType.BAG);
      return new Schema(bagFs);

    } catch (Exception e) {
      return null;
    }
  }
}
