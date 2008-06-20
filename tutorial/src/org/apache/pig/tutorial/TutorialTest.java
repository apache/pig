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
package org.apache.pig.tutorial;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.FilterFunc;
import org.apache.pig.data.DataAtom;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;

public class TutorialTest {

  private static Tuple[] getTuples(String[] queries) {
    Tuple[] tuples = new Tuple[queries.length];
    for (int i = 0; i < tuples.length; i++) {
      tuples[i] = new Tuple();
      tuples[i].appendField(new DataAtom(queries[i]));
    }
    return tuples;
  }
  
  public static String[] testDataAtomEvals(EvalFunc<DataAtom> eval, Tuple[] tuples) {
    
    List<String> res = new ArrayList<String>();
    try {
      for (Tuple t : tuples) {
        DataAtom atom = new DataAtom();
        eval.exec(t, atom);
        System.out.println("Converted: " + t + " to (" + atom + ")");
        res.add(atom.strval());
      }
    } catch (IOException e) {
      e.printStackTrace();
      System.exit(1);
    }

    System.out.println("===");
    return res.toArray(new String[res.size()]);
  }
  
  public static DataBag[] testDataBagEvals(EvalFunc<DataBag> eval, Tuple[] tuples) {
    
    List<DataBag> res = new ArrayList<DataBag>();
    try {
      for (Tuple t : tuples) {
        DataBag bag = new DefaultDataBag();
        eval.exec(t, bag);
        System.out.println("Converted: " + t + " to (" + bag + ")");
        res.add(bag);
      }
    } catch (IOException e) {
      e.printStackTrace();
      System.exit(1);
    }

    System.out.println("===");
    return res.toArray(new DataBag[res.size()]);
  }

  public static String[] testFilters (FilterFunc filter, Tuple[] tuples) {
    List<String> res = new ArrayList<String>();
    try {
      for (Tuple t : tuples) {
        if (filter.exec(t)) {
          System.out.println("accepted: " + t);
          res.add(t.getAtomField(0).strval());
        } else {
          System.out.println("rejected: " + t);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
      System.exit(1);
    }

    System.out.println("===");
    return res.toArray(new String[res.size()]);
  }

  public static void main(String[] args) {
    String[] queries = {
        "http://www.yahoo.com/",
        "\"http://www.yahoo.com/\"",
        "   http;//www.yahoo.com/ ",
        "https://www.yahoo.com/",
        "www.yahoo.com/",
        "\"www.yahoo.com/\"",
        "a real nice query ",
        "an UPPER CASE query",
        "  ",
        " nude picture",
        " +XXX",
        "\" +porno \"",
    };
    
    NonURLDetector filter1 = new NonURLDetector();
    String[] q1 = testFilters(filter1, getTuples(queries));

    ToLower eval1 = new ToLower();
    String[] q2 = testDataAtomEvals(eval1, getTuples(q1));
    
    String[] timestamps = {
        "970916072134",
        "970916072311",
        "970916123431",
    };
    
    ExtractHour eval2 = new ExtractHour();
    testDataAtomEvals(eval2, getTuples(timestamps));

    DataBag bag = new DefaultDataBag();
    
    Tuple t1 = new Tuple();
    t1.appendField(new DataAtom("word"));
    t1.appendField(new DataAtom("02"));
    t1.appendField(new DataAtom(2));
    bag.add(t1);
    
    Tuple t2 = new Tuple();
    t2.appendField(new DataAtom("word"));
    t2.appendField(new DataAtom("05"));
    t2.appendField(new DataAtom(2));
    bag.add(t2);

    Tuple t3 = new Tuple();
    t3.appendField(new DataAtom("word"));
    t3.appendField(new DataAtom("04"));
    t3.appendField(new DataAtom(3));
    bag.add(t3);

    Tuple t4 = new Tuple();
    t4.appendField(new DataAtom("word"));
    t4.appendField(new DataAtom("06"));
    t4.appendField(new DataAtom(4));
    bag.add(t4);

    Tuple[] t = new Tuple[1];
    t[0] = new Tuple();
    t[0].appendField(bag);

    ScoreGenerator eval4 = new ScoreGenerator();
    testDataBagEvals(eval4, t);
  }
}
