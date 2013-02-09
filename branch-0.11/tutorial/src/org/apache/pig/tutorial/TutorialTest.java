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
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class TutorialTest {

    private static Tuple[] getTuples(String[] queries) {
        Tuple[] tuples = new Tuple[queries.length];
        for (int i = 0; i < tuples.length; i++) {
            tuples[i] = TupleFactory.getInstance().newTuple(1);
            try{tuples[i].set(0, queries[i]);}catch(Exception e){}
        }
        return tuples;
    }
  
    public static String[] testDataAtomEvals(EvalFunc<String> eval, Tuple[] tuples) {
        List<String> res = new ArrayList<String>();
        try {
            for (Tuple t : tuples) {
                String output = eval.exec(t);
                System.out.println("Converted: " + t + " to (" + output + ")");
                res.add(output);
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
                DataBag output = eval.exec(t);
                System.out.println("Converted: " + t + " to (" + output + ")");
                res.add(output);
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
                    res.add((String)t.get(0));
                } else {
                    System.out.println("rejected: " + t);
                }
            }
        } catch (Exception e) {
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

        DataBag bag = DefaultBagFactory.getInstance().newDefaultBag();
    
        Tuple t1 = TupleFactory.getInstance().newTuple(3);
        try{
            t1.set(0, "word");
            t1.set(1, "02");
            t1.set(2, 2);
        }catch(Exception e){}
        bag.add(t1);
    
        Tuple t2 = TupleFactory.getInstance().newTuple(3);
        try{
            t2.set(0, "word");
            t2.set(1, "05");
            t2.set(2, 2);
        }catch(Exception e){}
        bag.add(t2);

        Tuple t3 = TupleFactory.getInstance().newTuple(3);
        try{
            t3.set(0, "word");
            t3.set(1, "04");
            t3.set(2, 3);
        }catch(Exception e){}
        bag.add(t3);

        Tuple t4 = TupleFactory.getInstance().newTuple(3);
        try{
            t4.set(0, "word");
            t4.set(1, "06");
            t4.set(2, 4);
        }catch(Exception e){}
        bag.add(t4);

        Tuple[] t = new Tuple[1];
        t[0] = TupleFactory.getInstance().newTuple(1);
        try{
            t[0].set(0, bag);
        }catch(Exception e){}

        ScoreGenerator eval4 = new ScoreGenerator();
        testDataBagEvals(eval4, t);
    }
}
