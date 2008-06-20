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
import java.util.HashSet;
import java.util.Set;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataAtom;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

/**
 * This function divides a search query string into wrods and extracts
 * n-grams with up to _ngramSizeLimit length.
 * Example 1: if query = "a real nice query" and _ngramSizeLimit = 2,
 * the query is split into: a, real, nice, query, a real, real nice, nice query
 * Example 2: if record = (u1, h1, pig hadoop) and _ngramSizeLimit = 2,
 * the record is split into: (u1, h1, pig), (u1, h1, hadoop), (u1, h1, pig hadoop)
 */
public class NGramGenerator extends EvalFunc<DataBag> {

  private static final int _ngramSizeLimit = 2;
  
  public void exec(Tuple arg0, DataBag arg1) throws IOException {
    String query = arg0.getAtomField(0).strval();
    String[] words = TutorialUtil.splitToWords(query);
    Set<String> ngrams = new HashSet<String>();
    TutorialUtil.makeNGram(words, ngrams, _ngramSizeLimit);
    for (String ngram : ngrams) {
      Tuple t = new Tuple();
      t.appendField(new DataAtom(ngram));
      arg1.add(t);
    }
  }
}
