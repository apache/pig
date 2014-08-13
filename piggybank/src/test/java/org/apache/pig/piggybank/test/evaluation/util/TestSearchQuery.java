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
package org.apache.pig.piggybank.test.evaluation.util;

import org.apache.pig.piggybank.evaluation.util.SearchQuery;

import org.apache.pig.data.DefaultTupleFactory;
import org.apache.pig.data.Tuple;
import org.junit.Test;

import junit.framework.TestCase;

public class TestSearchQuery extends TestCase {

  @Test
  public void testSearchQuery() throws Exception {
    String[] searchUrls = {
        "http://www.google.co.in/advanced_search?q=google+Advanced+Query&hl=en",
        "http://www.google.co.in/search?hl=en&as_q=google+simple+Query&as_epq=&as_oq=&as_eq=&num=10&lr=&as_filetype=&ft=i&as_sitesearch=&as_qdr=all&as_rights=&as_occt=any&cr=&as_nlo=&as_nhi=&safe=images",
        "http://search.live.com/results.aspx?q=live+Query&go=&form=QBRE",
        "http://search.aol.com/aol/search?s_it=searchbox.webhome&q=aol+query",
        "http://search.yahoo.com/search;_ylt=A0geu8zce8NJQxYBmgal87UF?p=Yahoo+query&fr=sfp&fr2=&iscqry=" };
    String[] queries = { "google advanced query", "google simple query",
        "live query", "aol query", "yahoo query" };
    SearchQuery sq = new SearchQuery();
    Tuple tuple = DefaultTupleFactory.getInstance().newTuple(1);
    for (int i = 0; i < searchUrls.length; i++) {
      tuple.set(0, searchUrls[i]);
      super.assertEquals(sq.exec(tuple), queries[i]);
    }
  }
}
