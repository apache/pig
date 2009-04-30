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
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * This small UDF takes a search engine URL (Google/Yahoo/AOL/Live) containing
 * the search query and extracts it. The URL is assumed to be encoded. The query
 * is normalized, converting it to lower-case, removing punctuations, removing
 * extra spaces.
 */
public class SearchQuery extends EvalFunc<String> {
  private static Pattern queryPattern = Pattern
      .compile("(?<=([\\&\\?](as_)?[pq]=)).*?(\\z|(?=[\\&\\\"]))");

  @Override
  public String exec(Tuple tuple) throws IOException {
    if (tuple == null || tuple.size() < 1) {
      return null;
    }
    try {
      String refURL = (String) tuple.get(0);
      return extractQuery(refURL);
    } catch (ExecException ee) {
      throw new IOException(ee);
    }
  }

  private String extractQuery(String url) {
    try {
      String refURL = url;
      if (refURL == null || refURL.isEmpty())
        return refURL;
      String query = null;
      refURL = refURL.toLowerCase().trim();
      Matcher matcher = queryPattern.matcher(refURL);
      if (matcher.find()) {
        query = matcher.group();
        query = URLDecoder.decode(query, "UTF-8"); // decode query
        query = query.replaceAll("[\\p{Punct}]+", ""); // remove punctuation
        query = query.trim().replaceAll("[\\s]+", " "); // remove extra spaces
        query = (query.length() > 80) ? query.substring(0, 80) : query;
      }
      return query;
    } catch (UnsupportedEncodingException ignore) { // should never happen
    }
    return null;
  }

  /* (non-Javadoc)
   * @see org.apache.pig.EvalFunc#getArgToFuncMapping()
   */
  @Override
  public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
      List<FuncSpec> funcList = new ArrayList<FuncSpec>();
      funcList.add(new FuncSpec(this.getClass().getName(), new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY))));
      return funcList;
  }
  @Override
  public Schema outputSchema(Schema input) {
    try {
      Schema s = new Schema();
      s.add(new Schema.FieldSchema("query", DataType.CHARARRAY));
      return s;
    } catch (Exception e) {
      return null;
    }
  }
}
