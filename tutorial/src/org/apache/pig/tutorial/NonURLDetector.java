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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

/**
 * This function removes search queries that are URLs (as defined by _urlPattern).
 * This function also removes empty queries.
 */
public class NonURLDetector extends FilterFunc {

  private Pattern _urlPattern = Pattern.compile("^[\"]?(http[:|;])|(https[:|;])|(www\\.)");
  
  public boolean exec(Tuple arg0) throws IOException {
    String query = arg0.getAtomField(0).strval().trim();
    if (query.equals("")) {
      return false;
    }
    Matcher m = _urlPattern.matcher(query);
    if (m.find()) {
      return false;
    }
    return true;
  }
  
}
