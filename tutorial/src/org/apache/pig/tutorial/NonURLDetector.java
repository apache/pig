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
import java.util.List;
import java.util.ArrayList;

import org.apache.pig.FilterFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;

/**
 * This function removes search queries that are URLs (as defined by _urlPattern).
 * This function also removes empty queries.
 */
public class NonURLDetector extends FilterFunc {

  private Pattern _urlPattern = Pattern.compile("^[\"]?(http[:|;])|(https[:|;])|(www\\.)");
  
  public Boolean exec(Tuple arg0) throws IOException {
      if (arg0 == null || arg0.size() == 0)
          return false;

    String query; 
    try{
        query = (String)arg0.get(0);
        if(query == null)
            return false;
        query = query.trim();
    }catch(Exception e){
        System.err.println("NonURLDetector: failed to process input; error - " + e.getMessage());
        return false;
    }

    if (query.equals("")) {
      return false;
    }
    Matcher m = _urlPattern.matcher(query);
    if (m.find()) {
      return false;
    }
    return true;
  }
  
  /* (non-Javadoc)
   * @see org.apache.pig.EvalFunc#getArgToFuncMapping()
   * This is needed to make sure that both bytearrays and chararrays can be passed as arguments
   */
  @Override
  public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
      List<FuncSpec> funcList = new ArrayList<FuncSpec>();
      funcList.add(new FuncSpec(this.getClass().getName(), new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY))));

      return funcList;
  }

}
