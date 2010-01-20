/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the
 * NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.pig.piggybank.evaluation.util.apachelogparser;


import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * HostExtractor takes a url and returns the host. For example,
 * 
 * http://sports.espn.go.com/mlb/recap?gameId=281009122
 * 
 * leads to
 * 
 * sports.espn.go.com
 * 
 * Pig latin usage looks like
 * 
 * host = FOREACH row GENERATE
 * org.apache.pig.piggybank.evaluation.util.apachelogparser.HostExtractor(referer);
 */
public class HostExtractor extends EvalFunc<String> {
  @Override
  public String exec(Tuple input) throws IOException {
    if (input == null || input.size() == 0)
      return null;
    String str="";
    try{
      str = (String)input.get(0);
      return new URL(str).getHost().toLowerCase();
    } catch (MalformedURLException me) {
      System.err.println("piggybank.evaluation.util.apachelogparser.HostExtractor: "+
          "url parsing exception for "+str);
      return null;
    } catch (Exception e) {
      throw new IOException("Caught exception processing input row ", e);
    }
  }

  @Override
  public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
    List<FuncSpec> funcList = new ArrayList<FuncSpec>();
    funcList.add(new FuncSpec(this.getClass().getName(), 
        new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY))));

    return funcList;
  }
}
