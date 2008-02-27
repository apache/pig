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
package org.apache.pig.backend.hadoop.executionengine;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.impl.FunctionInstantiator;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.eval.cond.Cond;
import org.apache.pig.impl.logicalLayer.LOSplit;
import org.apache.pig.impl.io.FileLocalizer;


public class SplitSpec implements Serializable{
    private static final long serialVersionUID = 1L;
    
    private final Log log = LogFactory.getLog(getClass());
    public ArrayList<Cond> conditions;
    public List<String> tempFiles;
       
    private static String getTempFile(PigContext pigContext) throws IOException {
        return FileLocalizer.getTemporaryPath(null, pigContext).toString();
    }
    
    public SplitSpec(LOSplit lo, PigContext pigContext){
        this.conditions = lo.getConditions();
        tempFiles = new ArrayList<String>();
       try{
            for (int i=0; i<conditions.size(); i++){
                tempFiles.add(getTempFile(pigContext));
            }
        }catch (IOException e){
            log.error(e);
        }
    }
        
    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        for (int i=0; i<conditions.size(); i++){
            if (i!=0) sb.append(";");
            sb.append(conditions.get(i).toString());
            sb.append(";");
            sb.append(tempFiles.get(i));
        }
        return sb.toString();
    }
    public void instantiateFunc(FunctionInstantiator instantiaor)
        throws IOException {
        for(Cond condition : conditions)
            condition.instantiateFunc(instantiaor);        
    }
}
