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
package org.apache.pig.impl.eval;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Datum;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.FunctionInstantiator;
import org.apache.pig.impl.eval.collector.DataCollector;
import org.apache.pig.impl.logicalLayer.schema.Schema;


public class SortDistinctSpec extends EvalSpec {
    private static final long serialVersionUID = 1L;
    transient DataBag bag;
    protected EvalSpec sortSpec;
    protected boolean eliminateDuplicates;
    
    
    public SortDistinctSpec(boolean eliminateDuplicates, EvalSpec sortSpec){
        this.eliminateDuplicates = eliminateDuplicates;
        this.sortSpec = sortSpec;
    }
        
    @Override
    public List<String> getFuncs() {
        if (sortSpec!=null)
            return sortSpec.getFuncs();
        else
            return new ArrayList<String>();
    }

    @Override
    protected Schema mapInputSchema(Schema schema) {
        return schema;
    }

    @Override
    protected DataCollector setupDefaultPipe(Properties properties,
                                             DataCollector endOfPipe) {
        return new DataCollector(endOfPipe){
            
            @Override
            public void add(Datum d) {
                if (inTheMiddleOfBag){
                    if (checkDelimiter(d)){
                        addToSuccessor(bag);
                    }else{
                        if (d instanceof Tuple){
                            bag.add((Tuple)d);
                        }else{
                            bag.add(new Tuple(d));
                        }
                    }
                }else{
                    if (checkDelimiter(d)){
                        //Bag must have started now
                        if (eliminateDuplicates) {
                            bag = BagFactory.getInstance().newDistinctBag();
                        } else {
                            bag = BagFactory.getInstance().newSortedBag(sortSpec);
                        }
                    }else{
                        addToSuccessor(d);
                    }
                }
            }
            
            @Override
            protected boolean needFlatteningLocally() {
                return true;
            }
            
            
            @Override
            protected void finish() {
            
                /*
                 * To clear the temporary files if it was a big bag
                 */
                if (bag!=null)
                    bag.clear();
            
            }
        };
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        sb.append(eliminateDuplicates?"DISTINCT ":"SORT ");
        if (sortSpec!=null)
            sb.append(sortSpec.toString());
        sb.append("]");
        return sb.toString();
    }

    @Override
    public void instantiateFunc(FunctionInstantiator instantiaor)
            throws IOException {
        super.instantiateFunc(instantiaor);
        if (sortSpec!=null)
            sortSpec.instantiateFunc(instantiaor);        
    }

    @Override
    public void visit(EvalSpecVisitor v) {
        v.visitSortDistinct(this);
    }

    public EvalSpec getSortSpec() { return sortSpec; }

    public boolean distinct() { return eliminateDuplicates; }
    
    
}
