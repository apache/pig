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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Datum;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.FunctionInstantiator;
import org.apache.pig.impl.eval.collector.DataCollector;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.TupleSchema;


public class GenerateSpec extends EvalSpec {
    private static final long serialVersionUID = 1L;
    
    protected List<EvalSpec> specs = new ArrayList<EvalSpec>();

    protected int driver;



    public GenerateSpec(List<EvalSpec> specs){
        this.specs = specs;
        selectDriver();
    }  
    
    public GenerateSpec(EvalSpec col){
        specs.add(col);
    }
    
    public GenerateSpec getGroupBySpec(){
        
        //Adding a new start spec to get the group by spec. The new star spec that
        //we are adding should not contain a schema since that can cause conflicts.
        
        StarSpec ss = new StarSpec();
        ss.setSchema(new TupleSchema());
        specs.add(ss);
        return new GenerateSpec(specs);
    }
    
    @Override
    protected DataCollector setupDefaultPipe(Properties properties,
                                             DataCollector endOfPipe) {
        return new DataCollector(endOfPipe){
            LinkedList<CrossProductItem> pendingCrossProducts = new LinkedList<CrossProductItem>();
            
            @Override
            public void add(Datum d) {
                
                if (checkDelimiter(d))
                    throw new RuntimeException("Internal error: not expecting a delimiter tuple");
                
                // general case (use driver method):
                CrossProductItem cpi = new CrossProductItem(d, successor);
                
                pendingCrossProducts.addLast(cpi);

                
                //Since potentially can return without filling output, mark output as stale
                //the exec method of CrossProductItem will mark output as not stale
                successor.markStale(true);
                while (!pendingCrossProducts.isEmpty() && pendingCrossProducts.peek().isReady()){
                    pendingCrossProducts.remove().exec();
                }
            }
            
            @Override
            protected void finish() {
                 for (EvalSpec spec: specs){
                    if (specs.get(driver)!=spec)
                        spec.finish();
                }
                 
                while (!pendingCrossProducts.isEmpty()){
                    CrossProductItem cpi = pendingCrossProducts.remove();
                    cpi.waitToBeReady();
                    cpi.exec();
                }
                
                specs.get(driver).finish();
            }
            
        };
    }
                 
    private class DatumBag extends DataCollector{
        DataBag bag;
        public DatumBag(){
            super(null);
            bag = BagFactory.getInstance().newDefaultBag();
        }
        
        @Override
        public void add(Datum d){
            bag.add(new Tuple(d));
        }
        
        public Iterator<Datum> content(){
            return new Iterator<Datum>(){
                Iterator<Tuple> iter;
                {
                    iter = bag.iterator();
                }
                public boolean hasNext() {
                    return iter.hasNext();
                }
                public Datum next() {
                    return iter.next().getField(0);
                }
                public void remove() {
                    throw new RuntimeException("Can't remove from read-only iterator");
                }
            };
        }
        
    }

    private class CrossProductItem extends DataCollector{
        DatumBag[] toBeCrossed;
        Datum cpiInput;        
        
        public CrossProductItem(Datum driverInput, DataCollector successor){
            super(successor);
            this.cpiInput = driverInput;
            
            // materialize data for all to-be-crossed items
            // (except driver, which is done in streaming fashion)
            toBeCrossed = new DatumBag[specs.size()];
            for (int i = 0; i < specs.size(); i++) {
                if (i == driver)
                    continue;
                toBeCrossed[i] = new DatumBag();

                specs.get(i).setupPipe(properties, toBeCrossed[i]).add(cpiInput);
            }
        }
        
        @Override
        public void add(Datum d){
            if (checkDelimiter(d))
                throw new RuntimeException("Internal error: not expecting a delimiter tuple");
           int numItems = specs.size();
           
           // create one iterator per to-be-crossed bag
           Iterator<Datum>[] its = new Iterator[numItems];
           for (int i = 0; i < numItems; i++) {
               if (i != driver){
                   its[i] = toBeCrossed[i].content();
                   if (!its[i].hasNext())
                       return; // one of inputs is empty, so cross-prod yields empty result
               }
           }

           Datum[] lastOutput = null;
           Datum[] outData = new Datum[numItems];

           boolean done = false;
           while (!done) {
               if (lastOutput == null) { // we're generating our first output
                   for (int i = 0; i < numItems; i++) {
                       if (i == driver)
                           outData[i] = d;
                       else 
                           outData[i] = its[i].next();
                   }
               } else {
                   boolean needToAdvance = true;

                   for (int i = 0; i < numItems; i++) {
                       if (i!=driver && needToAdvance) {
                           if (its[i].hasNext()) {
                               outData[i] = its[i].next();
                               needToAdvance = false;
                           } else {
                               its[i] = toBeCrossed[i].content(); // rewind iterator
                               outData[i] = its[i].next();
                               // still need to advance some other input..
                           }
                       } else {
                           outData[i] = lastOutput[i]; // use same value as last time
                       }
                   }
               }

               // check for completion:
               done = true;
               
               for (int i = 0; i < numItems; i++) {
                   if (i!=driver && its[i].hasNext()) {
                       done = false;
                       break;
                   }
               }

               Tuple outTuple = new Tuple();
               
               for (int i=0; i< numItems; i++){
                   if (specs.get(i).isFlattened() && outData[i] instanceof Tuple){
                       Tuple t = (Tuple)outData[i];
                       if(getLineage() != null) getLineage().unionFlatten(t, outTuple);
                       for (int j=0; j < t.arity(); j++){
                           outTuple.appendField(t.getField(j));
                       }
                   }else{
                       outTuple.appendField(outData[i]);
                   }
               }
               successor.add(outTuple);

               lastOutput = outData;
           }
        }            
       
       
        public boolean isReady(){
            for (int i=0; i<toBeCrossed.length; i++){
                if (i!=driver && toBeCrossed[i].isStale())
                    return false;
            }
            return true;
        }
        
        public void waitToBeReady(){
            for (int i=0; i<toBeCrossed.length; i++){
                if (i!=driver){
                    synchronized(toBeCrossed[i]){
                        while (toBeCrossed[i].isStale()){
                            try{
                                toBeCrossed[i].wait();
                            }catch (InterruptedException e){}
                        }
                    }
                }
            }
            
        }
        
        public void exec(){
            specs.get(driver).setupPipe(properties, this).add(cpiInput);
            //log.error(Thread.currentThread().getName() + ": Executing driver on " + cpiInput);
            successor.markStale(false);
        }
        
    }

    /**
     * Driver is the column that will be used to drive the cross product. The results
     * for all other colums will be materialized into a data bag, while the results of
     * the driver column will be streamed through to form the cross product.
     *
     */
    private void selectDriver() {
        driver = 0;

        for (int i = 0; i < specs.size(); i++) {
            EvalSpec spec = specs.get(i);
            if (spec.isFlattened() || spec.isAsynchronous()){
                //This is just a heuristic that if its a flattened bag eval function, it should
                //be chosen as the driver
                if (spec instanceof CompositeEvalSpec)
                    spec = ((CompositeEvalSpec)spec).getSpecs().get(0);
                if (spec instanceof FuncEvalSpec && ((FuncEvalSpec)spec).getFunc().getReturnType() == DataBag.class) { // trumps 'em all
                    driver = i;
                    return;
                } 
                driver = i; // we'll use this as the driver, unless something better comes along
            }
        }
    }
 
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("GENERATE ");
        sb.append("{");
        boolean first = true;
        for (EvalSpec spec: specs){
            if (!first)
                sb.append(",");
            else
                first = false;
            sb.append(spec);
        }
        sb.append("}");
        return sb.toString();
    }

    @Override
    public List<String> getFuncs() {
        List<String> funcs = new ArrayList<String>();
        for (EvalSpec spec: specs)
            funcs.addAll(spec.getFuncs());
        return funcs;
    }

    @Override
    protected TupleSchema mapInputSchema(Schema input) {
        TupleSchema output = new TupleSchema();
                
        for (EvalSpec spec: specs) {
            Schema schema = spec.getOutputSchemaForPipe(input).copy();
            
            if (spec.isFlattened()){
                List<Schema> flattenedSchema = schema.flatten(); 
                if (flattenedSchema.size() == 0){
                    output.add(new TupleSchema(),true);
                    continue;
                }
                for (Schema flattenedItem: flattenedSchema){
                    output.add(flattenedItem,true);
                }
            }else{
                output.add(schema,false);
            }
        }
        return output;
    }

    @Override
    public boolean isAsynchronous() {
        for (EvalSpec es: specs)
            if (es.isAsynchronous())
                return true;
        return false;
    }

    @Override
    public void instantiateFunc(FunctionInstantiator instantiaor)
            throws IOException {
        super.instantiateFunc(instantiaor);
        for (EvalSpec es: specs)
            es.instantiateFunc(instantiaor);        
    }

    public List<EvalSpec> getSpecs() {
        return specs;
    }

    @Override
    public void visit(EvalSpecVisitor v) {
        v.visitGenerate(this);
    }
    
}
