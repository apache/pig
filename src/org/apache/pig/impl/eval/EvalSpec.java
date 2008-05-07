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
import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

import org.apache.pig.ComparisonFunc;
import org.apache.pig.data.Datum;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.FunctionInstantiator;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.eval.collector.DataCollector;
import org.apache.pig.impl.eval.collector.FlattenCollector;
import org.apache.pig.impl.eval.collector.UnflattenCollector;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.DataBuffer;
import org.apache.pig.impl.util.LineageTracer;
import org.apache.pig.impl.util.ObjectSerializer;


public abstract class EvalSpec implements Serializable{
    boolean isFlattened; 
    Schema schema;
    transient DataBuffer simpleEvalOutput;
    transient DataCollector simpleEvalInput; 
    protected boolean inner = false; //used only if this generate spec is used in a group by

    private String comparatorFuncName;
    private transient Comparator<Tuple> comparator;
    private LineageTracer lineageTracer;
    
    protected Properties properties = new Properties();
    
    public void setLineageTracer(LineageTracer lineage) {
    	this.lineageTracer = lineage;
    }
	
	public LineageTracer getLineage() {
		return lineageTracer;
	}
    
    /*
     * Keep a precomputed pipeline ready if we do simple evals
     * No separate code path for simple evals as earlier 
     */
    private void init(){
        simpleEvalOutput = new DataBuffer();
        simpleEvalInput = setupPipe(properties, simpleEvalOutput);
    }
    
    public class UserComparator implements Comparator<Tuple> {
        Comparator<Tuple> nested;
        
        UserComparator(Comparator<Tuple> nested) {
            this.nested = nested;
        }
        public int compare(Tuple t1, Tuple t2) {
            Datum d1 = simpleEval(t1);
            Datum d2 = simpleEval(t2);
            if (d1 instanceof Tuple) {
                return nested.compare((Tuple)d1, (Tuple)d2);
            } else {
                return nested.compare(new Tuple(d1), new Tuple(d2));
            }
        }
    }
    
    public void instantiateFunc(FunctionInstantiator instantiaor) throws IOException{
        if (comparatorFuncName != null) {
            Comparator<Tuple> userComparator = 
                (ComparisonFunc)instantiaor.instantiateFuncFromAlias(comparatorFuncName);
            comparator = new UserComparator(userComparator);
        } else {
            comparator = new Comparator<Tuple>() {
                public int compare(Tuple t1, Tuple t2) {
                    return simpleEval(t1).compareTo(simpleEval(t2));
                }
            };
        }
    };
    
    /**
     * set up a default data processing pipe for processing by this spec
     * This pipe does not include unflattening/flattening at the end
     * @param properties properties for the pipe
     * @param endOfPipe The collector where output is desired
     * @return The collector where input tuples should be put
     */
    protected abstract DataCollector setupDefaultPipe(Properties properties,
                                                      DataCollector endOfPipe);
    
    
    /**
     * set up a data processing pipe with flattening/unflattening at the end
     * based on the isFlattened field
     * 
     * @param properties properties for the <code>EvalSpec</code>
     * @param endOfPipe where the output is desired 
     * @return The collector where input tuples should be put
     */
    public DataCollector setupPipe(Properties properties, 
                                   DataCollector endOfPipe){
        /*
         * By default tuples flow through the eval pipeline in a flattened fashion
         * Thus if flatten is true, we use the default setup pipe method, otherwise we add 
         * an unflatten at the end
          */
    
        if (isFlattened){
            FlattenCollector fc = new FlattenCollector(endOfPipe);
            return setupDefaultPipe(properties, fc);
        }else{
            UnflattenCollector uc = new UnflattenCollector(endOfPipe);
            return setupDefaultPipe(properties, uc);
        }
    }
    
    
    /**
     * set the succesor of this spec
     * @param spec the new succesor
     * @return
     */
    public EvalSpec addSpec(EvalSpec spec){
        CompositeEvalSpec ces = new CompositeEvalSpec(this);
        ces.addSpec(spec);
        return ces;
    }
    
    /**
     * Get the functions required by this spec
     * @return
     */
    public abstract List<String> getFuncs(); 
    
    
    public Schema getOutputSchemaForPipe(Schema input){
        if (schema!=null)
            return schema;
        else
            return mapInputSchema(input);
    }
    
    /**
     * Given an input schema, determine the output schema of this spec
     * as it operates on input tuples with the input schema.
     * @param input
     * @return
     */    
    protected abstract Schema mapInputSchema(Schema schema);

    /**
     * A placeholder for any cleanup action that the spec needs to perform
     *
     */
    public void finish(){
        if (simpleEvalInput == null)
            init();
        simpleEvalInput.finishPipe();
    }

    /**
     * Some specs may be asynchronous, i.e., they return before completing the processing fully. 
     * The default value is false, may be overridden to return true
     */
    public boolean isAsynchronous(){
        return false;
    }
    
    public void setComparatorName(String name) {
        comparatorFuncName = name;
    }
    
    public String getComparatorName() {
        return comparatorFuncName;
    }
    
    /**
     * Compare 2 tuples according to this spec. This is used while sorting by arbitrary (even generated) fields.
     * @return
     */
    public Comparator<Tuple> getComparator() {
        return comparator;
    }
    
    public void setFlatten(boolean isFlattened){
        this.isFlattened = isFlattened;
    }
    
    public boolean isFlattened(){
        return isFlattened;
    }
   
    /**
     * If the spec is such that it produces exactly one datum per input datum, we can use simple
     * eval as a shortcut to the whole process of setting the pipe etc. However, the code path is
     * still the same in both cases.
     * @param input
     * @return
     */
    public Datum simpleEval(Datum input){
        if (simpleEvalInput == null)
            init();
        simpleEvalInput.add(input);
        return simpleEvalOutput.removeFirstAndAssertEmpty();
    }
   
    public EvalSpec getCombiner(){
        //TODO
        return null;
    }
    
    public EvalSpec copy(PigContext pigContext){
        try{
            EvalSpec es = (EvalSpec) ObjectSerializer.deserialize(ObjectSerializer.serialize(this));
            es.instantiateFunc(pigContext);
            es.properties.putAll(properties);
            return es;
        }catch(IOException e){
            throw new RuntimeException(e);
        }
    }
    
    public void setSchema(Schema schema){
        this.schema = schema;
    }
    

    public boolean isInner() {
        return inner;
    }

    public void setInner(boolean inner) {
        this.inner = inner;
    }

    /**
     * Get properties specific to a given <code>EvalSpec</code>.
     * 
     * @return properties specific to a given <code>EvalSpec</code>
     */
    public Properties getProperties() {
        return properties;
    }
    
    public abstract void visit(EvalSpecVisitor v);
    
}
