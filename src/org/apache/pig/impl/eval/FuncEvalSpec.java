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
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.Properties;

import org.apache.pig.EvalFunc;
import org.apache.pig.Algebraic;
import org.apache.pig.data.DataAtom;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataMap;
import org.apache.pig.data.Datum;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.FunctionInstantiator;
import org.apache.pig.impl.eval.collector.DataCollector;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.TupleSchema;



public class FuncEvalSpec extends EvalSpec {
    private static final long serialVersionUID = 1L;
    
    String funcName;
    EvalSpec args;
    transient EvalFunc func;

    public FuncEvalSpec(FunctionInstantiator fInstantiaor, String funcName, EvalSpec args) throws IOException{        
        this.funcName = funcName;
        this.args = args;
        
        if (args!=null && args.isAsynchronous())
            throw new IOException("Can't have the output of an asynchronous function as the argument to an eval function");
        instantiateFunc(fInstantiaor);
    }
    
    @Override
    public void instantiateFunc(FunctionInstantiator instantiaor) throws IOException{
        if(instantiaor != null) {
            func = (EvalFunc) instantiaor.instantiateFuncFromAlias(funcName);
        }
        args.instantiateFunc(instantiaor);
    }
    
    @Override
    public List<String> getFuncs() {
        List<String> funcs = new ArrayList<String>();
        funcs.add(funcName);
        return funcs;
    }

    @Override
    protected Schema mapInputSchema(Schema schema) {
        Schema inputToFunction;
        if (args!=null){
            inputToFunction = args.mapInputSchema(schema);
        }else{
            inputToFunction = new TupleSchema();
        }
        
        return func.outputSchema(inputToFunction);
    }

    @Override
    protected DataCollector setupDefaultPipe(Properties properties,
                                             DataCollector endOfPipe) {
        return new DataCollector(endOfPipe){
            private Datum getPlaceHolderForFuncOutput(){
                Type returnType = func.getReturnType();
                if (returnType == DataAtom.class)
                    return new DataAtom();
                else if (returnType == Tuple.class)
                    return new Tuple();
                else if (returnType == DataBag.class)
                    return new FakeDataBag(successor);
                else if (returnType == DataMap.class)
                    return new DataMap();
                else throw new RuntimeException("Internal error: Unknown return type of eval function");
            }
            
            @Override
            public void add(Datum d) {
                if (checkDelimiter(d))
                    addToSuccessor(d);
                
                Datum argsValue = null;
                if (args!=null)
                    argsValue = args.simpleEval(d);
                
                if (argsValue!=null && !(argsValue instanceof Tuple))
                    throw new RuntimeException("Internal error: Non-tuple returned on evaluation of arguments.");
                
                Datum placeHolderForFuncOutput = getPlaceHolderForFuncOutput();
                try{
                    func.exec((Tuple)argsValue, placeHolderForFuncOutput);
                }catch (IOException e){
                    RuntimeException re = new RuntimeException(e);
                    re.setStackTrace(e.getStackTrace());
                    throw re;
                }
                
                if (placeHolderForFuncOutput instanceof FakeDataBag){
                    FakeDataBag fBag = (FakeDataBag)placeHolderForFuncOutput;
                    synchronized(fBag){
                        if (!fBag.isStale())
                            fBag.addDelimiters();
                    }
                }else{
                    addToSuccessor(placeHolderForFuncOutput);
                }
            }
            
            @Override
            protected void finish() {
                if (args!=null) 
                    args.finish();
                func.finish();
            }            
        };
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        sb.append(funcName);
        sb.append("(");
        sb.append(args);
        sb.append(")");
        sb.append("]");
        return sb.toString();
    }
    
    

    private class FakeDataBag extends DataBag{
        int staleCount = 0;
        DataCollector successor;
        boolean startAdded = false, endAdded = false;
        
        public FakeDataBag(DataCollector successor){
            this.successor = successor;
        }

        // To satisfy abstract functions in DataBag.
        public boolean isSorted() { return false; }
        public boolean isDistinct() { return false; }
        public Iterator<Tuple> iterator() { return null; }
        public long spill() { return 0; }

        
        void addStart(){
            successor.add(DataBag.startBag);
            startAdded = true;    
        }
        
        void addEnd(){
            successor.add(DataBag.endBag);
            endAdded = true;
        }
        
        void addDelimiters(){
            if (!startAdded)
                addStart();
            if (!endAdded)
                addEnd();    
        }
        
        @Override
        public void add(Tuple t) {
            synchronized(this){
                if (!startAdded)
                    addStart();
            }
            successor.add(t);
        }
        
        @Override
        public void markStale(boolean stale) {
            synchronized (this){
                if (stale)
                    staleCount++;
                else{
                    if (staleCount>0){
                        addDelimiters();
                        staleCount--;
                    }
                }
                super.markStale(stale);
            }
        }
        
        public boolean isStale(){
            synchronized(this){
                return staleCount > 0;
            }
        }
    }
    
    
    /**
     * Extend the default deserialization
     * @param in
     * @throws IOException
     * @throws ClassNotFoundException
     */
    /*
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException{
        in.defaultReadObject();
        instantiateFunc();
    }
    */
    public EvalFunc getFunc() {
        return func;
    }
    
    public Type getReturnType(){
        return func.getReturnType();
    }
    
    @Override
    public boolean isAsynchronous() {
        return func.isAsynchronous();
    }

    @Override
    public void visit(EvalSpecVisitor v) {
        v.visitFuncEval(this);
    }

    public String getFuncName() { return funcName; }

    public EvalSpec getArgs() { return args; }

    public void setArgs(EvalSpec a) { args = a; }

    /**
     * This will replace the function to be called by this spec to be the
     * initial instance instead of the general instance.  This should only
     * be called if the function is algebraic.  It will only change the
     * funcName variable, not the func variable itself.
     */
    public void resetFuncToInitial() {
        if (!combinable()) {
            throw new AssertionError(
                "Can't convert non-algebraic function to inital.");
        }
        funcName = ((Algebraic)func).getInitial();
    }

    /**
     * This will replace the function to be called by this spec to be the
     * intermediate instance instead of the general instance.  This should only
     * be called if the function is algebraic.  It will only change the
     * funcName variable, not the func variable itself.
     */
    public void resetFuncToIntermediate() {
        if (!combinable()) {
            throw new AssertionError(
                "Can't convert non-algebraic function to intermediate.");
        }
        funcName = ((Algebraic)func).getIntermed();
    }

    /**
     * This will replace the function to be called by this spec to be the
     * final instance instead of the general instance.  This should only
     * be called if the function is algebraic.  It will only change the
     * funcName variable, not the func variable itself.
     * @param finalTuplePos position in the tuple handed to the final
     * function that it should use.
     */
    public void resetFuncToFinal() {
        if (!combinable()) {
            throw new AssertionError(
                "Can't convert non-algebraic function to final.");
        }
        funcName = ((Algebraic)func).getFinal();
    }

    public boolean combinable() {
        // constructor should have called by instantiateFunc
        if (func != null) return (func instanceof Algebraic);
        else return false;
    }
    
}
