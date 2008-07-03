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

package org.apache.pig.piggybank.evaluation.stats;


import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Vector;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataAtom;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.AtomSchema;
import org.apache.pig.impl.logicalLayer.schema.Schema;


/**
* Computes the covariance between sets of data.  The returned value 
* will be a bag which will contain a tuple for each combination of input 
* schema and inside tuple we will have two schema name and covariance between 
* those  two schemas. 
* 
* <dl>
* <dt><b>Parameters:</b></dt>
* <dd><code>data sets</code> - <code>tuple which contain DataBag corresponding to each data set, and inside
* DataBag we have tuple corresponding to each data atom. E.g. ({(1),(2)},{(3),(4)}) </code>.</dd>
* 
* <dt><b>Return Value:</b></dt>
* <dd><code>DataBag which contain every possible combination of input schemas</code> covariance between data sets</dd>
* 
* <dt><b>Return Schema:</b></dt>
* <dd>covariance</dd>
* 
* <dt><b>Example:</b></dt>
* <dd><code>
* register statistics.jar;<br/>
* A = load 'input.xml' using PigStorage(':');<br/>
* B = group A all;<br/>
* define c COV('a','b','c');<br/>
* D = foreach B generate group,c(A.$0,A.$1,A.$2);<br/>
* </code></dd>
* </dl>
*
* @author ajay garg
* @see  <a href = "http://en.wikipedia.org/wiki/Covariance">http://en.wikipedia.org/wiki/Covariance</a><br>
*/
public class COV extends EvalFunc<DataBag> implements Algebraic,Serializable {
    //name of the schemas. Initialize when user use define
    protected Vector<String>schemaName = new Vector<String>();
    //flag to indicate if define is called or not. 
    private boolean flag = false;
    
    public COV(){}
    
    
    public COV(String... schemaName){
        for(int i=0;i<schemaName.length;i++){
            this.schemaName.add(schemaName[i]);
            flag = true;
        }
    }
    
    
    /**
     * Function to compute covariance between data sets.
     * @param input input tuple which contains data sets.
     * @param output output dataBag which contain covariance between each pair of data sets. 
     */
    @Override
    public void exec(Tuple input, DataBag output) throws IOException {
        for(int i=0;i<input.arity();i++){
            for(int j=i+1;j<input.arity();j++){
                Tuple temp = new Tuple(3);
                if(flag){
                    /*try{
                        temp.setField(0, schemaName.elementAt(i));
                    }
                    catch(ArrayIndexOutOfBoundsException e){
                        temp.setField(0, "var"+i);
                    }
                    
                    try{
                        temp.setField(1, schemaName.elementAt(j));
                    }
                    catch(ArrayIndexOutOfBoundsException e){
                        temp.setField(1, "var"+j);
                    }*/
                    try{
                        temp.setField(0, schemaName.elementAt(i));
                        temp.setField(1, schemaName.elementAt(j));
                    }
                    catch(ArrayIndexOutOfBoundsException e){
                        throw new IOException("number of parameters in define are less ");
                    }
                }
                else{
                    temp.setField(0, "var"+i);
                    temp.setField(1, "var"+j);
                }
                
                Tuple tempResult = computeAll(input.getBagField(i),input.getBagField(j));
                double size = input.getBagField(i).size();
                double sum_x_y = tempResult.getAtomField(0).numval();
                double sum_x = tempResult.getAtomField(1).numval();
                double sum_y = tempResult.getAtomField(2).numval();
                double result = (size*sum_x_y - sum_x*sum_y)/(size*size);
                temp.setField(2, result);
                output.add(temp);
                
                
            }
        }
    }
    

    //used to pass schema name to Final class constructor 
    /**
     * Function to return argument of constructor as string. It append ( and ) at starting and end or argument respectively.
     * If default constructor is called is returns empty string.
     * @return argument of constructor
     */
    @Override
    public String toString() {
        if(flag){
            StringBuilder sb = new StringBuilder();
            sb.append('(');
            if(schemaName!=null){
                for (String sch : schemaName) {
                    sb.append('\'' + sch + '\'' + ",");
                }
                sb.deleteCharAt(sb.length()-1);
            }
            sb.append(')');
            return sb.toString();
        }
        else return "";
    }
    public String getInitial() {
        return Initial.class.getName();
    }

    public String getIntermed() {
        return Intermed.class.getName();
    }

    public String getFinal() {
        //return Final.class.getName();
        return Final.class.getName() + toString();
    }

    
    static public class Initial extends EvalFunc<Tuple> implements Serializable{
        @Override
        public void exec(Tuple input, Tuple output) throws IOException {
            try {
            for(int i=0;i<input.arity();i++){
                for(int j=i+1;j<input.arity();j++){
                    DataBag first = input.getBagField(i);
                    DataBag second = input.getBagField(j);
                    output.appendField(computeAll(first, second));
                    output.appendField(new DataAtom(first.size()));
                    
                }
            }
            
            } catch(RuntimeException t) {
                throw new IOException(t.getMessage() + ": " + input, t);
            }
            
        }
    }

    static public class Intermed extends EvalFunc<Tuple> implements Serializable{
        
        @Override
        public void exec(Tuple input, Tuple output) throws IOException {
            
            combine(input.getBagField(0), output);
        }
    }

    public static class Final extends EvalFunc<DataBag> implements Serializable{
        protected Vector<String>schemaName = new Vector<String>();
        boolean flag = false;
        
        public Final(){}
        
        public Final(String... schemaName){
            for(int i=0;i<schemaName.length;i++){
                this.schemaName.add(schemaName[i]);
                flag = true;
            }
        }
        

        @Override
        public void exec(Tuple input, DataBag output) throws IOException {
            Tuple combined = new Tuple();
            if(input.getField(0) instanceof DataBag) {
                combine(input.getBagField(0), combined);    
            } else {
                throw new IOException("Bag not found in: " + input);
            }
            
            int count = 0;
            
            //for each pair of input schema combined contain 2 member. first member
            //is tuple containing sum_x,sum_y,sum_x_y and second is number of tuples 
            //from which it is calculated . So if arity of combined is n then total 
            //number of schemas would be root of x*x - x - n =0 
            int totalSchemas=2;
            while(totalSchemas*(totalSchemas-1)<combined.arity()){
                totalSchemas++;
            }
            //int totalSchemas = Double.valueOf(((1+Math.sqrt(1+4*combined.arity()))/2)).intValue();
            
            for(int i=0;i<totalSchemas;i++){
                for(int j=i+1;j<totalSchemas;j++){
                    Tuple result = new Tuple(3);
                    if(flag){
                        /*try{
                            result.setField(0, schemaName.elementAt(i));
                        }
                        catch(ArrayIndexOutOfBoundsException e){
                            result.setField(0, "var"+i);
                        }
                        
                        try{
                            result.setField(1, schemaName.elementAt(j));
                        }
                        catch(ArrayIndexOutOfBoundsException e){
                            result.setField(1, "var"+j);
                        }*/
                        try{
                            result.setField(0, schemaName.elementAt(i));
                            result.setField(1, schemaName.elementAt(j));
                        }
                        catch(ArrayIndexOutOfBoundsException e){
                            throw new IOException("number of parameters in define are less ");
                        }
                    }
                    else{
                        result.setField(0, "var"+i);
                        result.setField(1, "var"+j);
                    }
                    Tuple tup = combined.getTupleField(count);
                    double tempCount = combined.getAtomField(count+1).numval();
                    double sum_x_y = tup.getAtomField(0).numval();
                    double sum_x = tup.getAtomField(1).numval();
                    double sum_y = tup.getAtomField(2).numval();
                    double covar = (tempCount*sum_x_y - sum_x*sum_y)/(tempCount*tempCount);
                    result.setField(2, covar);
                    output.add(result);
                    count+=2;
                    
                }
            }
        }
    }

    /**
     * combine results of different data chunk 
     * @param values DataBag containing partial results computed on different data chunks
     * @param output Tuple containing combined data
     * @throws IOException
     */
    static protected void combine(DataBag values, Tuple output) throws IOException {
        Tuple tuple; // copy of DataBag values
        tuple = new Tuple(Double.valueOf(values.size()).intValue());
        int ct=0;
        for (Iterator<Tuple> it = values.iterator(); it.hasNext();ct++) {
            Tuple t = it.next();
            tuple.setField(ct, t);
        }
        
        int size = tuple.getTupleField(0).arity();
        for(int i=0;i<size;i=i+2){
            double count = 0;
            double sum_x_y = 0.0;
            double sum_x = 0.0;
            double sum_y = 0.0;
            for(int j=0;j<tuple.arity();j++){
                Tuple tem = tuple.getTupleField(j).getTupleField(i);
                count += tuple.getTupleField(j).getAtomField(i+1).numval();
                sum_x_y+=tem.getAtomField(0).numval();
                sum_x+=tem.getAtomField(1).numval();
                sum_y+=tem.getAtomField(2).numval();
            }
            Tuple result = new Tuple(3);
            result.setField(0, sum_x_y);
            result.setField(1, sum_x);
            result.setField(2, sum_y);
            output.appendField(result);
            output.appendField(new DataAtom(count));
        }
        
        
    }

    /**
     * compute sum(XY), sum(X), sum(Y) from given data sets
     * @param first DataBag containing first data set
     * @param second DataBag containing second data set
     * @return tuple containing sum(XY), sum(X), sum(Y)
     */
    protected static Tuple computeAll(DataBag first, DataBag second) {
        double sum_x_y = 0.0;
        double sum_x = 0.0;
        double sum_y = 0.0;
        Iterator<Tuple> iterator_x = first.iterator();
        Iterator<Tuple> iterator_y = second.iterator();
        while(iterator_x.hasNext()){
            double x = iterator_x.next().getAtomField(0).numval();
            double y = iterator_y.next().getAtomField(0).numval();
            sum_x_y+=x*y;
            sum_x+=x;
            sum_y+=y;
        }
        
        Tuple result = new Tuple(3);
        result.setField(0, sum_x_y);
        result.setField(1, sum_x);
        result.setField(2, sum_y);
        return result;
        
    }
    
    @Override
    public Schema outputSchema(Schema input) {
        return new AtomSchema("COVARIANCE");
    }

}
