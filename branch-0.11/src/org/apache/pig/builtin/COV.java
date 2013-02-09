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

package org.apache.pig.builtin;


import java.io.IOException;
import java.util.Iterator;
import java.util.Vector;

import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
* Computes the covariance between sets of data.  The returned value 
* will be a bag which will contain a tuple for each combination of input 
* schema and inside tuple we will have two schema name and covariance between 
* those  two schemas. 
* 
* A = load 'input.xml' using PigStorage(':');<br/>
* B = group A all;<br/>
* D = foreach B generate group,COV(A.$0,A.$1,A.$2);<br/>
*/
public class COV extends EvalFunc<DataBag> implements Algebraic {
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
     * @return output output dataBag which contain covariance between each pair of data sets. 
     */
    @Override
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;
        DataBag output = DefaultBagFactory.getInstance().newDefaultBag();

        try{
            for(int i=0;i<input.size();i++){
                for(int j=i+1;j<input.size();j++){
                    Tuple temp = TupleFactory.getInstance().newTuple(3);
                    if(flag){
                        temp.set(0, schemaName.elementAt(i));
                        temp.set(1, schemaName.elementAt(j));
                    }
                    else{
                        temp.set(0, "var"+i);
                        temp.set(1, "var"+j);
                    }
                
                    Tuple tempResult = computeAll((DataBag)input.get(i), (DataBag)input.get(j));
                    double size = ((DataBag)input.get(i)).size();
                    double sum_x_y = (Double)tempResult.get(0);
                    double sum_x = (Double)tempResult.get(1);
                    double sum_y = (Double)tempResult.get(2);
                    double result = (size*sum_x_y - sum_x*sum_y)/(size*size);
                    temp.set(2, result);
                    output.add(temp);
                }
            }
        }catch(Exception e){
            System.err.println("Failed to process input; error - " + e.getMessage());
            return null;
        }

        return output;
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

    static public class Initial extends EvalFunc<Tuple> {
        @Override
        public Tuple exec(Tuple input) throws IOException {
            if (input == null || input.size() == 0)
                return null;

            Tuple output = TupleFactory.getInstance().newTuple();
            try {
                for(int i=0;i<input.size();i++){
                    for(int j=i+1;j<input.size();j++){
                        DataBag first = (DataBag)input.get(i);
                        DataBag second = (DataBag)input.get(j);
                        output.append(computeAll(first, second));
                        output.append(first.size());
                    }
                }
            } catch(Exception t) {
                System.err.println("Failed to process input; error - " + t.getMessage());
                return null;
            }

            return output;
        }
    }

    static public class Intermed extends EvalFunc<Tuple> {
        
        @Override
        public Tuple exec(Tuple input) throws IOException {
            try{
                return combine((DataBag)input.get(0));
            }catch(Exception e){
                throw new IOException("Caught exception in COV.Intermed", e);
            }
        }
    }

    public static class Final extends EvalFunc<DataBag> {
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
        public DataBag exec(Tuple input) throws IOException {
            if (input == null || input.size() == 0)
                return null;
            
            DataBag output = DefaultBagFactory.getInstance().newDefaultBag();
            int count = 0;
            
            //for each pair of input schema combined contain 2 member. first member
            //is tuple containing sum_x,sum_y,sum_x_y and second is number of tuples 
            //from which it is calculated . So if arity of combined is n then total 
            //number of schemas would be root of x*x - x - n =0 
            
            try{
                Tuple combined = combine((DataBag)input.get(0));    
                int totalSchemas=2;
                while(totalSchemas*(totalSchemas-1)<combined.size()){
                    totalSchemas++;
                }
                for(int i=0;i<totalSchemas;i++){
                    for(int j=i+1;j<totalSchemas;j++){
                        Tuple result = TupleFactory.getInstance().newTuple(3);
                        if(flag){
                            result.set(0, schemaName.elementAt(i));
                            result.set(1, schemaName.elementAt(j));
                        }
                        else{
                            result.set(0, "var"+i);
                            result.set(1, "var"+j);
                        }
                        Tuple tup = (Tuple)combined.get(count);
                        double tempCount = (Double)combined.get(count+1);
                        double sum_x_y = (Double)tup.get(0);
                        double sum_x = (Double)tup.get(1);
                        double sum_y = (Double)tup.get(2);
                        double covar = (tempCount*sum_x_y - sum_x*sum_y)/(tempCount*tempCount);
                        result.set(2, covar);
                        output.add(result);
                        count+=2;
                    }
                }
            }catch(Exception e){
                throw new IOException("Caught exception in COV.Intermed", e);
            }

            return output;
        }
    }

    /**
     * combine results of different data chunk 
     * @param values DataBag containing partial results computed on different data chunks
     * @return output Tuple containing combined data
     * @throws IOException
     */
    static protected Tuple combine(DataBag values) throws IOException {
        Tuple tuple = TupleFactory.getInstance().newTuple(Double.valueOf(values.size()).intValue());
        Tuple output = TupleFactory.getInstance().newTuple();

        int ct=0;
        for (Iterator<Tuple> it = values.iterator(); it.hasNext();ct++) {
            Tuple t = it.next();
            try{ tuple.set(ct, t);}catch(Exception e){}
        }
        
        try{
            Tuple temp = (Tuple)tuple.get(0);
            int size = temp.size();
            for(int i=0;i<size;i=i+2){
                double count = 0;
                double sum_x_y = 0.0;
                double sum_x = 0.0;
                double sum_y = 0.0;
                for(int j=0;j<tuple.size();j++){
                    Tuple t = (Tuple)tuple.get(j);
                    Tuple tem = (Tuple)t.get(i);
                    count += (Double)t.get(i+1);
                    sum_x_y+=(Double)tem.get(0);
                    sum_x+=(Double)tem.get(1);
                    sum_y+=(Double)tem.get(2);
                }
                Tuple result = TupleFactory.getInstance().newTuple(3);
                result.set(0, sum_x_y);
                result.set(1, sum_x);
                result.set(2, sum_y);
                output.append(result);
                output.append(count);
            }
        }catch(Exception e){
            throw new IOException("Caught exception processing input", e);
        }
      
        return output;
    }

    /**
     * compute sum(XY), sum(X), sum(Y) from given data sets
     * @param first DataBag containing first data set
     * @param second DataBag containing second data set
     * @return tuple containing sum(XY), sum(X), sum(Y)
     */
    protected static Tuple computeAll(DataBag first, DataBag second) throws IOException {
        double sum_x_y = 0.0;
        double sum_x = 0.0;
        double sum_y = 0.0;
        Iterator<Tuple> iterator_x = first.iterator();
        Iterator<Tuple> iterator_y = second.iterator();
        try{
            while(iterator_x.hasNext()){
                double x = (Double)iterator_x.next().get(0);
                double y = (Double)iterator_y.next().get(0);
                sum_x_y+=x*y;
                sum_x+=x;
                sum_y+=y;
            }
        }catch (Exception e){
            throw new IOException("Caught exception processing input", e);
        }
        
        Tuple result = TupleFactory.getInstance().newTuple(3);
        try{
            result.set(0, sum_x_y);
            result.set(1, sum_x);
            result.set(2, sum_y);
        }catch(Exception e) {
            throw new IOException("Caught exception processing result", e);
        }
        return result;
        
    }
    
    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), DataType.BAG));
    }

}
