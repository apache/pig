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
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DefaultTupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.data.DataType;

/**
* Computes the correlation between sets of data.  The returned value 
* will be a bag which will contain a tuple for each combination of input 
* schema and inside tuple we will have two schema name and correlation between 
* those  two schemas. 
* 
* <dl>
* <dt><b>Parameters:</b></dt>
* <dd><code>data sets</code> - <code>tuple which contain DataBag corresponding to each data set, and inside
* DataBag we have tuple corresponding to each data atom. E.g. ({(1),(2)},{(3),(4)}) </code>.</dd>
* 
* <dt><b>Return Value:</b></dt>
* <dd><code>DataBag which contain every possible combination of input schemas</code> correlation between data sets</dd>
* 
* <dt><b>Return Schema:</b></dt>
* <dd>correlation</dd>
* 
* <dt><b>Example:</b></dt>
* <dd><code>
* register statistics.jar;<br/>
* A = load 'input.xml' using PigStorage(':');<br/>
* B = group A all;<br/>
* define c COR('a','b','c');<br/>
* D = foreach B generate group,c(A.$0,A.$1,A.$2);<br/>
* </code></dd>
* </dl>
*
* @author ajay garg
* @see  <a href = "http://en.wikipedia.org/wiki/Covariance"> http://en.wikipedia.org/wiki/Covariance</a><br>
*/

/**
 * @deprecated Use {@link org.apache.pig.builtin.COR}
 */
@Deprecated 

public class COR extends EvalFunc<DataBag> implements Algebraic,Serializable {
    //name of the schemas. Initialize when user use define
    protected Vector<String>schemaName = new Vector<String>();
    //flag to indicate if define is called or not. 
    private boolean flag = false;
    
    public COR(){}
    
    public COR(String... schemaName){
        for(int i=0;i<schemaName.length;i++){
            this.schemaName.add(schemaName[i]);
            flag = true;
        }
    }
    
    /**
     * Function to compute correlation between data sets.
     * @param input input tuple which contains data sets.
     * @param output output dataBag which contain correlation between each pair of data sets. 
     */
    @Override
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;

        DataBag output = DefaultBagFactory.getInstance().newDefaultBag();
        for(int i=0;i<input.size();i++){
            for(int j=i+1;j<input.size();j++){
                Tuple temp = DefaultTupleFactory.getInstance().newTuple(3);
                try{
                    if(flag){
                        temp.set(0, schemaName.elementAt(i));
                        temp.set(1, schemaName.elementAt(j));
                    }
                    else{
                        temp.set(0, "var"+i);
                        temp.set(1, "var"+j);
                    }
                
                    Tuple tempResult = computeAll((DataBag)input.get(i),(DataBag)input.get(j));
                    double size = ((DataBag)input.get(i)).size();
                    double sum_x_y = (Double)tempResult.get(0);
                    double sum_x = (Double)tempResult.get(1);
                    double sum_y = (Double)tempResult.get(2);
                    double sum_x_square = (Double)tempResult.get(3);
                    double sum_y_square = (Double)tempResult.get(4);
                    double result = (size*sum_x_y - sum_x*sum_y)/Math.sqrt((size*sum_x_square-sum_x*sum_x)*(size*sum_y_square-sum_y*sum_y));
                    temp.set(2, result);
                }catch (Exception e){
                    System.err.println("Failed to process input record; error - " + e.getMessage());
                    return null;
                }
                output.add(temp);
            }
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
        return Final.class.getName() + toString();
    }

    static public class Initial extends EvalFunc<Tuple> implements Serializable{
        @Override
        public Tuple exec(Tuple input) throws IOException {
            if (input == null || input.size() == 0)
                return null;
            Tuple output = DefaultTupleFactory.getInstance().newTuple(input.size() * 2); 
            try {
                int k = -1;
                for(int i=0;i<input.size();i++){
                    for(int j=i+1;j<input.size();j++){
                        DataBag first = (DataBag)input.get(i);
                        DataBag second = (DataBag)input.get(j);
                        output.set(k++, computeAll(first, second));
                        output.set(k++, (Long)first.size());
                    }
                }
            } catch(Exception t) {
                System.err.println("Failed to process input record; error - " + t.getMessage());
                return null;
            }
            return output;    
        }
    } 

    static public class Intermed extends EvalFunc<Tuple> implements Serializable{
        
        @Override
        public Tuple exec(Tuple input) throws IOException {
            if (input == null || input.size() == 0)
                return null;
            try{
                return combine((DataBag)input.get(0));
            }catch(Exception e){
                throw new IOException("Caught exception in COR.Intermed", e);
            }
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
        public DataBag exec(Tuple input) throws IOException {
            if (input == null || input.size() == 0)
                return null;

            Tuple combined;    
            try{
                combined = combine((DataBag)input.get(0));    
            }catch(Exception e){
                throw new IOException("Caught exception in COR.Final", e);
            }
            int count = 0;
            //for each pair of input schema combined contain 2 member. first member
            //is tuple containing sum_x,sum_y,sum_x_y, sum_x_square, sum_y_square
            //and second is number of tuples from which it is calculated . 
            //So if arity of combined is n then total number of schemas would be 
            //root of x*x - x - n =0 
            int totalSchemas=2;
            while(totalSchemas*(totalSchemas-1)<combined.size()){
                totalSchemas++;
            } 
                
            DataBag output = DefaultBagFactory.getInstance().newDefaultBag();
            for(int i=0;i<totalSchemas;i++){
                for(int j=i+1;j<totalSchemas;j++){
                    Tuple result = DefaultTupleFactory.getInstance().newTuple(3);
                    try{
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
                        double sum_x_square = (Double)tup.get(3);
                        double sum_y_square = (Double)tup.get(4);
                        double correl = (tempCount*sum_x_y - sum_x*sum_y)/Math.sqrt((tempCount*sum_x_square-sum_x*sum_x)*(tempCount*sum_y_square-sum_y*sum_y));
                        result.set(2, correl);
                    }catch(Exception e){
                        System.err.println("Failed to process input record; error - " + e.getMessage());
                        return null;
                    } 
                    output.add(result);
                    count+=2;
                }
            }

            return output;
        }
    }

    /**
     * combine results of different data chunk 
     * @param values DataBag containing partial results computed on different data chunks
     * @param output Tuple containing combined data
     * @throws IOException
     */
    static protected Tuple combine(DataBag values) throws IOException {
        Tuple output = DefaultTupleFactory.getInstance().newTuple();
        Tuple tuple; // copy of DataBag values 
        tuple =  DefaultTupleFactory.getInstance().newTuple(values.size());
        int ct=0;

        try{
            for (Iterator<Tuple> it = values.iterator(); it.hasNext();ct++) {
                Tuple t = it.next();
                tuple.set(ct, t);
            }
        }catch(Exception e){}

        try{
            int size = ((Tuple)tuple.get(0)).size();
            for(int i=0;i<size;i=i+2){
                double count = 0;
                double sum_x_y = 0.0;
                double sum_x = 0.0;
                double sum_y = 0.0;
                double sum_x_square = 0.0;
                double sum_y_square = 0.0;
                for(int j=0;j<tuple.size();j++){
                    Tuple temp = (Tuple)tuple.get(j);
                    Tuple tem = (Tuple)temp.get(i);
                    count += (Double)temp.get(i+1);
                    sum_x_y += (Double)tem.get(0);
                    sum_x += (Double)tem.get(1);
                    sum_y += (Double)tem.get(2);
                    sum_x_square += (Double)tem.get(3);
                    sum_y_square += (Double)tem.get(4);
                }
                Tuple result = DefaultTupleFactory.getInstance().newTuple(5);
                result.set(0, sum_x_y);
                result.set(1, sum_x);
                result.set(2, sum_y);
                result.set(3, sum_x_square);
                result.set(4, sum_y_square);
                output.append(result);
                output.append(count);
            }
        }catch(Exception e){
            throw new IOException("Caught exception in COR.combine", e);
        }

        return output;
    }

    /**
     * compute sum(XY), sum(X), sum(Y), sum(XX), sum(YY) from given data sets
     * @param first DataBag containing first data set
     * @param second DataBag containing second data set
     * @return tuple containing sum(XY), sum(X), sum(Y), sum(XX), sum(YY)
     */
    protected static Tuple computeAll(DataBag first, DataBag second) {
        double sum_x_y = 0.0;
        double sum_x = 0.0;
        double sum_y = 0.0;
        double sum_x_square = 0.0;
        double sum_y_square = 0.0;
        Iterator<Tuple> iterator_x = first.iterator();
        Iterator<Tuple> iterator_y = second.iterator();
        try{
            while(iterator_x.hasNext()){
                double x = (Double)iterator_x.next().get(0);
                double y = (Double)iterator_y.next().get(0);
                sum_x_y+=x*y;
                sum_x+=x;
                sum_y+=y;
                sum_x_square+=x*x;
                sum_y_square+=y*y;
            }
        }catch(Exception e){
            System.err.println("Failed to process input record; error - " + e.getMessage());
            return null;
        }
        
        Tuple result = DefaultTupleFactory.getInstance().newTuple(5);
        try{
            result.set(0, sum_x_y);
            result.set(1, sum_x);
            result.set(2, sum_y);
            result.set(3, sum_x_square);
            result.set(4, sum_y_square);
        }catch(Exception e){}
        return result;
        
    } 
    
    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), DataType.BAG));
    } 

}
