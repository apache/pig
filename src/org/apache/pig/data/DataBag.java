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
package org.apache.pig.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.impl.eval.EvalSpec;
import org.apache.pig.impl.eval.collector.DataCollector;


/**
 * A collection of Tuples
 */
public class DataBag extends Datum{
    protected List<Tuple> content;
    protected boolean isSorted = false;
    
    public DataBag() {
        content = new ArrayList<Tuple>();
    }

    public DataBag(List<Tuple> c) {
        content = c;
    }

    public DataBag(Tuple t) {
        content = new ArrayList<Tuple>();
        content.add(t);
    }

    public int cardinality() {
        return content.size();
    }

    public boolean isEmpty() {
        return content.size() == 0;
    }
    
    public int compareTo(Object other) {
    	if (this == other)
    		return 0;
    	if (other instanceof DataAtom) return +1;
        if (other instanceof Tuple) return -1;
        if (other instanceof DataBag){
        	DataBag bOther = (DataBag) other;
	        if (this.cardinality() != bOther.cardinality()) {
	            return (this.cardinality() - bOther.cardinality());
	        }
	        
	        // same cardinality, so compare tuple by tuple ...
	        if (!isSorted())
	        	this.sort();
	        if (!bOther.isSorted())
	        	bOther.sort();
	        
	        Iterator<Tuple> thisIt = this.content();
	        Iterator<Tuple> otherIt = bOther.content();
	        while (thisIt.hasNext() && otherIt.hasNext()) {
	            Tuple thisT = thisIt.next();
	            Tuple otherT = otherIt.next();
	            
	            int c = thisT.compareTo(otherT);
	            if (c != 0) return c;
	        }
	        
	        return 0;   // if we got this far, they must be equal
        }else{
        	return -1;
        }
	        	
    }
    
    @Override
	public boolean equals(Object other) {
        return (compareTo(other) == 0);
    }
    
    public void sort() {
        Collections.sort(content);
    }
    
    public void sort(EvalSpec spec) {
        Collections.sort(content, spec.getComparator());
        isSorted = true;
    }
    
    public void arrange(EvalSpec spec) {
        sort(spec);
        isSorted = true;
    }
    
    public void distinct() {
        
        Collections.sort(content);
        isSorted = true;
        
        Tuple lastTup = null;
        for (Iterator<Tuple> it = content.iterator(); it.hasNext(); ) {
            Tuple thisTup = it.next();
            
            if (lastTup == null) {
                lastTup = thisTup;
                continue;
            }
            
            if (thisTup.compareTo(lastTup) == 0) {
                it.remove();
            } else {
                lastTup = thisTup;
            }
        }
    }

    public Iterator<Tuple> content() {
        return content.iterator();
    }
    

    public void add(Tuple t) {
        if (t!=null)
        	content.add(t);
    }

    public void addAll(DataBag b) {
        
        Iterator<Tuple> it = b.content();
        while (it.hasNext()) {
            add(it.next());
        }
    }

    public void remove(Tuple d) {
        content.remove(d);
    }

    /**
     * Returns the value of field i. Since there may be more than one tuple in the bag, this
     * function throws an exception if it is not the case that all tuples agree on this field
     */
    public DataAtom getField(int i) throws IOException {
        DataAtom val = null;

        for (Iterator<Tuple> it = content(); it.hasNext();) {
            DataAtom currentVal = it.next().getAtomField(i);

            if (val == null) {
                val = currentVal;
            } else {
                if (!val.strval().equals(currentVal.strval()))
                    throw new IOException("Cannot call getField on a databag unless all tuples agree.");
            }
        }

        if (val == null)
            throw new IOException("Cannot call getField on an empty databag.");

        return val;
    }

    public void clear(){
    	content.clear();
    	isSorted = false;
    }

    
    @Override
	public void write(DataOutput out) throws IOException {
    	 out.write(BAG);
         Tuple.encodeInt(out, cardinality());
         Iterator<Tuple> it = content();
         while (it.hasNext()) {
             Tuple item = it.next();
             item.write(out);
         }	
    }
    
    public static abstract class BagDelimiterTuple extends Tuple{}
    public static class StartBag extends BagDelimiterTuple{}
    
    public static class EndBag extends BagDelimiterTuple{}
    
    public static final Tuple startBag = new StartBag();
    public static final Tuple endBag = new EndBag();
    
    static DataBag read(DataInput in) throws IOException {
        int size = Tuple.decodeInt(in);
        DataBag ret = BagFactory.getInstance().getNewBag();
        
        for (int i = 0; i < size; i++) {
            Tuple t = new Tuple();
            t.readFields(in);
            ret.add(t);
        }
        return ret;
    }
    
    public void markStale(boolean stale){}
    
    @Override
	public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append('{');
        Iterator<Tuple> it = content();
        while ( it.hasNext() ) {
        	Tuple t = it.next();
        	String s = t.toString();
        	sb.append(s);
            if (it.hasNext())
                sb.append(", ");
        }
        sb.append('}');
        return sb.toString();
    }
    
    public boolean isSorted(){
    	return isSorted;
    }
    
}
