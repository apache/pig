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
package org.apache.pig.test.utils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.Tuple;

public class GenRandomData {
    
    
    public static Map<String, Object> genRandMap(Random r, int numEnt) {
        Map<String,Object> ret = new HashMap<String, Object>();
        if(r==null){
            ret.put("random", "RANDOM");
            return ret;
        }
        for(int i=0;i<numEnt;i++){
            ret.put(genRandString(r), new DataByteArray(genRandString(r).getBytes()));
        }
        return ret;
    }
    
    public static String genRandString(Random r){
        if(r==null) return "RANDOM";
        char[] chars = new char[10];
        for(int i=0;i<10;i++){
            chars[i] = (char)(r.nextInt(26)+65);
        }
        return new String(chars);
    }    

    public static String genRandLargeString(Random r, int size){
        if(r==null) return "RANDOM";
        if(size <= 10) return genRandString(r);
        char[] chars = new char[size];
        for(int i=0;i<size;i++){
            chars[i] = (char)(r.nextInt(26)+65);
        }
        return new String(chars);
    }
    
    public static DataByteArray genRandDBA(Random r){
        if(r==null) return new DataByteArray("RANDOM".getBytes());
        byte[] bytes = new byte[10];
        r.nextBytes(bytes);
        return new DataByteArray(bytes);
    }
    
    public static DataByteArray genRandTextDBA(Random r){
        if(r==null) return new DataByteArray("RANDOM".getBytes());
        return new DataByteArray(genRandString(r).getBytes());
    }
    
    public static ResourceFieldSchema getSmallTupleFieldSchema() throws IOException{
        ResourceFieldSchema stringfs = new ResourceFieldSchema();
        stringfs.setType(DataType.CHARARRAY);
        ResourceFieldSchema intfs = new ResourceFieldSchema();
        intfs.setType(DataType.INTEGER);
        
        ResourceSchema tupleSchema = new ResourceSchema();
        tupleSchema.setFields(new ResourceFieldSchema[]{stringfs, intfs});
        ResourceFieldSchema tuplefs = new ResourceFieldSchema();
        tuplefs.setSchema(tupleSchema);
        tuplefs.setType(DataType.TUPLE);
        
        return tuplefs;
    }
    public static Tuple genRandSmallTuple(Random r, int limit){
        if(r==null){
            Tuple t = new DefaultTuple();
            t.append("RANDOM");
            return t;
        }
        Tuple t = new DefaultTuple();
        t.append(genRandString(r));
        t.append(r.nextInt(limit));
        return t;
    }
    
    public static Tuple genRandSmallTuple(String s, Integer value){
        Tuple t = new DefaultTuple();
        t.append(s);
        t.append(value);
        return t;
    }
    
    public static DataBag genRandSmallTupDataBagWithNulls(Random r, int num, int limit){
        if(r==null) {
            DataBag db = DefaultBagFactory.getInstance().newDefaultBag();
            Tuple t = new DefaultTuple();
            t.append("RANDOM");
            db.add(t);
            return db;
        }
        DataBag db = DefaultBagFactory.getInstance().newDefaultBag();
        for(int i=0;i<num;i++){
            // the first tuple is used as a sample tuple 
            // in some tests to deduce return type - so
            // don't introduce nulls into first tuple
            if(i == 0) {
                db.add(genRandSmallTuple(r, limit));
                continue;
            } else {
                int rand = r.nextInt(num);
                if(rand <= (0.2 * num) ) {
                    db.add(genRandSmallTuple((String)null, rand));
                } else if (rand > (0.2 * num) && rand <= (0.4 * num)) {
                    db.add(genRandSmallTuple(genRandString(r), null));
                } else if (rand > (0.4 * num) && rand <= (0.6 * num)) {
                    db.add(genRandSmallTuple(null, null));
                } else {
                    db.add(genRandSmallTuple(r, limit));
                }
            }
        }
        
        return db;
    }
    
    public static ResourceFieldSchema getSmallTupDataBagFieldSchema() throws IOException {
        ResourceFieldSchema tuplefs = getSmallTupleFieldSchema();
        
        ResourceSchema bagSchema = new ResourceSchema();
        bagSchema.setFields(new ResourceFieldSchema[]{tuplefs});
        ResourceFieldSchema bagfs = new ResourceFieldSchema();
        bagfs.setSchema(bagSchema);
        bagfs.setType(DataType.BAG);
        
        return bagfs;
    }
    
    public static DataBag genRandSmallTupDataBag(Random r, int num, int limit){
        if(r==null) {
            DataBag db = DefaultBagFactory.getInstance().newDefaultBag();
            Tuple t = new DefaultTuple();
            t.append("RANDOM");
            db.add(t);
            return db;
        }
        DataBag db = DefaultBagFactory.getInstance().newDefaultBag();
        for(int i=0;i<num;i++){
            db.add(genRandSmallTuple(r, limit));
        }
        return db;
    }
    
    public static Tuple genRandSmallBagTuple(Random r, int num, int limit){
        if(r==null){
            Tuple t = new DefaultTuple();
            t.append("RANDOM");
            return t;
        }
        Tuple t = new DefaultTuple();
        t.append(genRandSmallTupDataBag(r, num, limit));
        t.append(genRandDBA(r));
        t.append(genRandString(r));
        t.append(r.nextDouble());
        t.append(r.nextFloat());
        t.append(r.nextInt());
        t.append(r.nextLong());
        t.append(genRandMap(r, num));
        t.append(genRandSmallTuple(r, 100));
        t.append(new Boolean(r.nextBoolean()));
        return t;
    }
    
    public static ResourceFieldSchema getSmallBagTextTupleFieldSchema() throws IOException{
        ResourceFieldSchema dbafs = new ResourceFieldSchema();
        dbafs.setType(DataType.BYTEARRAY);
        
        ResourceFieldSchema stringfs = new ResourceFieldSchema();
        stringfs.setType(DataType.CHARARRAY);
        
        ResourceFieldSchema intfs = new ResourceFieldSchema();
        intfs.setType(DataType.INTEGER);
    	
        ResourceFieldSchema bagfs = getSmallTupDataBagFieldSchema();
        
        ResourceFieldSchema floatfs = new ResourceFieldSchema();
        floatfs.setType(DataType.FLOAT);
        
        ResourceFieldSchema doublefs = new ResourceFieldSchema();
        doublefs.setType(DataType.DOUBLE);
        
        ResourceFieldSchema longfs = new ResourceFieldSchema();
        longfs.setType(DataType.LONG);
        
        ResourceFieldSchema mapfs = new ResourceFieldSchema();
        mapfs.setType(DataType.MAP);
        
        ResourceFieldSchema tuplefs = getSmallTupleFieldSchema();

        ResourceFieldSchema boolfs = new ResourceFieldSchema();
        boolfs.setType(DataType.BOOLEAN);
        
        ResourceSchema outSchema = new ResourceSchema();
        outSchema.setFields(new ResourceFieldSchema[]{bagfs, dbafs, stringfs, doublefs, floatfs,
                intfs, longfs, mapfs, tuplefs, boolfs});
        ResourceFieldSchema outfs = new ResourceFieldSchema();
        outfs.setSchema(outSchema);
        outfs.setType(DataType.TUPLE);
        
        return outfs;
    }
    
    public static Tuple genRandSmallBagTextTuple(Random r, int num, int limit){
        if(r==null){
            Tuple t = new DefaultTuple();
            t.append("RANDOM");
            return t;
        }
        Tuple t = new DefaultTuple();
        t.append(genRandSmallTupDataBag(r, num, limit));
        //TODO Fix
        //The text representation of byte array and char array
        //cannot be disambiguated without annotation. For now,
        //the tuples will not contain byte array
        t.append(genRandTextDBA(r));
        t.append(genRandString(r));
        t.append(r.nextDouble());
        t.append(r.nextFloat());
        t.append(r.nextInt());
        t.append(r.nextLong());
        t.append(genRandMap(r, num));
        t.append(genRandSmallTuple(r, 100));
        t.append(new Boolean(r.nextBoolean()));
        return t;
    }
    
    public static DataBag genRandFullTupDataBag(Random r, int num, int limit){
        if(r==null) {
            DataBag db = DefaultBagFactory.getInstance().newDefaultBag();
            Tuple t = new DefaultTuple();
            t.append("RANDOM");
            db.add(t);
            return db;
        }
        DataBag db = DefaultBagFactory.getInstance().newDefaultBag();
        for(int i=0;i<num;i++){
            db.add(genRandSmallBagTuple(r, num, limit));
        }
        return db;
    }

    public static ResourceFieldSchema getFullTupTextDataBagFieldSchema() throws IOException{
        ResourceFieldSchema tuplefs = getSmallBagTextTupleFieldSchema();
        
        ResourceSchema outBagSchema = new ResourceSchema();
        outBagSchema.setFields(new ResourceFieldSchema[]{tuplefs});
        ResourceFieldSchema outBagfs = new ResourceFieldSchema();
        outBagfs.setSchema(outBagSchema);
        outBagfs.setType(DataType.BAG);
        
        return outBagfs;
    }
    
    public static DataBag genRandFullTupTextDataBag(Random r, int num, int limit){
        if(r==null) {
            DataBag db = DefaultBagFactory.getInstance().newDefaultBag();
            Tuple t = new DefaultTuple();
            t.append("RANDOM");
            db.add(t);
            return db;
        }
        DataBag db = DefaultBagFactory.getInstance().newDefaultBag();
        for(int i=0;i<num;i++){
            db.add(genRandSmallBagTextTuple(r, num, limit));
        }
        return db;
    }
    
    public static Tuple genRandSmallBagTupleWithNulls(Random r, int num, int limit){
        if(r==null){
            Tuple t = new DefaultTuple();
            t.append("RANDOM");
            return t;
        }
        Tuple t = new DefaultTuple();
        t.append(genRandSmallTupDataBag(r, num, limit));
        t.append(genRandDBA(r));
        t.append(genRandString(r));
        t.append(r.nextDouble());
        t.append(r.nextFloat());
        t.append(r.nextInt());
        t.append(r.nextLong());
        t.append(genRandMap(r, num));
        t.append(genRandSmallTuple(r, 100));
        t.append(new Boolean(r.nextBoolean()));
        t.append(null);
        return t;
    }

    public static Tuple genRandSmallBagTextTupleWithNulls(Random r, int num, int limit){
        if(r==null){
            Tuple t = new DefaultTuple();
            t.append("RANDOM");
            return t;
        }
        Tuple t = new DefaultTuple();
        t.append(genRandSmallTupDataBag(r, num, limit));
        //TODO Fix
        //The text representation of byte array and char array
        //cannot be disambiguated without annotation. For now,
        //the tuples will not contain byte array
        t.append(genRandTextDBA(r));
        t.append(genRandString(r));
        t.append(r.nextDouble());
        t.append(r.nextFloat());
        t.append(r.nextInt());
        t.append(r.nextLong());
        t.append(genRandMap(r, num));
        t.append(genRandSmallTuple(r, 100));
        t.append(new Boolean(r.nextBoolean()));
        t.append(null);
        return t;
    }
    
    public static DataBag genFloatDataBag(Random r, int column, int row) {
        DataBag db = DefaultBagFactory.getInstance().newDefaultBag();
        for (int i=0;i<row;i++) {
            Tuple t = TupleFactory.getInstance().newTuple();
            for (int j=0;j<column;j++) {
                t.append(r.nextFloat()*1000);
            }
            db.add(t);
        }
        return db;
    }
    
    public static ResourceFieldSchema getFloatDataBagFieldSchema(int column) throws IOException {
        ResourceFieldSchema intfs = new ResourceFieldSchema();
        intfs.setType(DataType.INTEGER);
        
        ResourceSchema tupleSchema = new ResourceSchema();
        ResourceFieldSchema[] fss = new ResourceFieldSchema[column];
        for (int i=0;i<column;i++) {
            fss[i] = intfs;
        }
        tupleSchema.setFields(fss);
        ResourceFieldSchema tuplefs = new ResourceFieldSchema();
        tuplefs.setSchema(tupleSchema);
        tuplefs.setType(DataType.TUPLE);
        
        ResourceSchema bagSchema = new ResourceSchema();
        bagSchema.setFields(new ResourceFieldSchema[]{tuplefs});
        ResourceFieldSchema bagfs = new ResourceFieldSchema();
        bagfs.setSchema(bagSchema);
        bagfs.setType(DataType.BAG);
        
        return bagfs;
    }
    
    public static Tuple genMixedTupleToConvert(Random r) {
        Tuple t = TupleFactory.getInstance().newTuple();
        t.append(r.nextInt());
        t.append(r.nextInt());
        long l = 0;
        while (l<=Integer.MAX_VALUE && l>=Integer.MIN_VALUE)
            l = r.nextLong();
        t.append(l);
        t.append(r.nextFloat()*1000);
        t.append(r.nextDouble()*10000);
        t.append(genRandString(r));
        t.append("K"+genRandString(r));
        t.append("K"+genRandString(r));
        t.append("K"+genRandString(r));
        if (r.nextFloat()>0.5)
            t.append("true");
        else
            t.append("false");
        return t;
    }
    
    public static ResourceFieldSchema getMixedTupleToConvertFieldSchema() throws IOException {
        ResourceFieldSchema stringfs = new ResourceFieldSchema();
        stringfs.setType(DataType.CHARARRAY);
        ResourceFieldSchema intfs = new ResourceFieldSchema();
        intfs.setType(DataType.INTEGER);
        ResourceFieldSchema longfs = new ResourceFieldSchema();
        longfs.setType(DataType.LONG);
        ResourceFieldSchema floatfs = new ResourceFieldSchema();
        floatfs.setType(DataType.FLOAT);
        ResourceFieldSchema doublefs = new ResourceFieldSchema();
        doublefs.setType(DataType.DOUBLE);
        ResourceFieldSchema boolfs = new ResourceFieldSchema();
        boolfs.setType(DataType.BOOLEAN);
        
        ResourceSchema tupleSchema = new ResourceSchema();
        tupleSchema.setFields(new ResourceFieldSchema[]{stringfs, longfs, intfs, doublefs, floatfs, stringfs, intfs, doublefs, floatfs, boolfs});
        ResourceFieldSchema tuplefs = new ResourceFieldSchema();
        tuplefs.setSchema(tupleSchema);
        tuplefs.setType(DataType.TUPLE);
        
        return tuplefs;
    }

    
}
