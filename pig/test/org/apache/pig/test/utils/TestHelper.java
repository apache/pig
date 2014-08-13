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

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Map.Entry;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;

/**
 * Will contain static methods that will be useful
 * for unit tests
 *
 */
public class TestHelper {
    public static int dispAfterNumTuples = 1000;
    public static boolean bagContains(DataBag db, Tuple t) {
        Iterator<Tuple> iter = db.iterator();
        for (Tuple tuple : db) {
            if (tuple.compareTo(t) == 0 || tupleEquals(tuple, t))
                return true;
        }
        return false;
    }
    
    public static boolean compareBags(DataBag db1, DataBag db2) {
        if (db1.size() != db2.size())
            return false;
        
        boolean equal = true;
        for (Tuple tuple : db2) {
            boolean contains = false;
            for (Tuple tuple2 : db1) {
                if (tuple.compareTo(tuple2) == 0) {
                    contains = true;
                    break;
                }
            }
            if (!contains) {
                equal = false;
                break;
            }
        }
        return equal;
    }
    
    public static boolean compareCogroupOutputs(DataBag db1, DataBag db2) throws ExecException{
        
        if (db1.size() != db2.size())
            return false;
        
        Map<Object,DataBag> first  = new HashMap<Object, DataBag>();
        for (Tuple t : db1)
           first.put(t.get(0), (DataBag)t.get(1)); 
        
        Map<Object,DataBag> second = new HashMap<Object, DataBag>();
        for (Tuple t : db2)
            second.put(t.get(0), (DataBag)t.get(1)); 
        
        Set<Entry<Object,DataBag>> entrySet =  first.entrySet();
        
        for (Entry<Object,DataBag> entry : entrySet){
            Object key = entry.getKey();
            DataBag bagOfSecond = second.get(key);
            if(bagOfSecond == null)
                return false;
            boolean cmpVal = compareBags(bagOfSecond, entry.getValue());
            if(cmpVal == false)
                return false;
        }
        return true;
    }
    
    public static DataBag projectBag(DataBag db2, int i) throws ExecException {
        DataBag ret = DefaultBagFactory.getInstance().newDefaultBag();
        for (Tuple tuple : db2) {
            Object o = tuple.get(i);
            Tuple t1 = new DefaultTuple();
            t1.append(o);
            ret.add(t1);
        }
        return ret;
    }
    
    public static DataBag projectBag(DataBag db2, int[] fields) throws ExecException {
        DataBag ret = DefaultBagFactory.getInstance().newDefaultBag();
        for (Tuple tuple : db2) {
            Tuple t1 = new DefaultTuple();
            for (int fld : fields) {
                Object o = tuple.get(fld);
                t1.append(o);
            }
            ret.add(t1);
        }
        return ret;
    }
    
    public static int compareInputStreams(InputStream exp, InputStream act) throws IOException{
        byte[] bExp = new byte[4096], bAct = new byte[4096];
        
        int outLen,inLen = -1;
        while(act.read(bAct)!=-1){
            exp.read(bExp);
            int cmp = compareByteArray(bExp, bAct);
            if(cmp!=0)
                return cmp;
        }
        return 0;
    }
    
    public static int compareByteArray(byte[] b1, byte[] b2){
        if(b1.length>b2.length)
            return 1;
        else if(b1.length<b2.length)
            return -1;
        for(int i=0;i<b1.length;i++){
            if(b1[i]>b2[i])
                return 1;
            else if(b1[i]<b2[i])
                return -1;
        }
        return 0;
    }
    
    /*public static boolean areFilesSame(FileSpec expLocal, FileSpec actHadoop, PigContext pc, int dispAftNumTuples) throws ExecException, IOException{
        Random r = new Random();
        
        POLoad ldExp = new POLoad(new OperatorKey("", r.nextLong()));
        ldExp.setPc(pc);
        ldExp.setLFile(expLocal);
        
        POLoad ldAct = new POLoad(new OperatorKey("", r.nextLong()));
        ldAct.setPc(pc);
        ldAct.setLFile(actHadoop);
        
        Tuple t = null;
        int numActTuples = -1;
        boolean matches = true;
        for(Result resAct=ldAct.getNext(t);resAct.returnStatus!=POStatus.STATUS_EOP;resAct=ldAct.getNext(t)){
            Tuple tupAct = (Tuple)resAct.result;
            ++numActTuples;
            boolean found = false;
            for(Result resExp=ldExp.getNext(t);resExp.returnStatus!=POStatus.STATUS_EOP;resExp=ldExp.getNext(t)){
                Tuple tupExp = (Tuple)resExp.result;
                if(tupAct.compareTo(tupExp)==0){
                    found = true;
                    ldExp.tearDown();
                    break;
                }
            }
            if(!found){
                matches = false;
                break;
            }
            if(numActTuples%dispAftNumTuples ==0)
                System.out.println(numActTuples/dispAftNumTuples);
        }
        
        int numExpTuples = -1;
        while(ldExp.getNext(t).returnStatus!=POStatus.STATUS_EOP)
            ++numExpTuples;
        
        return (matches && numActTuples==numExpTuples);
    }*/
    
    public static boolean areFilesSame(FileSpec expLocal, FileSpec actHadoop, PigContext pc) throws ExecException, IOException{
        Random r = new Random();
        
        POLoad ldExp = new POLoad(new OperatorKey("", r.nextLong()));
        ldExp.setPc(pc);
        ldExp.setLFile(expLocal);
        
        POLoad ldAct = new POLoad(new OperatorKey("", r.nextLong()));
        ldAct.setPc(pc);
        ldAct.setLFile(actHadoop);
        
        Tuple t = null;
        int numActTuples = -1;
        DataBag bagAct = DefaultBagFactory.getInstance().newDefaultBag();
        Result resAct = null;
        while((resAct = ldAct.getNextTuple()).returnStatus!=POStatus.STATUS_EOP){
            ++numActTuples;
            bagAct.add(trimTuple((Tuple)resAct.result));
        }
        
        int numExpTuples = -1;
        DataBag bagExp = DefaultBagFactory.getInstance().newDefaultBag();
        Result resExp = null;
        while((resExp = ldExp.getNextTuple()).returnStatus!=POStatus.STATUS_EOP){
            ++numExpTuples;
            bagExp.add(trimTuple((Tuple)resExp.result));
        }
        
        if(numActTuples!=numExpTuples)
            return false;
        
        return compareBags(bagExp, bagAct);
    }
    
    private static Tuple trimTuple(Tuple t){
        Tuple ret = TupleFactory.getInstance().newTuple();
        for (Object o : t.getAll()) {
            DataByteArray dba = (DataByteArray)o;
            DataByteArray nDba = new DataByteArray(dba.toString().trim().getBytes());
            ret.append(nDba);
        }
        return ret;
    }

       /**
     * Create temp file from a given dataset
     * This assumes
     *  1) The dataset has at least 1 record
     *  2) All records are of the same size
     */
    public static File createTempFile(String[][] data) throws IOException {

        File fp1 = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(fp1));

        for(int i = 0; i < data.length ; i++) {

            // Building up string for each line
            StringBuilder sb = new StringBuilder() ;
            for(int j = 0 ; j < data[0].length ; j++) {
                if (j != 0) {
                    sb.append("\t") ;
                }
                sb.append(data[i][j]) ;
            }

            // Write the line to file
            ps.println(sb.toString());
        }

        ps.close();
        return fp1 ;
    }
    
    //a quick way to check for map equality as the map value returned by PigStorage has byte array
    public static boolean mapEquals(Map<String, Object> expectedMap, Map<String, Object> convertedMap) {
        if(expectedMap == null) {
            if(convertedMap != null) {
                return false;
            }
        } else {
            if (convertedMap == null) {
                return false;
            }
        }
        
        if(expectedMap.size() != convertedMap.size()) {
            return false;
        }
        
        for(String key: expectedMap.keySet()) {
            Object v = convertedMap.get(key);
            String convertedValue = new String(((DataByteArray)v).get());
            if(!expectedMap.get(key).toString().equals(convertedValue)) {
                return false;
            }
        }
        return true;
    }
    
    @SuppressWarnings("unchecked")
    public static boolean tupleEquals(Tuple expectedTuple, Tuple convertedTuple) {
        if(expectedTuple == null) {
            if(convertedTuple != null) {
                return false;
            }
        } else {
            if(convertedTuple == null) {
                return false;
            }
        }
        
        if(expectedTuple.size() != convertedTuple.size()) {
            return false;
        }
        
        for(int i = 0; i < expectedTuple.size(); ++i) {
            Object e ;
            Object c ;
            
            try {
                e = expectedTuple.get(i);
                c = convertedTuple.get(i);
            } catch (Exception e1) {
                return false;
            }
            
            if(e instanceof Map) {
                Map<String, Object> eMap = (Map<String, Object>)e;
                if(c instanceof Map) {
                    Map<String, Object> cMap = (Map<String, Object>)c;
                    if(!mapEquals(eMap, cMap)) {
                        return false;
                    }
                } else {
                    return false;
                }
            } else if (e instanceof Tuple) {
                if(c instanceof Tuple) {
                    if(!tupleEquals((Tuple)e, (Tuple)c)) {
                        return false;
                    }
                } else {
                    return false;
                }
            } else if (e instanceof DataBag){
                if(c instanceof DataBag) {
                    if(!bagEquals((DataBag)e, (DataBag)c)) {
                        return false;
                    }
                } else {
                    return false;
                }
            } else {
                if(e == null) {
                    if(c != null) {
                        return false;
                    }
                } else {
                    if(c == null) {
                        return false;
                    } else {
                        if(!e.equals(c)) {
                            return false;
                        }
                    }
                }
            }
        }
        
        return true;
    }   
    
    public static boolean bagEquals(DataBag expectedBag, DataBag convertedBag) {
        if(expectedBag == null) {
            if(convertedBag != null) {
                return false;
            }
        } else {
            if(convertedBag == null) {
                return false;
            }
        }
        
        if(expectedBag.size() != convertedBag.size()) {
            return false;
        }
        
        Iterator<Tuple> expectedBagIterator = expectedBag.iterator();
        Iterator<Tuple> convertedBagIterator = convertedBag.iterator();
        
        while(expectedBagIterator.hasNext()) {
            Tuple expectedBagTuple = expectedBagIterator.next();
            Tuple convertedBagTuple = convertedBagIterator.next();
            if(!tupleEquals(expectedBagTuple, convertedBagTuple)) {
                return false;
            }
        }
        
        return true;

    }

    /**
     * Find out the string which matches "regex" from "target", and sort the string with spacial
     * order. The string elements are split by "split".
     */
    public static String sortString(String regex, String target, String split) {
        Pattern p = Pattern.compile(regex);
        Matcher matcher = p.matcher(target);
        String original = null;
        String replaceString = new String();

        if (matcher.find()) {
            original = matcher.group(1);
            String[] out = original.split(split);
            Collections.sort(Arrays.asList(out));
            for (int j = 0; j < out.length; j++) {
                replaceString += (j > 0 ? ", " + out[j] : out[j]);
            }
            return target.replace(original, replaceString);
        }
        return target;
    }

    /**
     * Sort UDFs for golden plan
     */
    public static String sortUDFs(String goldenString) {
        String regex = "MapReduce\\([0-9]*\\,(.*)\\) - -[0-9]*\\:";
        String[] goldenArray = goldenString.split("\n");

        for (int i = 0; i < goldenArray.length; i++) {
            goldenString = goldenString.replace(goldenArray[i],
                    sortString(regex, goldenArray[i], ","));
        }

        return goldenString;
    }

    /**
     * sort subFields for LogMessages
     */
    public static List<String> sortSubFields(List<String> logMessages) {
        String regex = "\\[(.*)\\]";

        for (int i = 0; i < logMessages.size(); i++) {
            logMessages.set(i, sortString(regex, logMessages.get(i), ", "));
        }

        return logMessages;
    }

    public static String sortStringList(String text, String delimiter1, String delimiter2, String separator){
        Pattern pattern = Pattern.compile(String.format("(\\%s.*?\\%s)", delimiter1, delimiter2));
        Matcher matcher = pattern.matcher(text);
        String sortedString = text;

        while (matcher.find()) {
            // split and sort the list
            String value = matcher.group(1);
            value = value.substring(1,value.length()-1);
            String[] sortedList = value.split(separator);
            Arrays.sort(sortedList);

            // pretty-print the output and replace the unsorted string with the sorted one
            String sorted = Arrays.asList(sortedList).toString().replaceAll("(^.|.$)", "").replace(", ", separator);
            sorted = String.format("%s%s%s", delimiter1, sorted, delimiter2);
            sortedString = sortedString.replace(matcher.group(1), sorted);
        }   

        return sortedString;
    }   

}
