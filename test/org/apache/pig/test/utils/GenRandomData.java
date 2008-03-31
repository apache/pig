package org.apache.pig.test.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;

public class GenRandomData {
	
	
	public static Map<Integer,String> genRandMap(Random r, int numEnt) {
		Map<Integer,String> ret = new HashMap<Integer, String>();
		if(r==null){
			ret.put(1, "RANDOM");
			return ret;
		}
		for(int i=0;i<numEnt;i++){
			ret.put(r.nextInt(), genRandString(r));
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
	
	public static DataByteArray genRandDBA(Random r){
		if(r==null) return new DataByteArray("RANDOM".getBytes());
		byte[] bytes = new byte[10];
		r.nextBytes(bytes);
		return new DataByteArray(bytes);
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
	
	public static Tuple genRandSmallTuple(String s, int value){
		Tuple t = new DefaultTuple();
		t.append(s);
		t.append(value);
		return t;
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
		t.append(r.nextBoolean());
		t.append(genRandDBA(r));
		t.append(genRandString(r));
		t.append(r.nextDouble());
		t.append(r.nextFloat());
		t.append(r.nextInt());
		t.append(r.nextLong());
		t.append(genRandMap(r, num));
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
}
