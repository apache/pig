package org.apache.pig.impl.physicalLayer.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.pig.data.DataType;

public class operatorHelper {
	public static int numTypes(){
		byte[] types = genAllTypes();
		return types.length;
	}
	public static byte[] genAllTypes(){
		byte[] types = { DataType.BAG, DataType.BOOLEAN, DataType.BYTEARRAY, DataType.CHARARRAY, 
				DataType.DOUBLE, DataType.FLOAT, DataType.INTEGER, DataType.LONG, DataType.MAP, DataType.TUPLE};
		return types;
	}
	
	private static String[] genAllTypeNames(){
		String[] names = { "BAG", "BOOLEAN", "BYTEARRAY", "CHARARRAY", "DOUBLE", "FLOAT", "INTEGER", "LONG", 
				"MAP", "TUPLE" };
		return names;
	}
	
	public static Map<Byte, String> genTypeToNameMap(){
		byte[] types = genAllTypes();
		String[] names = genAllTypeNames();
		Map<Byte,String> ret = new HashMap<Byte, String>();
		for(int i=0;i<types.length;i++){
			ret.put(types[i], names[i]);
		}
		return ret;
	}
}
