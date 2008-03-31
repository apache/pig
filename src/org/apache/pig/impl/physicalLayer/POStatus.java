package org.apache.pig.impl.physicalLayer;

public class POStatus {
	public static final byte STATUS_OK = 0;
	public static final byte STATUS_NULL = 1;
	public static final byte STATUS_ERR = 2;
	public static final byte STATUS_EOP = 3; // end of processing 
	
	public static Object result;
}
