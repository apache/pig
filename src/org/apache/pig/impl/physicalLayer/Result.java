package org.apache.pig.impl.physicalLayer;

import java.io.Serializable;

public class Result implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public byte returnStatus;
	public Object result;
	
	public Result(){
		returnStatus = POStatus.STATUS_ERR;
		result = null;
	}
}
