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
package org.apache.pig.impl.physicalLayer.expressionOperators;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.physicalLayer.PhysicalOperator;
import org.apache.pig.impl.physicalLayer.POStatus;
import org.apache.pig.impl.physicalLayer.Result;
import org.apache.pig.impl.physicalLayer.plans.ExprPlanVisitor;
import org.apache.pig.impl.plan.VisitorException;

/**
 * This is just a cast that converts DataByteArray into either
 * String or Integer. Just added it for testing the POUnion. 
 * Need the full operator implementation.
 */
public class POCast extends ExpressionOperator {
    private String loadFSpec;
	transient private LoadFunc load;
	private Log log = LogFactory.getLog(getClass());
	
    private static final long serialVersionUID = 1L;

    public POCast(OperatorKey k) {
        super(k);
        // TODO Auto-generated constructor stub
    }

    public POCast(OperatorKey k, int rp) {
        super(k, rp);
        // TODO Auto-generated constructor stub
    }
    
    private void instantiateFunc() {
        if(load!=null) return;
        this.load = (LoadFunc) PigContext.instantiateFuncFromSpec(this.loadFSpec);
    }
    
    public void setLoadFSpec(String fSpec) {
    	this.loadFSpec = fSpec;
        instantiateFunc();
    }

    @Override
    public void visit(ExprPlanVisitor v) throws VisitorException {
        v.visitCast(this);

    }

    @Override
    public String name() {
        return "Cast" + "[" + DataType.findTypeName(resultType) + "]" +" - " + mKey.toString();
    }

    @Override
    public boolean supportsMultipleInputs() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Result getNext(Integer i) throws ExecException {
    	PhysicalOperator in = inputs.get(0);
    	Byte resultType = in.getResultType();
        switch(resultType) {
        case DataType.BAG : {
        	Result res = new Result();
        	res.returnStatus = POStatus.STATUS_ERR;
        	return res;
        }
        
        case DataType.TUPLE : {
        	Result res = new Result();
        	res.returnStatus = POStatus.STATUS_ERR;
        	return res;
        }

        case DataType.BYTEARRAY : {
        	DataByteArray dba = null;
        	Result res = in.getNext(dba);
        	if(res.returnStatus == POStatus.STATUS_OK) {
        		//res.result = new Integer(Integer.valueOf((((DataByteArray)res.result).toString())));
        		dba = (DataByteArray) res.result;
        		try {
					res.result = load.bytesToInteger(dba.get());
				} catch (IOException e) {
					log.error("Error while casting from ByteArray to Integer");
				}
        	}
        	return res;
        }
        
        case DataType.MAP : {
        	Result res = new Result();
        	res.returnStatus = POStatus.STATUS_ERR;
        	return res;        	
        }
        
        case DataType.BOOLEAN : {
        	Boolean b = null;
        	Result res = in.getNext(b);
        	if(res.returnStatus == POStatus.STATUS_OK) {
        		if (((Boolean)res.result) == true) res.result = new Integer(1);
                else res.result = new Integer(0);
        	}
        	return res;
        }
        case DataType.INTEGER : {
        	
        	Result res = in.getNext(i);
        	return res;
        }

        case DataType.DOUBLE : {
        	Double d = null;
        	Result res = in.getNext(d);
        	if(res.returnStatus == POStatus.STATUS_OK) {
        		//res.result = DataType.toInteger(res.result);
        		res.result = new Integer(((Double)res.result).intValue());
        	}
        	return res;
        }

        case DataType.LONG : {
        	Long l = null;
        	Result res = in.getNext(l);
        	if(res.returnStatus == POStatus.STATUS_OK) {
        		res.result = new Integer(((Long)res.result).intValue());
        	}
        	return res;
        }
        
        case DataType.FLOAT : {
        	Float f = null;
        	Result res = in.getNext(f);
        	if(res.returnStatus == POStatus.STATUS_OK) {
        		res.result = new Integer(((Float)res.result).intValue());
        	}
        	return res;
        }
        
        case DataType.CHARARRAY : {
        	String str = null;
        	Result res = in.getNext(str);
        	if(res.returnStatus == POStatus.STATUS_OK) {
        		res.result = new Integer(Integer.valueOf((String)res.result));
        	}
        	return res;
        }
        
        }
        
        Result res = new Result();
        res.returnStatus = POStatus.STATUS_ERR;
        return res;
    }
    
    @Override
    public Result getNext(Long l) throws ExecException {
    	PhysicalOperator in = inputs.get(0);
    	Byte resultType = in.getResultType();
        switch(resultType) {
        case DataType.BAG : {
        	Result res = new Result();
        	res.returnStatus = POStatus.STATUS_ERR;
        	return res;
        }
        
        case DataType.TUPLE : {
        	Result res = new Result();
        	res.returnStatus = POStatus.STATUS_ERR;
        	return res;
        }

        case DataType.MAP : {
        	Result res = new Result();
        	res.returnStatus = POStatus.STATUS_ERR;
        	return res;        	
        }
                
        case DataType.BYTEARRAY : {
        	DataByteArray dba = null;
        	Result res = in.getNext(dba);
        	if(res.returnStatus == POStatus.STATUS_OK) {
        		//res.result = new Long(Long.valueOf((((DataByteArray)res.result).toString())));
        		dba = (DataByteArray) res.result;
        		try {
					res.result = load.bytesToLong(dba.get());
				} catch (IOException e) {
					log.error("Error while casting from ByteArray to Long");
				}
        	}
        	return res;
        }
        
        case DataType.BOOLEAN : {
        	Boolean b = null;
        	Result res = in.getNext(b);
        	if(res.returnStatus == POStatus.STATUS_OK) {
        		if (((Boolean)res.result) == true) res.result = new Long(1);
                else res.result = new Long(0);
        	}
        	return res;
        }
        case DataType.INTEGER : {
        	Integer dummyI = null;
        	Result res = in.getNext(dummyI);
        	if(res.returnStatus == POStatus.STATUS_OK) {
        		res.result = new Long(((Integer)res.result).longValue());
        	}
        	return res;
        }

        case DataType.DOUBLE : {
        	Double d = null;
        	Result res = in.getNext(d);
        	if(res.returnStatus == POStatus.STATUS_OK) {
        		//res.result = DataType.toInteger(res.result);
        		res.result = new Long(((Double)res.result).longValue());
        	}
        	return res;
        }

        case DataType.LONG : {
        	
        	Result res = in.getNext(l);
        	
        	return res;
        }
        
        case DataType.FLOAT : {
        	Float f = null;
        	Result res = in.getNext(f);
        	if(res.returnStatus == POStatus.STATUS_OK) {
        		res.result = new Long(((Float)res.result).longValue());
        	}
        	return res;
        }
        
        case DataType.CHARARRAY : {
        	String str = null;
        	Result res = in.getNext(str);
        	if(res.returnStatus == POStatus.STATUS_OK) {
        		res.result = new Long(Long.valueOf((String)res.result));
        	}
        	return res;
        }
        
        }
        
        Result res = new Result();
        res.returnStatus = POStatus.STATUS_ERR;
        return res;
    }
    
    @Override
    public Result getNext(Double d) throws ExecException {
    	PhysicalOperator in = inputs.get(0);
    	Byte resultType = in.getResultType();
        switch(resultType) {
        case DataType.BAG : {
        	Result res = new Result();
        	res.returnStatus = POStatus.STATUS_ERR;
        	return res;
        }
        
        case DataType.TUPLE : {
        	Result res = new Result();
        	res.returnStatus = POStatus.STATUS_ERR;
        	return res;
        }

        case DataType.MAP : {
        	Result res = new Result();
        	res.returnStatus = POStatus.STATUS_ERR;
        	return res;        	
        }
                
        case DataType.BYTEARRAY : {
        	DataByteArray dba = null;
        	Result res = in.getNext(dba);
        	if(res.returnStatus == POStatus.STATUS_OK) {
        		//res.result = new Double(Double.valueOf((((DataByteArray)res.result).toString())));
        		dba = (DataByteArray) res.result;
        		try {
					res.result = load.bytesToDouble(dba.get());
				} catch (IOException e) {
					log.error("Error while casting from ByteArray to Double");
				}
        	}
        	return res;
        }
        
        case DataType.BOOLEAN : {
        	Boolean b = null;
        	Result res = in.getNext(b);
        	if(res.returnStatus == POStatus.STATUS_OK) {
        		if (((Boolean)res.result) == true) res.result = new Double(1);
                else res.result = new Double(0);
        	}
        	return res;
        }
        case DataType.INTEGER : {
        	Integer dummyI = null;
        	Result res = in.getNext(dummyI);
        	if(res.returnStatus == POStatus.STATUS_OK) {
        		res.result = new Double(((Integer)res.result).doubleValue());
        	}
        	return res;
        }

        case DataType.DOUBLE : {
        	
        	Result res = in.getNext(d);
        	
        	return res;
        }

        case DataType.LONG : {
        	Long l = null;
        	Result res = in.getNext(l);
        	if(res.returnStatus == POStatus.STATUS_OK) {
        		res.result = new Double(((Long)res.result).doubleValue());
        	}
        	return res;
        }
        
        case DataType.FLOAT : {
        	Float f = null;
        	Result res = in.getNext(f);
        	if(res.returnStatus == POStatus.STATUS_OK) {
        		res.result = new Double(((Float)res.result).doubleValue());
        	}
        	return res;
        }
        
        case DataType.CHARARRAY : {
        	String str = null;
        	Result res = in.getNext(str);
        	if(res.returnStatus == POStatus.STATUS_OK) {
        		res.result = new Double(Double.valueOf((String)res.result));
        	}
        	return res;
        }
        
        }
        
        Result res = new Result();
        res.returnStatus = POStatus.STATUS_ERR;
        return res;
    }
    
    @Override
    public Result getNext(Float f) throws ExecException {
    	PhysicalOperator in = inputs.get(0);
    	Byte resultType = in.getResultType();
        switch(resultType) {
        case DataType.BAG : {
        	Result res = new Result();
        	res.returnStatus = POStatus.STATUS_ERR;
        	return res;
        }
        
        case DataType.TUPLE : {
        	Result res = new Result();
        	res.returnStatus = POStatus.STATUS_ERR;
        	return res;
        }

        case DataType.MAP : {
        	Result res = new Result();
        	res.returnStatus = POStatus.STATUS_ERR;
        	return res;        	
        }
                
        case DataType.BYTEARRAY : {
        	DataByteArray dba = null;
        	Result res = in.getNext(dba);
        	if(res.returnStatus == POStatus.STATUS_OK) {
        		//res.result = new Float(Float.valueOf((((DataByteArray)res.result).toString())));
        		dba = (DataByteArray) res.result;
        		try {
					res.result = load.bytesToFloat(dba.get());
				} catch (IOException e) {
					log.error("Error while casting from ByteArray to Float");
				}
        	}
        	return res;
        }
        
        case DataType.BOOLEAN : {
        	Boolean b = null;
        	Result res = in.getNext(b);
        	if(res.returnStatus == POStatus.STATUS_OK) {
        		if (((Boolean)res.result) == true) res.result = new Float(1);
                else res.result = new Float(0);
        	}
        	return res;
        }
        case DataType.INTEGER : {
        	Integer dummyI = null;
        	Result res = in.getNext(dummyI);
        	if(res.returnStatus == POStatus.STATUS_OK) {
        		res.result = new Float(((Integer)res.result).floatValue());
        	}
        	return res;
        }

        case DataType.DOUBLE : {
        	Double d = null;
        	Result res = in.getNext(d);
        	if(res.returnStatus == POStatus.STATUS_OK) {
        		//res.result = DataType.toInteger(res.result);
        		res.result = new Float(((Double)res.result).floatValue());
        	}
        	return res;
        }

        case DataType.LONG : {
        	
        	Long l = null;
        	Result res = in.getNext(l);
        	if(res.returnStatus == POStatus.STATUS_OK) {
        		res.result = new Float(((Long)res.result).floatValue());
        	}
        	return res;
        }
        
        case DataType.FLOAT : {
        
        	Result res = in.getNext(f);
        	
        	return res;
        }
        
        case DataType.CHARARRAY : {
        	String str = null;
        	Result res = in.getNext(str);
        	if(res.returnStatus == POStatus.STATUS_OK) {
        		res.result = new Float(Float.valueOf((String)res.result));
        	}
        	return res;
        }
        
        }
        
        Result res = new Result();
        res.returnStatus = POStatus.STATUS_ERR;
        return res;
    }
    
    @Override
    public Result getNext(String str) throws ExecException {
    	PhysicalOperator in = inputs.get(0);
    	Byte resultType = in.getResultType();
        switch(resultType) {
        case DataType.BAG : {
        	Result res = new Result();
        	res.returnStatus = POStatus.STATUS_ERR;
        	return res;
        }
        
        case DataType.TUPLE : {
        	Result res = new Result();
        	res.returnStatus = POStatus.STATUS_ERR;
        	return res;
        }

        case DataType.MAP : {
        	Result res = new Result();
        	res.returnStatus = POStatus.STATUS_ERR;
        	return res;        	
        }
                
        case DataType.BYTEARRAY : {
        	DataByteArray dba = null;
        	Result res = in.getNext(dba);
        	if(res.returnStatus == POStatus.STATUS_OK) {
        		//res.result = new String(((DataByteArray)res.result).toString());
        		dba = (DataByteArray) res.result;
        		try {
					res.result = load.bytesToCharArray(dba.get());
				} catch (IOException e) {
					log.error("Error while casting from ByteArray to CharArray");
				}
        	}
        	return res;
        }
        
        case DataType.BOOLEAN : {
        	Boolean b = null;
        	Result res = in.getNext(b);
        	if(res.returnStatus == POStatus.STATUS_OK) {
        		if (((Boolean)res.result) == true) res.result = new String("1");
                else res.result = new String("1");
        	}
        	return res;
        }
        case DataType.INTEGER : {
        	Integer dummyI = null;
        	Result res = in.getNext(dummyI);
        	if(res.returnStatus == POStatus.STATUS_OK) {
        		res.result = new String(((Integer)res.result).toString());
        	}
        	return res;
        }

        case DataType.DOUBLE : {
        	Double d = null;
        	Result res = in.getNext(d);
        	if(res.returnStatus == POStatus.STATUS_OK) {
        		//res.result = DataType.toInteger(res.result);
        		res.result = new String(((Double)res.result).toString());
        	}
        	return res;
        }

        case DataType.LONG : {
        	
        	Long l = null;
        	Result res = in.getNext(l);
        	if(res.returnStatus == POStatus.STATUS_OK) {
        		res.result = new String(((Long)res.result).toString());
        	}
        	return res;
        }
        
        case DataType.FLOAT : {
        	Float f = null;
        	Result res = in.getNext(f);
        	if(res.returnStatus == POStatus.STATUS_OK) {
        		res.result = new String(((Float)res.result).toString());
        	}
        	return res;
        }
        
        case DataType.CHARARRAY : {
        	
        	Result res = in.getNext(str);
        	
        	return res;
        }
        
        }
        
        Result res = new Result();
        res.returnStatus = POStatus.STATUS_ERR;
        return res;
    }
    
    @Override
    public Result getNext(DataByteArray dba) throws ExecException {
    	String str = null;
    	Result res = getNext(str);
    	if(res.returnStatus == POStatus.STATUS_OK) {
    		res.result = new DataByteArray(((String)res.result).getBytes());
    	}
    	return res;
    }
    
    @Override
    public Result getNext(Tuple t) throws ExecException {
    	PhysicalOperator in = inputs.get(0);
    	Byte resultType = in.getResultType();
        switch(resultType) {
        
        case DataType.TUPLE : {
        	Result res = in.getNext(t);
        	return res;
        }
        
        case DataType.BYTEARRAY : {
        	DataByteArray dba = null;
        	Result res = in.getNext(dba);
        	if(res.returnStatus == POStatus.STATUS_OK) {
        		//res.result = new String(((DataByteArray)res.result).toString());
        		dba = (DataByteArray) res.result;
        		try {
					res.result = load.bytesToTuple(dba.get());
				} catch (IOException e) {
					log.error("Error while casting from ByteArray to Tuple");
				}
        	}
        	return res;
        }

        case DataType.BAG :
        
        case DataType.MAP : 
        	
        case DataType.INTEGER :
        	
        case DataType.DOUBLE :
        	
        case DataType.LONG :
        	
        case DataType.FLOAT :
        	
        case DataType.CHARARRAY :
        
        case DataType.BOOLEAN : {
        	Result res = new Result();
        	res.returnStatus = POStatus.STATUS_ERR;
        	return res; 
        }
        
        
        }
        
        Result res = new Result();
        res.returnStatus = POStatus.STATUS_ERR;
        return res;
    }
    
    @Override
    public Result getNext(DataBag bag) throws ExecException {
    	PhysicalOperator in = inputs.get(0);
    	Byte resultType = in.getResultType();
        switch(resultType) {
        
        case DataType.BAG : {
        	Result res = in.getNext(bag);
        	return res;
        }
        
        case DataType.BYTEARRAY : {
        	DataByteArray dba = null;
        	Result res = in.getNext(dba);
        	if(res.returnStatus == POStatus.STATUS_OK) {
        		//res.result = new String(((DataByteArray)res.result).toString());
        		dba = (DataByteArray) res.result;
        		try {
					res.result = load.bytesToBag(dba.get());
				} catch (IOException e) {
					log.error("Error while casting from ByteArray to DataBag");
				}
        	}
        	return res;
        }

        case DataType.TUPLE :
        
        case DataType.MAP : 
        	
        case DataType.INTEGER :
        	
        case DataType.DOUBLE :
        	
        case DataType.LONG :
        	
        case DataType.FLOAT :
        	
        case DataType.CHARARRAY :
        
        case DataType.BOOLEAN : {
        	Result res = new Result();
        	res.returnStatus = POStatus.STATUS_ERR;
        	return res; 
        }
        
        
        }
        
        Result res = new Result();
        res.returnStatus = POStatus.STATUS_ERR;
        return res;
    }
    
    @Override
    public Result getNext(Map m) throws ExecException {
    	PhysicalOperator in = inputs.get(0);
    	Byte resultType = in.getResultType();
        switch(resultType) {
        
        case DataType.MAP : {
        	Result res = in.getNext(m);
        	return res;
        }
        
        case DataType.BYTEARRAY : {
        	DataByteArray dba = null;
        	Result res = in.getNext(dba);
        	if(res.returnStatus == POStatus.STATUS_OK) {
        		//res.result = new String(((DataByteArray)res.result).toString());
        		dba = (DataByteArray) res.result;
        		try {
					res.result = load.bytesToMap(dba.get());
				} catch (IOException e) {
					log.error("Error while casting from ByteArray to Map");
				}
        	}
        	return res;
        }

        case DataType.TUPLE :
        
        case DataType.BAG : 
        	
        case DataType.INTEGER :
        	
        case DataType.DOUBLE :
        	
        case DataType.LONG :
        	
        case DataType.FLOAT :
        	
        case DataType.CHARARRAY :
        
        case DataType.BOOLEAN : {
        	Result res = new Result();
        	res.returnStatus = POStatus.STATUS_ERR;
        	return res; 
        }
        
        
        }
        
        Result res = new Result();
        res.returnStatus = POStatus.STATUS_ERR;
        return res;
    }
    
    private void readObject(ObjectInputStream is) throws IOException, ClassNotFoundException{
        is.defaultReadObject();
        instantiateFunc();
    }
    

}
