package org.apache.pig.piggybank.evaluation.math;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataAtom;
import org.apache.pig.data.Datum;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.AtomSchema;
import org.apache.pig.impl.logicalLayer.schema.Schema;
/**
 * math.ASIN implements a binding to the Java function
* {@link java.lang.Math#asin(double) Math.asin(double)} for computing the
* arc sine of value of the argument. The returned value will be a double which is 
* the arc sine of the value of  input.
* 
* <dl>
* <dt><b>Parameters:</b></dt>
* <dd><code>value</code> - <code>DataAtom [double]</code>.</dd>
* 
* <dt><b>Return Value:</b></dt>
* <dd><code>DataAtom [double]</code> arc sine of the value of input</dd>
* 
* <dt><b>Return Schema:</b></dt>
* <dd>asin_inputSchema</dd>
* 
* <dt><b>Example:</b></dt>
* <dd><code>
* register math.jar;<br/>
* A = load 'mydata' using PigStorage() as ( float1 );<br/>
* B = foreach A generate float1, math.ASIN(float1);
* </code></dd>
* </dl>
* 
* @see Math#asin(double)
* @see
* @author ajay garg
*
*/
public class ASIN extends EvalFunc<DataAtom>{
	/**
	 * java level API
	 * @param input expects a single numeric DataAtom value
	 * @param output returns a single numeric DataAtom value, arc sine value of the argument
	 */
	@Override
	public void exec(Tuple input, DataAtom output) throws IOException {
		output.setValue(asin(input));
	}
	
	protected double asin(Tuple input) throws IOException{
		Datum temp = input.getField(0);
		double retVal;
		if(!(temp instanceof DataAtom)){
			throw new IOException("invalid input format. ");
		} 
		else{
			try{
				retVal=((DataAtom)temp).numval();
			}
			catch(RuntimeException e){
				throw new IOException((DataAtom)temp+" is not a valid number");
			}
		}
		return Math.asin(retVal);
		
	}
	
	@Override
	public Schema outputSchema(Schema input) {
		return new AtomSchema("asin_"+input.toString()); 
	}

}
