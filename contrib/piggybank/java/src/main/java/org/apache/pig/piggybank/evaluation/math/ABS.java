package org.apache.pig.piggybank.evaluation.math;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataAtom;
import org.apache.pig.data.Datum;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.AtomSchema;
import org.apache.pig.impl.logicalLayer.schema.Schema;
/**
  * math.ABS implements a binding to the Java function
 * {@link java.lang.Math#abs(double) Math.abs(double)} for computing the
 * absolute value of the argument. The returned value will be a double which is 
 * absolute value of the input.
 * 
 * <dl>
 * <dt><b>Parameters:</b></dt>
 * <dd><code>value</code> - <code>DataAtom [double]</code>.</dd>
 * 
 * <dt><b>Return Value:</b></dt>
 * <dd><code>DataAtom [double]</code> absolute value of input</dd>
 * 
 * <dt><b>Return Schema:</b></dt>
 * <dd>abs_inputSchema</dd>
 * 
 * <dt><b>Example:</b></dt>
 * <dd><code>
 * register math.jar;<br/>
 * A = load 'mydata' using PigStorage() as ( float1 );<br/>
 * B = foreach A generate float1, math.ABS(float1);
 * </code></dd>
 * </dl>
 * 
 * @see Math#abs(double)
 * @see
 * @author ajay garg
 *
 */
public class ABS extends EvalFunc<DataAtom>{
	/**
	 * java level API
	 * @param input expects a single numeric DataAtom value
	 * @param output returns a single numeric DataAtom value, absolute value of the argument
	 */
	@Override
	public void exec(Tuple input, DataAtom output) throws IOException {
		output.setValue(abs(input));
	}
	

	protected double abs(Tuple input) throws IOException{
		Datum temp = input.getField(0);
		double retVal;
		if(!(temp instanceof DataAtom)){
			throw new IOException("invalid input format. ");
		} 
		else{
			try{
				retVal=Math.abs(((DataAtom)temp).numval());
			}
			catch(RuntimeException e){
				throw new IOException((DataAtom)temp+" is not a valid number");
			}
		}
		return retVal;
		
	}
	
	@Override
	public Schema outputSchema(Schema input) {
		return new AtomSchema("abs_"+input.toString()); 
	}
	
	

}
