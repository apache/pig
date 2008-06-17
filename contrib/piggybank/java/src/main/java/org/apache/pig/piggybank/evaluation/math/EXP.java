package org.apache.pig.piggybank.evaluation.math;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataAtom;
import org.apache.pig.data.Datum;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.AtomSchema;
import org.apache.pig.impl.logicalLayer.schema.Schema;
/**
 * math.EXP implements a binding to the Java function
* {@link java.lang.Math#exp(double) Math.exp(double)}. Given a single 
* data atom it  Returns the Euler's number e raised to the power of input
* 
* <dl>
* <dt><b>Parameters:</b></dt>
* <dd><code>value</code> - <code>DataAtom [double]</code>.</dd>
* 
* <dt><b>Return Value:</b></dt>
* <dd><code>DataAtom [double]</code> </dd>
* 
* <dt><b>Return Schema:</b></dt>
* <dd>exp_inputSchema</dd>
* 
* <dt><b>Example:</b></dt>
* <dd><code>
* register math.jar;<br/>
* A = load 'mydata' using PigStorage() as ( float1 );<br/>
* B = foreach A generate float1, math.exp(float1);
* </code></dd>
* </dl>
* 
* @see Math#exp(double)
* @see
* @author ajay garg
*
*/
public class EXP extends EvalFunc<DataAtom>{
	/**
	 * java level API
	 * @param input expects a single numeric DataAtom value
	 * @param output returns a single numeric DataAtom value, exponential value of the argument
	 */
	@Override
	public void exec(Tuple input, DataAtom output) throws IOException {
		output.setValue(exp(input));
	}
	
	protected double exp(Tuple input) throws IOException{
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
		return Math.exp(retVal);
		
	}
	@Override
	public Schema outputSchema(Schema input) {
		return new AtomSchema("exp_"+input.toString()); 
	}

}
