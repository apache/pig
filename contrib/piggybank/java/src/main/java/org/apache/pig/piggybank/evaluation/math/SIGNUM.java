package org.apache.pig.piggybank.evaluation.math;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataAtom;
import org.apache.pig.data.Datum;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.AtomSchema;
import org.apache.pig.impl.logicalLayer.schema.Schema;
/**
 * math.SIGNUM implements a binding to the Java function
* {@link java.lang.Math#signum(double) Math.signum(double)}. 
* Given a single data atom it Returns the signum function of the argument.
* 
* <dl>
* <dt><b>Parameters:</b></dt>
* <dd><code>value</code> - <code>DataAtom [double]</code>.</dd>
* 
* <dt><b>Return Value:</b></dt>
* <dd><code>DataAtom [double]</code> </dd>
* 
* <dt><b>Return Schema:</b></dt>
* <dd>SIGNUM_inputSchema</dd>
* 
* <dt><b>Example:</b></dt>
* <dd><code>
* register math.jar;<br/>
* A = load 'mydata' using PigStorage() as ( float1 );<br/>
* B = foreach A generate float1, math.SIGNUM(float1);
* </code></dd>
* </dl>
* 
* @see Math#signum(double)
* @see
* @author ajay garg
*
*/
public class SIGNUM extends EvalFunc<DataAtom>{
	/**
	 * java level API
	 * @param input expects a single numeric DataAtom value
	 * @param output returns a single numeric DataAtom value, 
	 * signum function of the argument
	 */
	@Override
	public void exec(Tuple input, DataAtom output) throws IOException {
		output.setValue(signum(input));
	}
	
	protected double signum(Tuple input) throws IOException{
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
		return Math.signum(retVal);
		
	}
	
	@Override
	public Schema outputSchema(Schema input) {
		return new AtomSchema("SIGNUM_"+input.toString()); 
	}

}
