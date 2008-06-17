package org.apache.pig.piggybank.evaluation.math;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataAtom;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.AtomSchema;
import org.apache.pig.impl.logicalLayer.schema.Schema;
/**
 * math.ATAN2 implements a binding to the Java function
* {@link java.lang.Math#atan2(double,double) Math.atan2(double,double)}. Given a tuple with
* two data atom x and y it will returns the angle theta from the conversion of 
* rectangular coordinates (x, y) to polar coordinates (r, theta).
* 
* <dl>
* <dt><b>Parameters:</b></dt>
* <dd><code>value</code> - <code>Tuple containing two DataAtom [double]</code>.</dd>
* 
* <dt><b>Return Value:</b></dt>
* <dd><code>DataAtom [double]</code> </dd>
* 
* <dt><b>Return Schema:</b></dt>
* <dd>atan2_inputSchema</dd>
* 
* <dt><b>Example:</b></dt>
* <dd><code>
* register math.jar;<br/>
* A = load 'mydata' using PigStorage() as ( float1 );<br/>
* B = foreach A generate float1, math.ATAN2(float1);
* </code></dd>
* </dl>
* 
* @see Math#atan2(double,double)
* @see
* @author ajay garg
*
*/
public class ATAN2 extends EvalFunc<DataAtom>{
	/**
	 * java level API
	 * @param input expects a tuple containing two numeric DataAtom value
	 * @param output returns a single numeric DataAtom value
	 */
	@Override
	public void exec(Tuple input, DataAtom output) throws IOException {
		output.setValue(atan2(input));
	}
	
	protected double atan2(Tuple input) throws IOException{
		try{
			double first = input.getAtomField(0).numval();
			double second = input.getAtomField(1).numval();
			return Math.atan2(first, second);
		}
		catch(RuntimeException e){
			throw new IOException("invalid input " + e.getMessage());
		}
		
	}
	
	@Override
	public Schema outputSchema(Schema input) {
		return new AtomSchema("atan2_"+input.toString()); 
	}

}
