package org.apache.pig.piggybank.evaluation.math;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataAtom;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.AtomSchema;
import org.apache.pig.impl.logicalLayer.schema.Schema;
/**
 * math.IEEEremainder implements a binding to the Java function
* {@link java.lang.Math#IEEEremainder(double,double) Math.IEEEremainder(double,double)}. 
* Given a tuple with two data atom it Returns the remainder operation on two 
* arguments as prescribed by the IEEE 754 standard.
* 
* <dl>
* <dt><b>Parameters:</b></dt>
* <dd><code>value</code> - <code>Tuple containing two DataAtom [double]</code>.</dd>
* 
* <dt><b>Return Value:</b></dt>
* <dd><code>DataAtom [double]</code> </dd>
* 
* <dt><b>Return Schema:</b></dt>
* <dd>IEEEremainder_inputSchema</dd>
* 
* <dt><b>Example:</b></dt>
* <dd><code>
* register math.jar;<br/>
* A = load 'mydata' using PigStorage() as ( float1 );<br/>
* B = foreach A generate float1, math.IEEEremainder(float1);
* </code></dd>
* </dl>
* 
* @see Math#IEEEremainder(double,double)
* @see
* @author ajay garg
*
*/
public class IEEEremainder extends EvalFunc<DataAtom>{
	/**
	 * java level API
	 * @param input expects a tuple containing two numeric DataAtom value
	 * @param output returns a single numeric DataAtom value, which is 
	 * the remainder operation on two arguments as prescribed by the 
	 * IEEE 754 standard.
	 */
	@Override
	public void exec(Tuple input, DataAtom output) throws IOException {
		output.setValue(remainder(input));
	}
	
	protected double remainder(Tuple input) throws IOException{
		try{
			double first = input.getAtomField(0).numval();
			double second = input.getAtomField(1).numval();
			return Math.IEEEremainder(first, second);
		}
		catch(RuntimeException e){
			throw new IOException("invalid input "+e.getMessage());
		}
		
	}
	@Override
	public Schema outputSchema(Schema input) {
		return new AtomSchema("IEEEremainder_"+input.toString()); 
	}

}
