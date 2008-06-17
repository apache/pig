package org.apache.pig.piggybank.evaluation.math;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataAtom;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.AtomSchema;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * math.SCALB implements a binding to the Java function
* {@link java.lang.Math#scalb(double,int) Math.scalb(double,int)}. 
* Given a tuple with two data atom x and y ,it Return x × pow(2,y) 
* rounded as if performed by a single correctly rounded 
* floating-point multiply to a member of the double value set.
* 
* <dl>
* <dt><b>Parameters:</b></dt>
* <dd><code>value</code> - <code>Tuple containing two DataAtom [double]</code>.</dd>
* 
* <dt><b>Return Value:</b></dt>
* <dd><code>DataAtom [double]</code> </dd>
* 
* <dt><b>Return Schema:</b></dt>
* <dd>SCALB_inputSchema</dd>
* 
* <dt><b>Example:</b></dt>
* <dd><code>
* register math.jar;<br/>
* A = load 'mydata' using PigStorage() as ( float1 );<br/>
* B = foreach A generate float1, math.SCALB(float1);
* </code></dd>
* </dl>
* 
* @see Math#scalb(double,int)
* @see
* @author ajay garg
*
*/
public class SCALB extends EvalFunc<DataAtom>{
	
	/**
	 * java level API
	 * @param input expects a tuple containing two numeric DataAtom value
	 * @param output returns a single numeric DataAtom value, which is 
	 * fistArgument × pow(2,secondArgument) rounded as if performed by a 
	 * single correctly rounded floating-point multiply to a member of 
	 * the double value set.
	 */
	@Override
	public void exec(Tuple input, DataAtom output) throws IOException {
		output.setValue(scalb(input));
	}
	
	protected double scalb(Tuple input) throws IOException{
		double first;
		double second;
		try{
			first = input.getAtomField(0).numval();
			second = input.getAtomField(1).numval();
		}
		catch(Exception e){
			throw new IOException("invalid input ");
		}
		try{
			
			int sec= Double.valueOf(second).intValue();
			return Math.scalb(first, sec);
			
		}
		catch(NumberFormatException e){
			throw new IOException("Integer value is required");
		}
		
		
	}
	
	@Override
	public Schema outputSchema(Schema input) {
		return new AtomSchema("SCALB_"+input.toString()); 
	}


}
