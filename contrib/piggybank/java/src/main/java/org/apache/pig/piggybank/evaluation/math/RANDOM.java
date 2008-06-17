package org.apache.pig.piggybank.evaluation.math;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataAtom;
import org.apache.pig.data.Tuple;

public class RANDOM extends EvalFunc<DataAtom>{

	@Override
	public void exec(Tuple input, DataAtom output) throws IOException {
		output.setValue(random(input));
	}
	
	protected double random(Tuple input) throws IOException{
		
		return Math.random();
		
	}

}
