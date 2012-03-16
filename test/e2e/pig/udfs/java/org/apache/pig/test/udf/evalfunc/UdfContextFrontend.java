package org.apache.pig.test.udf.evalfunc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;

public class UdfContextFrontend extends EvalFunc<Boolean> {
	
	public UdfContextFrontend(){}
	
	public UdfContextFrontend(String param){
		checkJobConf(); // call here and it will execute multiple times in front end
	}
	
	private void checkJobConf() {
		Configuration jobConf = UDFContext.getUDFContext().getJobConf();
		System.out.println("checkJobConf: conf is null: " + (jobConf == null) + " conf: "  + jobConf);
	}
	
	public Boolean exec(Tuple input) throws IOException {
		checkJobConf(); // will execute in map only
		return true;
	}
}
