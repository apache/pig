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

package org.apache.pig.backend.local.executionengine;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.Map;

import org.apache.pig.backend.executionengine.ExecPhysicalOperator;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.OperatorKey;
import org.apache.pig.impl.physicalLayer.POVisitor;
import org.apache.pig.impl.physicalLayer.PhysicalOperator;

public class PORead extends PhysicalOperator {
	
	DataBag inputBag;
	Iterator<Tuple> it;

	public PORead(String scope, long id,
			Map<OperatorKey, ExecPhysicalOperator> opTable, int outputType, DataBag bag) {
		super(scope, id, opTable, outputType);
		// TODO Auto-generated constructor stub
		inputs = new OperatorKey[0];
		this.inputBag = bag;
	}
	
	/*public PORead(DataBag bag) {
		this.inputBag = bag;
	}*/
	
	@Override
	public boolean open() throws IOException {
		it = inputBag.iterator();
		if(it == null) return false;
		return true;
	}

	@Override
	public Tuple getNext() throws IOException {
		// TODO Auto-generated method stub
		return it.next();
	}

	@Override
	public void visit(POVisitor v) {
		// TODO Auto-generated method stub
		v.visitRead(this);
	}

	

}
