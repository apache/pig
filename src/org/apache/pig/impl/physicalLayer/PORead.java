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
package org.apache.pig.impl.physicalLayer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;


public class PORead extends PhysicalOperator {
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	DataBag             bag;
    Iterator<Tuple> it;

    public PORead(DataBag bagIn, int outputType) {
    	super(outputType);
        inputs = new PhysicalOperator[0];

        bag = bagIn;

        it = null;
    }

    @Override
	public boolean open(boolean continueFromLast) throws IOException {
    	if (continueFromLast){
    		throw new RuntimeException("LOReads should not occur in continuous plans");
    	}
        it = bag.content();

        return true;
    }

    @Override
	public Tuple getNext() throws IOException {
        if (it.hasNext())
            return it.next();
        else
            return null;
    }

    public void visit(POVisitor v) {
        v.visitRead(this);
    }

}
