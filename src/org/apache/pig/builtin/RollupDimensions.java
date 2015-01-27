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
package org.apache.pig.builtin;

import java.io.IOException;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import com.google.common.collect.Lists;

/**
 * Produces a DataBag with hierarchy of values (from the most detailed level of
 * aggregation to most general level of aggregation) of the specified dimensions
 * For example, (a, b, c) will produce the following bag:
 * 
 * <pre>
 * { (a, b, c), (a, b, null), (a, null, null), (null, null, null) }
 * </pre>
 */
public class RollupDimensions extends EvalFunc<DataBag> {

    private static BagFactory bf = BagFactory.getInstance();
    private static TupleFactory tf = TupleFactory.getInstance();
    private final String allMarker;
    // the pivot position
    private int pivot = -1;
    // to check if rollup is optimized or not
    private boolean rollupHIIoptimizable = false;

    public RollupDimensions() {
	this(null);
    }

    public RollupDimensions(String allMarker) {
	super();
	this.allMarker = allMarker;
    }

    public void setRollupHIIOptimizable(boolean check) {
        this.rollupHIIoptimizable = check;
    }

    public boolean getRollupHIIOptimizable() {
        return this.rollupHIIoptimizable;
    }

    public void setPivot(int pvt) throws IOException {
        this.pivot = pvt;
    }

    @Override
    public DataBag exec(Tuple tuple) throws IOException {
	List<Tuple> result = Lists.newArrayListWithCapacity(tuple.size() + 1);
	CubeDimensions.convertNullToUnknown(tuple);
	result.add(tuple);
	iterativelyRollup(result, tuple);
	return bf.newDefaultBag(result);
    }

    private void iterativelyRollup(List<Tuple> result, Tuple input)
            throws IOException {

        Tuple tempTup = tf.newTuple(input.getAll());

        //if (this.rollupHIIoptimizable != null) { // rule is enabled
            if (this.rollupHIIoptimizable == true) {
                if (this.pivot == -1) // user did not specify the pivot position
                                      // --> IRG approach
                    return;
                else { // user did specify the pivot position --> IRG + IRG
                    if (this.pivot == 0) // we use the IRG approach
                        return;
                    else { // we use IRG+IRG approach
                        for (int i = this.pivot - 1; i < input.size(); i++)
                            tempTup.set(i, allMarker);
                        result.add(tf.newTuple(tempTup.getAll()));
                    }
                }
            }
            else { // we can not optimize --> Vanilla approach
            for (int i = input.size() - 1; i >= 0; i--) {
                tempTup.set(i, allMarker);
                result.add(tf.newTuple(tempTup.getAll()));
            }
        }
    }

    @Override
    public Schema outputSchema(Schema input) {
	// "dimensions" string is the default namespace assigned to the output
	// schema. this can be overridden by specifying user defined schema
	// names in foreach operator. if user defined schema names are not
	// specified then the output schema of foreach operator using this UDF
	// will have "dimensions::" namespace for all fields in the tuple
	try {
	    return new Schema(new FieldSchema("dimensions", input, DataType.BAG));
	} catch (FrontendException e) {
	    // we are specifying BAG explicitly, so this should not happen.
	    throw new RuntimeException(e);
	}
    }

    @Override
    public boolean allowCompileTimeCalculation() {
        return true;
    }
}
