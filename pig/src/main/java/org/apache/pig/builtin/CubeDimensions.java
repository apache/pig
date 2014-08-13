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
 * Produces a DataBag with all combinations of the argument tuple members
 * as in a data cube. Meaning, (a, b, c) will produce the following bag:
 * <pre>
 * { (a, b, c), (null, null, null), (a, b, null), (a, null, c),
 *   (a, null, null), (null, b, c), (null, null, c), (null, b, null) }
 * </pre>
 * <p>
 * The "all" marker is null by default, but can be set to an arbitrary string by
 * invoking a constructor (via a DEFINE). The constructor takes a single argument,
 * the string you want to represent "all".
 * <p>
 * Usage goes something like this:
 * <pre>{@code
 * events = load '/logs/events' using EventLoader() as (lang, event, app_id);
 * cubed = foreach x generate
 *   FLATTEN(piggybank.CubeDimensions(lang, event, app_id))
 *     as (lang, event, app_id),
 *   measure;
 * cube = foreach (group cubed
 *                 by (lang, event, app_id) parallel $P)
 *        generate
 *   flatten(group) as (lang, event, app_id),
 *   COUNT_STAR(cubed),
 *   SUM(measure);
 * store cube into 'event_cube';
 * }</pre>
 * <p>
 * <b>Note</b>: doing this with non-algebraic aggregations on large data can result
 * in very slow reducers, since one of the groups is going to get <i>all</i> the
 * records in your relation.
 */
public class CubeDimensions extends EvalFunc<DataBag> {

    private static BagFactory bf = BagFactory.getInstance();
    private static TupleFactory tf = TupleFactory.getInstance();
    private final String allMarker;
    private static final String unknown = "unknown";

    public CubeDimensions() {
        this(null);
    }
    public CubeDimensions(String allMarker) {
        super();
        this.allMarker = allMarker;
    }
    @Override
    public DataBag exec(Tuple tuple) throws IOException {
        List<Tuple> result = Lists.newArrayListWithCapacity((int) Math.pow(2, tuple.size()));
        convertNullToUnknown(tuple);
        Tuple newt = tf.newTuple(tuple.size());
        recursivelyCube(result, tuple, 0, newt);
        return bf.newDefaultBag(result);
    }

    // if the dimension values contain null then replace it with "unknown" value
    // since null will be used for rollups
    public static void convertNullToUnknown(Tuple tuple) throws ExecException {
	int idx = 0;
	for(Object obj : tuple.getAll()) {
	    if( (obj == null) ) {
		tuple.set(idx, unknown);
	    }
	    idx++;
	}
    }
    
    private void recursivelyCube(List<Tuple> result, Tuple input, int index, Tuple newt) throws ExecException {
        newt.set(index, input.get(index));
        if (index == input.size() - 1 ) {
            result.add(newt);
        } else {
            recursivelyCube(result, input, index + 1, newt);
        }
        // tf.newTuple makes a copy. tf.newTupleNoCopy doesn't.
        Tuple newnewt = tf.newTuple(newt.getAll());
        newnewt.set(index, allMarker);
        if (index == input.size() - 1) {
            result.add(newnewt);
        } else {
            recursivelyCube(result, input, index + 1, newnewt);
        }
    }

    @Override
    public Schema outputSchema(Schema input) {
        try {
            return new Schema(new FieldSchema("dimensions", input, DataType.BAG));
        } catch (FrontendException e) {
            // we are specifying BAG explicitly, so this should not happen.
            throw new RuntimeException(e);
        }
    }
}
