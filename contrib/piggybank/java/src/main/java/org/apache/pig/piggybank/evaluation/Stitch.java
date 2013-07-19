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
package org.apache.pig.piggybank.evaluation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/**
 * Given a set of bags, stitch them together tuple by tuple.  That is, 
 * assuming the bags have row numbers join them by row number.  So given
 * two bags
 * <p> {(1, 2), (3, 4)} and
 * <p> {(5, 6), (7, 8)} the result will be
 * <p> {(1, 2, 5, 6), (3, 4, 7, 8)}
 * In general it is assumed that each bag has the same number of tuples.
 * The implementation uses the first bag to determine the number of tuples
 * placed in the output.  If bags beyond the first have fewer tuples then
 * the resulting tuples will have fewer fields.  Nulls will not be filled in.
 * <p>Any number of bags can be passed to this function.
 */
public class Stitch extends EvalFunc<DataBag> {

    @Override
    public DataBag exec(Tuple input) throws IOException {

        if (input == null || input.size() == 0) return null;

        List<DataBag> bags = new ArrayList<DataBag>(input.size());

        for (int i = 0; i < input.size(); i++) {
            Object o = input.get(i);
            try {
                bags.add((DataBag)o);
            } catch (ClassCastException cce) {
                int errCode = 2107; // TODO not sure this is the right one
                String msg = "Stitch expected bags as input but argument " +
                    i + " is a " + DataType.findTypeName(o);
                throw new ExecException(msg, errCode, PigException.INPUT);
            }
        }

        if (bags.size() == 1) return bags.get(0);

        DataBag output = BagFactory.getInstance().newDefaultBag();
        List<Iterator<Tuple>> iters = new ArrayList<Iterator<Tuple>>(bags.size());
        for (DataBag bag : bags) {
            iters.add(bag.iterator());
        }

        while (iters.get(0).hasNext()) {
            Tuple outTuple = TupleFactory.getInstance().newTuple();
            for (Iterator<Tuple> iter : iters) {
                if (iter.hasNext()) {
                    Tuple t = iter.next();
                    List<Object> fields = t.getAll();
                    for (Object field : fields) {
                        outTuple.append(field);
                    }
                }
            }
            output.add(outTuple);
        }
        return output;
    }

    @Override
    public Schema outputSchema(Schema inputSch) {
        // We should get a tuple full of bags.  Merge the schema of all the
        // bags so we get one bag with the unioned schema.
        List<FieldSchema> fields = inputSch.getFields();

        try {
            Schema bagSchema = null;
            for (FieldSchema field : fields) {
                if (field.type != DataType.BAG) {
                    throw new RuntimeException( "Only bags should be passed to "
                            + "Stitch, schema indicates a " +
                            DataType.findTypeName(field.type) + " being passed");
                }
                if (bagSchema == null && field.schema != null ) {
                    // Copy the schema of this bag, we'll add additional fields
                    // to it from the other bags
                    if (field.schema.getField(0).type == DataType.TUPLE) {
                        // need to go one more level
                        field = field.schema.getField(0);
                    }
                    bagSchema = new Schema(field.schema);
                } else if (field.schema != null) {
                    // Append the fields of this bag to our bag's schema
                    if (field.schema.getField(0).type == DataType.TUPLE) {
                        field = field.schema.getField(0);
                    }

                    for (FieldSchema fs : field.schema.getFields()) {
                        bagSchema.add(fs);
                    }
                }
            }

            FieldSchema outFS =
                new FieldSchema("stitched", bagSchema, DataType.BAG);
            return new Schema(outFS);
        } catch (FrontendException fe) {
            throw new RuntimeException("Unable to create nested schema", fe);
        }
    }
}

