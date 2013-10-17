package org.apache.pig.backend.hadoop.executionengine.tez;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.apache.tez.runtime.library.input.ShuffledMergedInput;

/**
 * An input handler for shuffle inputs. Wraps and manages the POPackage, which will return tuples like
 * (key, {bag of tuples from input 1}, {bag of tuples from input 2}, ...)
 * 
 */
public class ShuffledInputHandler implements InputHandler {
    // For now we'll assume that there's just one shuffle input
    ShuffledMergedInput input;
    KeyValuesReader reader;
    
    //Move the package to the InputHandler since it's really part of input.
    POPackage pack;
    
    Tuple current;
    
    // CastIterator because KeyValueReader returns a Iterator<Objects>
    class CastIterator<E> implements Iterator<E>{
        Iterator iter;
        
        @SuppressWarnings("rawtypes")
        CastIterator(Iterator iter){
            this.iter = iter;
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public E next() {
            return (E) iter.next();
        }

        @Override
        public void remove() {
            iter.remove();
        }
    }
    
    @Override
    public void initialize(Configuration conf, Map<String, LogicalInput> inputs)
            throws IOException {
        //TODO: more than one shuffle input
        input = (ShuffledMergedInput) inputs.values().iterator().next();
        pack = (POPackage)ObjectSerializer.deserialize(conf.get("pig.reduce.package"));
        reader = input.getReader();
    }

    @Override
    public boolean next() throws IOException {
        //TODO: Handle POJoinPackage
        // join is not optimized, so package will
        // give only one tuple out for the key
        boolean next = reader.next();
        if (next){
            pack.attachInput((PigNullableWritable) reader.getCurrentKey(),
                    new CastIterator<NullableTuple>(reader.getCurrentValues().iterator()));
            Result result = pack.getNextTuple();
            if (result.returnStatus != POStatus.STATUS_OK){
                next = false;
                current = null;
            } else {
                current = (Tuple) result.result;
            }
        } else {
            current = null;
        }
        return next;
    }

    @Override
    public Tuple getCurrentTuple() throws IOException {
        return current;
    }

}
