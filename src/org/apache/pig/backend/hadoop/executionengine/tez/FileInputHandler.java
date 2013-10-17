package org.apache.pig.backend.hadoop.executionengine.tez;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.PigException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.runtime.api.Input;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.library.api.KeyValueReader;

public class FileInputHandler implements InputHandler {
    MRInput mrInput;
    KeyValueReader reader;

    protected TupleFactory tf = TupleFactory.getInstance();
    
    @Override
    public boolean next() throws IOException {
        return reader.next();
    }

    @Override
    public Tuple getCurrentTuple() throws IOException {
        Tuple readTuple = (Tuple) reader.getCurrentValue();
        return tf.newTupleNoCopy(readTuple.getAll());
    }

    @Override
    public void initialize(Configuration conf, Map<String, LogicalInput> inputs) throws IOException {
        Input input = inputs.get("PigInput");
        if (input == null || !(input instanceof MRInput)){
            throw new PigException("Incorrect Input class. Expected sub-type of MRInput," +
            		" got " + input.getClass().getName());
        }
        
        mrInput = (MRInput) input;
        reader = mrInput.getReader();
    }
}
