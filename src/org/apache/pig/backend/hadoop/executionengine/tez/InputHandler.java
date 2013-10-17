package org.apache.pig.backend.hadoop.executionengine.tez;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.data.Tuple;
import org.apache.tez.runtime.api.LogicalInput;

public interface InputHandler
{
    public void initialize(Configuration conf, Map<String, LogicalInput> inputs) throws IOException;
    public boolean next() throws IOException;
    public Tuple getCurrentTuple() throws IOException;
}