package org.apache.pig.backend.hadoop.executionengine.tez;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.engine.api.Input;
import org.apache.tez.engine.api.Master;
import org.apache.tez.engine.api.Output;
import org.apache.tez.engine.api.Processor;

public class PigProcessor implements Processor {
    // Names of the properties that store serialized physical plans
    public static final String PLAN = "pig.exec.tez.plan";
    public static final String COMBINE_PLAN = "pig.exec.tez.combine.plan";

    @Override
    public void close() throws IOException, InterruptedException {
        // TODO Auto-generated method stub
    }

    @Override
    public void initialize(Configuration arg0, Master arg1) throws IOException,
            InterruptedException {
        // TODO Auto-generated method stub
    }

    @Override
    public void process(Input[] arg0, Output[] arg1) throws IOException,
            InterruptedException {
        // TODO Auto-generated method stub
    }
}

