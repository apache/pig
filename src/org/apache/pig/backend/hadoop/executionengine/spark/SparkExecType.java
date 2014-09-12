package org.apache.pig.backend.hadoop.executionengine.spark;

import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.backend.executionengine.ExecutionEngine;
import org.apache.pig.impl.PigContext;

public class SparkExecType implements ExecType {

    private static final long serialVersionUID = 1L;
    private static final String mode = "SPARK";

    @Override
    public boolean accepts(Properties properties) {
        String execTypeSpecified = properties.getProperty("exectype", "")
                .toUpperCase();
        if (execTypeSpecified.equals(mode))
            return true;
        return false;
    }

    @Override
    public ExecutionEngine getExecutionEngine(PigContext pigContext) {
        return new SparkExecutionEngine(pigContext);
    }

    @Override
    public Class<? extends ExecutionEngine> getExecutionEngineClass() {
        return SparkExecutionEngine.class;
    }

    @Override
    public boolean isLocal() {
        return false;
    }

    @Override
    public String name() {
        return "SPARK";
    }

    public String toString() {
        return name();
    }
}
