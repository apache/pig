package org.apache.pig.backend.hadoop.executionengine.spark.converter;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.data.Tuple;

abstract class POOutputConsumerIterator implements java.util.Iterator<Tuple> {
    private final java.util.Iterator<Tuple> input;
    private Result result = null;
    private boolean returned = true;
    private boolean finished = false;

    POOutputConsumerIterator(java.util.Iterator<Tuple> input) {
        this.input = input;
    }

    abstract protected void attach(Tuple tuple);

    abstract protected Result getNextResult() throws ExecException;

    private void readNext() {
        try {
            if (result != null && !returned) {
                return;
            }
            // see PigGenericMapBase
            if (result == null) {
                if (!input.hasNext()) {
                    finished = true;
                    return;
                }
                Tuple v1 = input.next();
                attach(v1);
            }
            result = getNextResult();
            returned = false;
            switch (result.returnStatus) {
            case POStatus.STATUS_OK:
                returned = false;
                break;
            case POStatus.STATUS_NULL:
                returned = true; // skip: see PigGenericMapBase
                readNext();
                break;
            case POStatus.STATUS_EOP:
                finished = !input.hasNext();
                if (!finished) {
                    result = null;
                    readNext();
                }
                break;
            case POStatus.STATUS_ERR:
                throw new RuntimeException("Error while processing " + result);
            }
        } catch (ExecException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean hasNext() {
        readNext();
        return !finished;
    }

    @Override
    public Tuple next() {
        readNext();
        if (finished) {
            throw new RuntimeException("Passed the end. call hasNext() first");
        }
        if (result == null || result.returnStatus != POStatus.STATUS_OK) {
            throw new RuntimeException("Unexpected response code in ForEach: "
                    + result);
        }
        returned = true;
        return (Tuple) result.result;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}