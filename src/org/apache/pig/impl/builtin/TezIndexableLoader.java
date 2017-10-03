package org.apache.pig.impl.builtin;

import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.FuncSpec;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.tez.runtime.TezInput;
import org.apache.pig.data.Tuple;
import org.apache.tez.runtime.api.LogicalInput;

public class TezIndexableLoader extends DefaultIndexableLoader {

    public TezIndexableLoader(String loaderFuncSpec, String scope, String inputLocation) {
        super(loaderFuncSpec, null, null, scope, inputLocation);
    }

    @Override
    public void loadIndex() throws ExecException {
        // no op
    }

    /**
     * Loads indices from provided LinkedList
     *
     * @param index
     * @throws ExecException
     */
    public void setIndex(LinkedList<Tuple> index) throws ExecException {
            this.index = index;
    }
}
