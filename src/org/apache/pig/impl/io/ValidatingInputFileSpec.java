package org.apache.pig.impl.io;

import java.io.IOException;

import org.apache.pig.Slicer;
import org.apache.pig.PigServer.ExecType;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.executionengine.PigSlicer;
import org.apache.pig.impl.PigContext;

/**
 * Creates a Slicer using its funcSpec in its construction and checks that it's
 * valid.
 */
public class ValidatingInputFileSpec extends FileSpec {

    // Don't send the instantiated slicer over the wire.
    private transient Slicer slicer;

    private static final long serialVersionUID = 1L;

    public ValidatingInputFileSpec(FileSpec fileSpec, DataStorage store)
            throws IOException {
        super(fileSpec.getFileName(), fileSpec.getFuncSpec());
        validate(store);
    }

    /**
     * If the <code>ExecType</code> of <code>context</code> is LOCAL,
     * validation is not performed.
     */
    public ValidatingInputFileSpec(String fileName, String funcSpec,
            PigContext context) throws IOException {

        super(fileName, funcSpec);
        if (context.getExecType() != ExecType.LOCAL) {
            validate(context.getDfs());
        }
    }

    private void validate(DataStorage store) throws IOException {
        getSlicer().validate(store, getFileName());
    }

    /**
     * Returns the Slicer created by this spec's funcSpec.
     */
    public Slicer getSlicer() {
        if (slicer == null) {
            Object loader = PigContext.instantiateFuncFromSpec(getFuncSpec());
            if (loader instanceof Slicer) {
                slicer = (Slicer) loader;
            } else {
                slicer = new PigSlicer(getFuncSpec());
            }
        }
        return slicer;
    }
}
