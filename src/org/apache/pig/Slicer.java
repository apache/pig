package org.apache.pig;

import java.io.IOException;

import org.apache.pig.backend.datastorage.DataStorage;

/**
 * Produces independent slices of data from a given location to be processed in
 * parallel by Pig.
 * <p>
 * If a class implementing this interface is given as the LoadFunc in a Pig
 * script, it will be used to make slices for that load statement.
 */
public interface Slicer {
    /**
     * Checks that <code>location</code> is parsable by this Slicer, and that
     * if the DataStorage is used by the Slicer, it's readable from there. If it
     * isn't, an IOException with a message explaining why will be thrown.
     * <p>
     * This does not ensure that all the data in <code>location</code> is
     * valid. It's a preflight check that there's some chance of the Slicer
     * working before actual Slices are created and sent off for processing.
     */
    void validate(DataStorage store, String location) throws IOException;

    /**
     * Creates slices of data from <code>store</code> at <code>location</code>.
     * 
     * @return the Slices to be serialized and sent out to nodes for processing.
     */
    Slice[] slice(DataStorage store, String location) throws IOException;
}
