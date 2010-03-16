package org.apache.pig;

import java.io.IOException;

/**
 * This interface implemented by {@link LoadFunc} implementations indicates to 
 * Pig that it has the capability to load data such that all instances of a key 
 * will occur in same split.
 * @since Pig 0.7
 */
public interface CollectableLoadFunc {

    /**
     * When this method is called, Pig is communicating to Loader that it must
     * load data such that all instances of a key are in same split. Pig will
     * make no further checks at runtime to ensure whether contract is honored
     * or not.
     * @throws IOException
     */
    public void ensureAllKeyInstancesInSameSplit() throws IOException;
}
