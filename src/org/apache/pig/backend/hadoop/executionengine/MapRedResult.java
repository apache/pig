package org.apache.pig.backend.hadoop.executionengine;

import org.apache.pig.impl.io.FileSpec;

public class MapRedResult {
    public FileSpec outFileSpec;
    public int parallelismRequest;
    
    public MapRedResult(FileSpec outFileSpec,
    					int parallelismRequest) {
        this.outFileSpec = outFileSpec;
        this.parallelismRequest = parallelismRequest;
    }
}

