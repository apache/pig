package org.apache.pig.backend.local.executionengine;

import java.util.Iterator;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.FileSpec;

public class LocalResult {
    // could record other information like when file was first
    // created, how many times it's been re-used (in this context we
    // are inside the same scope)
    // ...
    //
    public FileSpec outFileSpec;
    
    public LocalResult(FileSpec outFileSpec) {
        this.outFileSpec = outFileSpec;
    }
}

