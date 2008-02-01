package org.apache.pig.backend.datastorage;

import java.io.OutputStream;
import java.io.IOException;

public class ImmutableOutputStream extends OutputStream {
    
    private String destination;
    
    public ImmutableOutputStream(String destination) {
        super();
        this.destination = destination;
    }
    
    public void write(int b) throws IOException {
        throw new IOException("Write not supported on " + destination);
    }
}
