package org.apache.pig.impl.streaming;

import org.apache.pig.PigStreamingBase;

public class StreamingUDFInputHandler extends DefaultInputHandler {
    
    public StreamingUDFInputHandler(PigStreamingBase serializer) {
        this.serializer = serializer;
    }
}