package org.apache.pig.impl.streaming;


public class StreamingUDFOutputHandler extends DefaultOutputHandler { 
    
    public StreamingUDFOutputHandler(PigStreamingUDF deserializer) {
        this.deserializer = deserializer;
    }
    
    @Override
    protected byte[] getRecordDelimiter() {
        return ( ((PigStreamingUDF)deserializer).getRecordDelim() );
    }
}
