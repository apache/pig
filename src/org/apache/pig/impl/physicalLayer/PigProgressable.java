package org.apache.pig.impl.physicalLayer;

public interface PigProgressable {
    //Use to just inform that you are
    //alive
    public void progress();
    
    //If you have a status to report
    public void progress(String msg);
}
