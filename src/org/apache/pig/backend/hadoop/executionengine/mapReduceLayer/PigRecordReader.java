/**
 * 
 */
package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.FuncSpec;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;

/**
 * A wrapper around the actual RecordReader and loadfunc - this is needed for
 * two reasons
 * 1) To intercept the initialize call from hadoop and initialize the underlying
 * actual RecordReader with the right Context object - this is achieved by 
 * looking up the Context corresponding to the input split this Reader is 
 * supposed to process
 * 2) We need to give hadoop consistent key-value types - text and tuple 
 * respectively - so PigRecordReader will call underlying Loader's getNext() to
 * get the Tuple value - the key is null text since key is not used in input to
 * map() in Pig.
 */
public class PigRecordReader extends RecordReader<Text, Tuple> {

    /**
     * the current Tuple value as returned by underlying
     * {@link LoadFunc#getNext()}
     */
    Tuple curValue = null;
    
    // the underlying RecordReader used by the loader
    @SuppressWarnings("unchecked")
    private RecordReader wrappedReader;
    
    // the loader object
    private LoadFunc loadfunc;
    
    /**
     * the Configuration object with data specific to the input the underlying
     * RecordReader will process (this is obtained after a 
     * {@link LoadFunc#setLocation(String, org.apache.hadoop.mapreduce.Job)} 
     * call and hence can contain specific properties the underlying
     * {@link InputFormat} might have put in.
     */
    private Configuration inputSpecificConf;
    /**
     * @param conf 
     * 
     */
    public PigRecordReader(RecordReader wrappedReader, 
            LoadFunc loadFunc, Configuration conf) {
        this.wrappedReader = wrappedReader; 
        this.loadfunc = loadFunc;
        this.inputSpecificConf = conf;
    }
    
    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.RecordReader#close()
     */
    @Override
    public void close() throws IOException {
        wrappedReader.close();        
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentKey()
     */
    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        // In pig we don't really use the key in the input to the map - so send
        // null
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentValue()
     */
    @Override
    public Tuple getCurrentValue() throws IOException, InterruptedException {
        return curValue;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.RecordReader#getProgress()
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return wrappedReader.getProgress();
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.RecordReader#initialize(org.apache.hadoop.mapreduce.InputSplit, org.apache.hadoop.mapreduce.TaskAttemptContext)
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        // initialize the underlying actual RecordReader with the right Context 
        // object - this is achieved by merging the Context corresponding to 
        // the input split this Reader is supposed to process with the context
        // passed in.
        PigSplit pigSplit = (PigSplit)split;
        ConfigurationUtil.mergeConf(context.getConfiguration(),
                inputSpecificConf);
        // Pass loader signature to LoadFunc and to InputFormat through
        // the conf
        PigInputFormat.passLoadSignature(loadfunc, pigSplit.getInputIndex(), 
                context.getConfiguration());
        // now invoke initialize() on underlying RecordReader with
        // the "adjusted" conf
        wrappedReader.initialize(pigSplit.getWrappedSplit(), context);
        loadfunc.prepareToRead(wrappedReader, pigSplit);
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.RecordReader#nextKeyValue()
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        curValue = loadfunc.getNext();
        return curValue != null;
    }

}
