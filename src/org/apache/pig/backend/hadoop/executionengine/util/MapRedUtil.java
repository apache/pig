/**
 * 
 */
package org.apache.pig.backend.hadoop.executionengine.util;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;
import org.apache.pig.PigException;
import org.apache.pig.StoreConfig;
import org.apache.pig.StoreFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.util.ObjectSerializer;

/**
 * A class of utility static methods to be used in the hadoop map reduce backend
 */
public class MapRedUtil {

    /**
     * This method is to be called from an 
     * {@link org.apache.hadoop.mapred.OutputFormat#getRecordWriter(FileSystem ignored, JobConf job,
                                     String name, Progressable progress)}
     * method to obtain a reference to the {@link org.apache.pig.StoreFunc} object to be used by
     * that OutputFormat to perform the write() operation
     * @param conf the JobConf object
     * @return the StoreFunc reference
     * @throws ExecException
     */
    public static StoreFunc getStoreFunc(JobConf conf) throws ExecException {
        StoreFunc store;
        try {
            String storeFunc = conf.get("pig.storeFunc", "");
            if (storeFunc.length() == 0) {
                store = new PigStorage();
            } else {
                storeFunc = (String) ObjectSerializer.deserialize(storeFunc);
                store = (StoreFunc) PigContext
                        .instantiateFuncFromSpec(storeFunc);
            }
        } catch (Exception e) {
            int errCode = 2081;
            String msg = "Unable to setup the store function.";
            throw new ExecException(msg, errCode, PigException.BUG, e);
        }
        return store;
    }
    
    /**
     * This method is to be called from an 
     * {@link org.apache.hadoop.mapred.OutputFormat#getRecordWriter(FileSystem ignored, JobConf job,
                                     String name, Progressable progress)}
     * method to obtain a reference to the {@link org.apache.pig.StoreConfig} object. The StoreConfig
     * object will contain metadata information like schema and location to be used by
     * that OutputFormat to perform the write() operation
     * @param conf the JobConf object
     * @return StoreConfig object containing metadata information useful for
     * an OutputFormat to write the data
     * @throws IOException
     */
    public static StoreConfig getStoreConfig(JobConf conf) throws IOException {
        return (StoreConfig) ObjectSerializer.deserialize(conf.get(JobControlCompiler.PIG_STORE_CONFIG));
    }
}
