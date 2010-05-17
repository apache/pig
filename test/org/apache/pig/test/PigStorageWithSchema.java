package org.apache.pig.test;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.Expression;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.builtin.PigStorage;

public class PigStorageWithSchema extends PigStorage implements LoadMetadata {
    private String signature;

    @Override
    public String[] getPartitionKeys(String location, Job job)
            throws IOException {
        return null;
    }

    @Override
    public ResourceSchema getSchema(String location, Job job)
            throws IOException {
        return null;
    }

    @Override
    public ResourceStatistics getStatistics(String location, Job job)
            throws IOException {
        return null;
    }

    @Override
    public void setPartitionFilter(Expression partitionFilter)
            throws IOException {
    }
    
    @Override
    public void setUDFContextSignature(String signature) {
        this.signature = signature;
    }
    
    public String getUDFContextSignature() {
        return signature;
    }
}
