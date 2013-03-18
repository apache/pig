package org.apache.pig.test.utils;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;

public class UDFContextTestLoaderWithSignature extends PigStorage {
    private String val;
    
    public UDFContextTestLoaderWithSignature(String v1) {
        val = v1;
    }
    
    @Override
    public void setLocation(String location, Job job)
            throws IOException {
        super.setLocation(location, job);
        Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass());
        if (p.get(signature)==null) {
            p.put("test_" + signature, val);
        }
    }
    
    @Override
    public Tuple getNext() throws IOException {
        Tuple t = super.getNext();
        if (t!=null) {
            Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass());
            t.append(p.get("test_" + signature));
        }
        return t;
    }
}
