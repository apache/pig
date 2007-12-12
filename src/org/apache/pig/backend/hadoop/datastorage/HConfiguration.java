package org.apache.pig.backend.hadoop.datastorage;

import java.util.Iterator;
import java.util.Map;
import java.util.Enumeration;

import org.apache.hadoop.conf.Configuration;

import java.util.Properties;


public class HConfiguration extends Properties {
    
    private static final long serialVersionUID = 1L;
        
    public HConfiguration() {
    }

    //
    // TODO: this implementation has a problem: it does not
    //       respect final attributes
    //
    public HConfiguration(Configuration other) {
        if (other != null) {
            Iterator<Map.Entry<String, String>> iter = other.iterator();
            
            while (iter.hasNext()) {
                Map.Entry<String, String> entry = iter.next();
                
                put(entry.getKey(), entry.getValue());
            }
        }
    }
        
    public Configuration getConfiguration() {
        Configuration config = new Configuration();

        Enumeration<Object> iter = keys();
        
        while (iter.hasMoreElements()) {
            String key = (String) iter.nextElement();
            String val = getProperty(key);
           
            config.set(key, val);
        }

        return config;
    }
}



