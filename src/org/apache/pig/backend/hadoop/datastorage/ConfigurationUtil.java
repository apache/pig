package org.apache.pig.backend.hadoop.datastorage;

import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;

public class ConfigurationUtil {

    public static Configuration toConfiguration(Properties properties) {
        assert properties != null;
        final Configuration config = new Configuration();
        final Enumeration<Object> iter = properties.keys();
        while (iter.hasMoreElements()) {
            final String key = (String) iter.nextElement();
            final String val = properties.getProperty(key);
            config.set(key, val);
        }
        return config;
    }

    public static Properties toProperties(Configuration configuration) {
        Properties properties = new Properties();
        assert configuration != null;
        Iterator<Map.Entry<String, String>> iter = configuration.iterator();
        while (iter.hasNext()) {
            Map.Entry<String, String> entry = iter.next();
            properties.put(entry.getKey(), entry.getValue());
        }
        return properties;
    }
}
