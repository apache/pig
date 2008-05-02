package org.apache.pig.impl.util;

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ConfigurationValidator {
    
    private final static Log log = LogFactory.getLog(PropertiesUtil.class);
    /***
     * All pig configurations should be validated in here before use
     * @param properties
     */
    
    public static void validatePigProperties(Properties properties) {
        ensureLongType(properties, "pig.spill.size.threshold", 0L) ;
        ensureLongType(properties, "pig.spill.gc.activation.size", Long.MAX_VALUE) ;   
    }
    
    private static void ensureLongType(Properties properties,
                                       String key, 
                                       long defaultValue) {
        String str = properties.getProperty(key) ;          
        if (str != null) {
            try {
                Long.parseLong(str) ;
            }
            catch (NumberFormatException  nfe) {
                log.error(str + " has to be parsable to long") ;
                properties.setProperty(key, Long.toString(defaultValue)) ;
            }
        }
        else {
            properties.setProperty(key, Long.toString(defaultValue)) ;
        }        
    }
}
