/**
 * 
 */
package org.apache.pig;

import java.io.Serializable;

import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * A Class which will encapsulate metadata information that a
 * OutputFormat (or possibly StoreFunc) may want to know
 * about the data that needs to be stored.  
 */
public class StoreConfig implements Serializable {
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    private String location;
    private Schema schema;
    
    
    /**
     * @param location
     * @param schema
     */
    public StoreConfig(String location, Schema schema) {
        this.location = location;
        this.schema = schema;
    }
    
    /**
     * @return the location
     */
    public String getLocation() {
        return location;
    }
    /**
     * @param location the location to set
     */
    public void setLocation(String location) {
        this.location = location;
    }
    /**
     * @return the schema
     */
    public Schema getSchema() {
        return schema;
    }
    /**
     * @param schema the schema to set
     */
    public void setSchema(Schema schema) {
        this.schema = schema;
    }
    
    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "{location:" + location + ", schema:" + schema + "}";
    }

}
