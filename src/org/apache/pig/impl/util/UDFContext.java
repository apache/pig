/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.impl.util;

import java.io.IOException;
//import java.io.Serializable;
import java.util.HashMap;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;

import org.apache.pig.impl.util.ObjectSerializer;

public class UDFContext {
    
    private Configuration jconf = null;
    private HashMap<Integer, Properties> udfConfs;
    private Properties clientSysProps;
    private static final String CLIENT_SYS_PROPS = "pig.client.sys.props";
    private static final String UDF_CONTEXT = "pig.udf.context"; 
    private static UDFContext self = null;
    private UDFContext() {
        udfConfs = new HashMap<Integer, Properties>();
    }

    public static UDFContext getUDFContext() {
        if (self == null) {
            self = new UDFContext();
        }
        return self;
    }

    // internal pig use only - should NOT be called from user code
    public void setClientSystemProps() {
        clientSysProps = System.getProperties();        
    }
    
    /**
     * Get the System Properties (Read only) as on the client machine from where Pig
     * was launched. This will include command line properties passed at launch
     * time
     * @return client side System Properties including command line properties
     */
    public Properties getClientSystemProps() {
        return clientSysProps;
    }
    /**
     * Adds the JobConf to this singleton.  Will be 
     * called on the backend by the Map and Reduce 
     * functions so that UDFs can obtain the JobConf
     * on the backend.
     */
    public void addJobConf(Configuration conf) {
        jconf = conf;
    }

    /**
     * Get the JobConf.  This should only be called on
     * the backend.  It will return null on the frontend.
     * @return JobConf for this job.  This is a copy of the
     * JobConf.  Nothing written here will be kept by the system.
     * getUDFConf should be used for recording UDF specific
     * information.
     */
    public Configuration getJobConf() {
        if (jconf != null)  return new Configuration(jconf);
        else return null;
    }

    /**
     * Get a properties object that is specific to this UDF.
     * Note that if a given UDF is called multiple times in a script, 
     * and each instance passes different arguments, then each will
     * be provided with different configuration object.
     * This can be used by loaders to pass their input object path
     * or URI and separate themselves from other instances of the
     * same loader.  Constructor arguments could also be used,
     * as they are available on both the front and back end.
     *
     * Note that this can only be used to share information
     * across instantiations of the same function in the front end
     * and between front end and back end.  It cannot be used to
     * share information between instantiations (that is, between
     * map and/or reduce instances) on the back end at runtime.
     * @param c of the UDF obtaining the properties object.
     * @param args String arguments that make this instance of
     * the UDF unique.
     * @return A reference to the properties object specific to
     * the calling UDF.  This is a reference, not a copy.
     * Any changes to this object will automatically be 
     * propogated to other instances of the UDF calling this 
     * function.
     */
    
    @SuppressWarnings("unchecked")
    public Properties getUDFProperties(Class c, String[] args) {
        Integer k = generateKey(c, args);
        Properties p = udfConfs.get(k);
        if (p == null) {
            p = new Properties();
            udfConfs.put(k, p);
        }
        return p;
    }
    
     /**
     * Get a properties object that is specific to this UDF.
     * Note that if a given UDF is called multiple times in a script, 
     * they will all be provided the same configuration object.  It
     * is up to the UDF to make sure the multiple instances do not
     * stomp on each other.
     *
     * It is guaranteed that this properties object will be separate
     * from that provided to any other UDF.
     *
     * Note that this can only be used to share information
     * across instantiations of the same function in the front end
     * and between front end and back end.  It cannot be used to
     * share information between instantiations (that is, between
     * map and/or reduce instances) on the back end at runtime.
     * @param c of the UDF obtaining the properties object.
     * @return A reference to the properties object specific to
     * the calling UDF.  This is a reference, not a copy.
     * Any changes to this object will automatically be 
     * propogated to other instances of the UDF calling this 
     * function.
     */
    @SuppressWarnings("unchecked")
    public Properties getUDFProperties(Class c) {
        Integer k = generateKey(c);
        Properties p = udfConfs.get(k);
        if (p == null) {
            p = new Properties();
            udfConfs.put(k, p);
        }
        return p;
    }
    
    /**
     * Serialize the UDF specific information into an instance
     * of JobConf.  This function is intended to be called on
     * the front end in preparation for sending the data to the
     * backend.
     * @param conf JobConf to serialize into
     * @throws IOException if underlying serialization throws it
     */
    public void serialize(Configuration conf) throws IOException {
        conf.set(UDF_CONTEXT, ObjectSerializer.serialize(udfConfs));
        conf.set(CLIENT_SYS_PROPS, ObjectSerializer.serialize(clientSysProps));
    }
    
    /**
     * Populate the udfConfs field.  This function is intended to
     * be called by Map.configure or Reduce.configure on the backend.
     * It assumes that addJobConf has already been called.
     * @throws IOException if underlying deseralization throws it
     */
    @SuppressWarnings("unchecked")
    public void deserialize() throws IOException {  
        udfConfs = (HashMap<Integer, Properties>)ObjectSerializer.deserialize(jconf.get(UDF_CONTEXT));
        clientSysProps = (Properties)ObjectSerializer.deserialize(
                jconf.get(CLIENT_SYS_PROPS));
    }
    
    @SuppressWarnings("unchecked")
    private int generateKey(Class c) {
        return c.getName().hashCode();
    }
    
    @SuppressWarnings("unchecked")
    private int generateKey(Class c, String[] args) {
        int hc = c.getName().hashCode();
        for (int i = 0; i < args.length; i++) {
            hc <<= 1;
            hc ^= args[i].hashCode();
        }
        return hc;
    }
    
    public void reset() {
        udfConfs.clear();
    }
    
    public boolean isUDFConfEmpty() {
        return udfConfs.isEmpty();
    }
}