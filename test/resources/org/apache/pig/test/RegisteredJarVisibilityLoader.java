package org.apache.pig.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.impl.util.UDFContext;

import java.io.IOException;
import java.util.Properties;

/**
 * Please see {@link TestRegisteredJarVisibility} for information about this class.
 */
public class RegisteredJarVisibilityLoader extends PigStorage {
    private static final Log LOG = LogFactory.getLog(RegisteredJarVisibilityLoader.class);
    private static final String REGISTERED_JAR_VISIBILITY_SCHEMA = "registered.jar.visibility.schema";

    @Override
    public void setLocation(String location, Job job) throws IOException {
        UDFContext udfContext = UDFContext.getUDFContext();
        Properties properties = udfContext.getUDFProperties(RegisteredJarVisibilityLoader.class);

        if (!properties.containsKey(REGISTERED_JAR_VISIBILITY_SCHEMA)) {
            LOG.info("Storing " + RegisteredJarVisibilitySchema.class.getName() + " in UDFContext.");
            properties.put(REGISTERED_JAR_VISIBILITY_SCHEMA, new RegisteredJarVisibilitySchema());
            LOG.info("Stored " + RegisteredJarVisibilitySchema.class.getName() + " in UDFContext.");
        } else {
            LOG.info("Retrieving " + REGISTERED_JAR_VISIBILITY_SCHEMA + " from UDFContext.");
            RegisteredJarVisibilitySchema registeredJarVisibilitySchema =
                    (RegisteredJarVisibilitySchema) properties.get(REGISTERED_JAR_VISIBILITY_SCHEMA);
            LOG.info("Retrieved " + REGISTERED_JAR_VISIBILITY_SCHEMA + " from UDFContext.");
        }

        super.setLocation(location, job);
    }
}
