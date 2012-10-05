package org.apache.pig;

public class PigConstants {
    private PigConstants() {}

    /**
     * This key is used in the job conf to let the various jobs know what code was
     * generated.
     */
    public static final String GENERATED_CLASSES_KEY = "pig.schematuple.classes";

    /**
     * This key is used when a job is run in local mode to pass the location of the generated code
     * from the front-end to the "back-end" (which, in this case, is in the same JVM).
     */
    public static final String LOCAL_CODE_DIR = "pig.schematuple.local.dir";

    // This makes it easy to turn SchemaTuple on globally.
    public static final boolean SCHEMA_TUPLE_ON_BY_DEFAULT = false;
}