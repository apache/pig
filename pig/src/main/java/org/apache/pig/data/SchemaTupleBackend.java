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
package org.apache.pig.data;

import static org.apache.pig.PigConfiguration.SHOULD_USE_SCHEMA_TUPLE;
import static org.apache.pig.PigConstants.SCHEMA_TUPLE_ON_BY_DEFAULT;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.ExecType;
import org.apache.pig.PigConstants;
import org.apache.pig.data.SchemaTupleClassGenerator.GenContext;
import org.apache.pig.data.utils.StructuresHelper.SchemaKey;
import org.apache.pig.data.utils.StructuresHelper.Triple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

public class SchemaTupleBackend {
    private static final Log LOG = LogFactory.getLog(SchemaTupleBackend.class);

    private Set<String> filesToResolve = Sets.newHashSet();

        /**
         * We use the URLClassLoader to resolve the generated classes because we can
         * simply give it the directory we put all of the compiled class files into,
         * and it will handle the dynamic loading.
         */
    private URLClassLoader classLoader;
    private Map<Triple<SchemaKey, Boolean, GenContext>, SchemaTupleFactory> schemaTupleFactoriesByTriple = Maps.newHashMap();
    private Map<Integer, SchemaTupleFactory> schemaTupleFactoriesById = Maps.newHashMap();
    private Configuration jConf;

    private File codeDir;

    private boolean isLocal;
    private boolean abort = false;

    /**
     * The only information this class needs is a directory of generated code to resolve
     * classes in.
     * @param jConf
     * @param directory of generated code
     */
    private SchemaTupleBackend(Configuration jConf, boolean isLocal) {
        if (isLocal) {
            String localCodeDir = jConf.get(PigConstants.LOCAL_CODE_DIR);
            if (localCodeDir == null) {
                LOG.debug("No local code dir set in local mode. Aborting code gen resolution.");
                abort = true;
                return;
            }
            codeDir = new File(jConf.get(PigConstants.LOCAL_CODE_DIR));
        } else {
            codeDir = Files.createTempDir();
            codeDir.deleteOnExit();
        }

        try {
            classLoader = new URLClassLoader(new URL[] { codeDir.toURI().toURL() });
        } catch (MalformedURLException e) {
            throw new RuntimeException("Unable to make URLClassLoader for tempDir: "
                    + codeDir.getAbsolutePath());
        }
        this.jConf = jConf;
        this.isLocal = isLocal;
    }

    /**
     * This method fetches the SchemaTupleFactory that can create Tuples of the given
     * Schema (ignoring aliases) and appendability. IMPORTANT: if no such SchemaTupleFactory
     * is available, this returns null.
     * @param   schema
     * @param   true if it should be appendable
     * @param   the context in which this SchemaTupleFactory is being requested
     * @return  generating SchemaTupleFactory, null otherwise
     */
    private SchemaTupleFactory internalNewSchemaTupleFactory(Schema s, boolean isAppendable, GenContext context)  {
        return newSchemaTupleFactory(Triple.make(new SchemaKey(s), isAppendable, context));
    }

    /**
     * This method fetches the SchemaTupleFactory that generates the SchemaTuple
     * registered with the given identifier. IMPORTANT: if no such SchemaTupleFactory
     * is available, this returns null.
     * @param   identifier
     * @return  generating schemaTupleFactory, null otherwise
     */
    private SchemaTupleFactory internalNewSchemaTupleFactory(int id) {
        SchemaTupleFactory stf = schemaTupleFactoriesById.get(id);
        if (stf == null) {
            LOG.debug("No SchemaTupleFactory present for given identifier: " + id);
        }
        return stf;
    }

    /**
     * This method fetches the SchemaTupleFactory that can create Tuples of the given
     * Schema and appendability. IMPORTANT: if no such SchemaTupleFactory is available,
     * this returns null.
     * @param   SchemaKey/appendability pair
     * @return  generating SchemaTupleFactory, null otherwise
     */
    private SchemaTupleFactory newSchemaTupleFactory(Triple<SchemaKey, Boolean, GenContext> trip) {
        SchemaTupleFactory stf = schemaTupleFactoriesByTriple.get(trip);
        if (stf == null) {
            LOG.debug("No SchemaTupleFactory present for given SchemaKey/Boolean/Context combination " + trip);
        }
        return stf;
    }

    /**
     * This method copies all of the generated classes from the distributed cache to a local directory,
     * and then seeks to resolve them and cache their respective SchemaTupleFactories.
     * @param   configuration
     * @param   true if the job is local
     * @throws  IOException
     */
    private void copyAndResolve() throws IOException {
        if (abort) {
            LOG.debug("Nothing to resolve on the backend.");
            return;
        }
        // Step one is to see if there are any classes in the distributed cache
        if (!jConf.getBoolean(SHOULD_USE_SCHEMA_TUPLE, SCHEMA_TUPLE_ON_BY_DEFAULT)) {
            LOG.info("Key [" + SHOULD_USE_SCHEMA_TUPLE +"] was not set... will not generate code.");
            return;
        }
        // Step two is to copy everything from the distributed cache if we are in distributed mode
        if (!isLocal) {
            copyAllFromDistributedCache();
        }
        // Step three is to see if the file needs to be resolved
        // If there is a "$" in the name, we know that it is an inner
        // class and thus doesn't need to be instantiated directly.
        for (File f : codeDir.listFiles()) {
            String name = f.getName().split("\\.")[0];
            if (!name.contains("$")) {
                filesToResolve.add(name);
                LOG.info("Added class to list of class to resolve: " + name);
            }
        }
        // Step four is to actually try and resolve the classes
        resolveClasses();
    }

    private void copyAllFromDistributedCache() throws IOException {
        String toDeserialize = jConf.get(PigConstants.GENERATED_CLASSES_KEY);
        if (toDeserialize == null) {
            LOG.info("No classes in in key [" + PigConstants.GENERATED_CLASSES_KEY + "] to copy from distributed cache.");
            return;
        }
        LOG.info("Copying files in key ["+PigConstants.GENERATED_CLASSES_KEY+"] from distributed cache: " + toDeserialize);
        for (String s : toDeserialize.split(",")) {
            LOG.info("Attempting to read file: " + s);
            // The string is the symlink into the distributed cache
            File src = new File(s);
            FileInputStream fin = new FileInputStream(src);
            FileOutputStream fos = new FileOutputStream(new File(codeDir, s));

            fin.getChannel().transferTo(0, src.length(), fos.getChannel());

            fin.close();
            fos.close();
            LOG.info("Successfully copied file to local directory.");
        }
    }

    /**
     * Once all of the files are copied from the distributed cache to the local
     * temp directory, this will attempt to resolve those files and add their information.
     */
    @SuppressWarnings("unchecked")
    private void resolveClasses() {
        for (String s : filesToResolve) {
            SchemaTupleFactory.LOG.info("Attempting to resolve class: " + s);
            // Step one is to simply attempt to get the class object from the classloader
            // that includes the generated code.
            Class<?> clazz;
            try {
                clazz = classLoader.loadClass(s);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Unable to find class: " + s, e);
            }

            // Step three is to check if the class is a SchemaTuple. If it isn't,
            // we do not attempt to resolve it, because it is support code, such
            // as anonymous classes.
            if (!SchemaTuple.class.isAssignableFrom(clazz)) {
                return;
            }

            Class<SchemaTuple<?>> stClass = (Class<SchemaTuple<?>>)clazz;

            // Step four is to actually try to create the SchemaTuple instance.
            SchemaTuple<?> st;
            try {
                st = stClass.newInstance();
            } catch (InstantiationException e) {
                throw new RuntimeException("Error instantiating file: " + s, e);
            } catch (IllegalAccessException e) {
                throw new RuntimeException("Error accessing file: " + s, e);
            }

            // Step five is to get information about the class.
            boolean isAppendable = st instanceof AppendableSchemaTuple<?>;
            int id = st.getSchemaTupleIdentifier();
            Schema schema = st.getSchema();

            SchemaTupleFactory stf = new SchemaTupleFactory(stClass, st.getQuickGenerator());

            for (GenContext context : GenContext.values()) {
                if (context != GenContext.FORCE_LOAD && !context.shouldGenerate(stClass)) {
                    SchemaTupleFactory.LOG.debug("Context ["+context+"] not present for class, skipping.");
                    continue;
                }

                // the SchemaKey (Schema sans alias) and appendability are how we will
                // uniquely identify a SchemaTupleFactory
                Triple<SchemaKey, Boolean, GenContext> trip =
                    Triple.make(new SchemaKey(schema), isAppendable, context);

                schemaTupleFactoriesByTriple.put(trip, stf);

                SchemaTupleFactory.LOG.info("Successfully resolved class for schema ["+schema+"] and appendability ["+isAppendable+"]"
                        + " in context: " + context);
            }
            schemaTupleFactoriesById.put(id, stf);
        }
    }

    public static void reset() {
        stb = null;
    }

    private static SchemaTupleBackend stb;

    public static void initialize(Configuration jConf, PigContext pigContext) throws IOException {
        initialize(jConf, pigContext, pigContext.getExecType().isLocal());
    }

    public static void initialize(Configuration jConf, PigContext pigContext, boolean isLocal) throws IOException {
        if (stb != null) {
            LOG.warn("SchemaTupleBackend has already been initialized");
        } else {
            SchemaTupleFrontend.lazyReset(pigContext);
            SchemaTupleFrontend.reset();
            SchemaTupleBackend stbInstance = new SchemaTupleBackend(jConf, isLocal);
            stbInstance.copyAndResolve();
            stb = stbInstance;
        }
    }

    public static SchemaTupleFactory newSchemaTupleFactory(Schema s, boolean isAppendable, GenContext context)  {
        if (stb == null) {
            // It is possible (though ideally should be avoided) for this to be called on the frontend if
            // the Tuple processing path of the POPlan is invoked (perhaps for optimization purposes)
            throw new RuntimeException("initialize was not called! Even when SchemaTuple feature is not set, it should be called.");
        }
        return stb.internalNewSchemaTupleFactory(s, isAppendable, context);
    }

    protected static SchemaTupleFactory newSchemaTupleFactory(int id) {
        if (stb == null) {
            // It is possible (though ideally should be avoided) for this to be called on the frontend if
            // the Tuple processing path of the POPlan is invoked (perhaps for optimization purposes)
            throw new RuntimeException("initialize was not called! Even when SchemaTuple feature is not set, it should be called.");
        }
        return stb.internalNewSchemaTupleFactory(id);
    }
}
