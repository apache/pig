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

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.data.SchemaTuple.SchemaTupleQuickGenerator;
import org.apache.pig.data.utils.HierarchyHelper;
import org.apache.pig.data.utils.MethodHelper;
import org.apache.pig.data.utils.MethodHelper.NotImplemented;
import org.apache.pig.data.utils.StructuresHelper.Pair;
import org.apache.pig.data.utils.StructuresHelper.SchemaKey;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
/**
 * This is an implementation of TupleFactory that will instantiate
 * SchemaTuple's. This class has nothing to do with the actual generation
 * of code, and instead simply encapsulates the classes which allow
 * for efficiently creating SchemaTuples.
 */
public class SchemaTupleFactory extends TupleFactory {
    private static final Log LOG = LogFactory.getLog(SchemaTupleFactory.class);

    private SchemaTupleQuickGenerator<? extends SchemaTuple<?>> generator;
    private Class<SchemaTuple<?>> clazz;

    private SchemaTupleFactory(Class<SchemaTuple<?>> clazz,
            SchemaTupleQuickGenerator<? extends SchemaTuple<?>> generator) {
        this.clazz = clazz;
        this.generator = generator;
    }

    /**
     * This method inspects a Schema to see whether or
     * not a SchemaTuple implementation can be generated
     * for the types present. Currently, bags and maps
     * are not supported.
     * @param   schema
     * @return  true if it is generatable
     */
    public static boolean isGeneratable(Schema s) {
        if (s == null) {
            return false;
        }

        for (Schema.FieldSchema fs : s.getFields()) {
            if (fs.type == DataType.BAG || fs.type == DataType.MAP) {
                return false;
            }

            if (fs.type == DataType.TUPLE && !isGeneratable(fs.schema)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public Tuple newTuple() {
        return generator.make();
    }

    /**
     * The notion of instantiating a SchemaTuple with a given
     * size doesn't really make sense, as the size is set
     * by the Schema.
     */
    @Override
    @NotImplemented
    public Tuple newTuple(int size) {
        throw MethodHelper.methodNotImplemented();
    }

    /**
     * As with newTuple(int), it doesn't make much sense
     * to instantiate a Tuple with a notion of Schema from
     * an untyped list of objects. Note: in the future
     * we may inspect the type of the Objects in the list
     * and see if they match with the underlying Schema, and if so,
     * generate the Tuple. For now the gain seems minimal.
     */
    @Override
    @NotImplemented
    public Tuple newTuple(List c) {
        throw MethodHelper.methodNotImplemented();
    }

    @Override
    @NotImplemented
    public Tuple newTupleNoCopy(List c) {
        throw MethodHelper.methodNotImplemented();
    }

    /**
     * It does not make any sense to instantiate with
     * one object a Tuple whose size and type is already known.
     */
    @Override
    @NotImplemented
    public Tuple newTuple(Object datum) {
        throw MethodHelper.methodNotImplemented();
    }

    @Override
    public Class<SchemaTuple<?>> tupleClass() {
        return clazz;
    }

    private static SchemaTupleResolver schemaTupleResolver =
        new SchemaTupleResolver(SchemaTupleClassGenerator.getGenerateCodeTempDir());

    /**
     * This method returns the SchemaTupleResolver associated with the generated
     * code temporary directory.
     * @return  SchemaTupleResolver
     */
    protected static SchemaTupleResolver getSchemaTupleResolver() {
        return schemaTupleResolver;
    }

    /**
     * This class encapsulates the logic around resolving the generated class
     * files.
     */
    protected static class SchemaTupleResolver {
        private Set<String> filesToResolve = Sets.newHashSet();

        /**
         * We use the URLClassLoader to resolve the generated classes because we can
         * simply give it the directory we put all of the compiled class files into,
         * and it will handle the dynamic loading.
         */
        private URLClassLoader classLoader;
        private Map<Pair<SchemaKey, Boolean>, SchemaTupleFactory> schemaTupleFactoriesByPair = Maps.newHashMap();
        private Map<Integer, SchemaTupleFactory> schemaTupleFactoriesById = Maps.newHashMap();

        /**
         * The only information this class needs is a directory of generated code to resolve
         * classes in.
         * @param directory of generated code
         */
        private SchemaTupleResolver(File codeDir) {
            try {
                classLoader = new URLClassLoader(new URL[] { codeDir.toURI().toURL() });
            } catch (MalformedURLException e) {
                throw new RuntimeException("Unable to make URLClassLoader for tempDir: "
                        + codeDir.getAbsolutePath());
            }
        }

        /**
         * This method fetches the SchemaTupleFactory that can create Tuples of the given
         * Schema (ignoring aliases) and appendability. IMPORTANT: if no such SchemaTupleFactory
         * is available, this returns null.
         * @param   schema
         * @param   true if it should be appendable
         * @return  generating SchemaTupleFactory, null otherwise
         */
        public SchemaTupleFactory newSchemaTupleFactory(Schema s, boolean isAppendable)  {
            return newSchemaTupleFactory(Pair.make(new SchemaKey(s), isAppendable));
        }

        /**
         * This method fetches the SchemaTupleFactory that generates the SchemaTuple
         * registered with the given identifier. IMPORTANT: if no such SchemaTupleFactory
         * is available, this returns null.
         * @param   identifier
         * @return  generating schemaTupleFactory, null otherwise
         */
        protected SchemaTupleFactory newSchemaTupleFactory(int id) {
            SchemaTupleFactory stf = schemaTupleFactoriesById.get(id);
            if (stf == null) {
                LOG.warn("No SchemaTupleFactory present for given identifier: " + id);
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
        private SchemaTupleFactory newSchemaTupleFactory(Pair<SchemaKey, Boolean> pr) {
            SchemaTupleFactory stf = schemaTupleFactoriesByPair.get(pr);
            if (stf == null) {
                LOG.warn("No SchemaTupleFactory present for given SchemaKey/Boolean combination: " + pr);
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
        public void copyAndResolve(Configuration conf, boolean isLocal) throws IOException {
            // Step one is to see if there are any classes in the distributed cache
            String shouldGenerate = conf.get(SchemaTupleClassGenerator.SHOULD_GENERATE_KEY);
            if (shouldGenerate == null || !Boolean.parseBoolean(shouldGenerate)) {
                LOG.info("Key [" + SchemaTupleClassGenerator.SHOULD_GENERATE_KEY +"] was not set... aborting generation");
                return;
            }
            // Step two is to copy everything from the distributed cache if we are in distributed mode
            if (!isLocal) {
                SchemaTupleClassGenerator.copyAllFromDistributedCache(conf);
            }
            // Step three is to see if the file needs to be resolved
            // If there is a "$" in the name, we know that it is an inner
            // class and thus doesn't need to be instantiated directly.
            for (File f : SchemaTupleClassGenerator.getGeneratedFiles()) {
                String name = f.getName().split("\\.")[0];
                if (!name.contains("$")) {
                    filesToResolve.add(name);
                    LOG.info("Added class to list of class to resolve: " + name);
                }
            }
            // Step four is to actually try and resolve the classes
            resolveClasses();
        }

        /**
         * Once all of the files are copied from the distributed cache to the local
         * temp directory, this will attempt to resolve those files and add their information.
         */
        @SuppressWarnings("unchecked")
        public void resolveClasses() {
            for (String s : filesToResolve) {
                LOG.info("Attempting to resolve class: " + s);
                // Step one is to simply attempt to get the class object from the classloader
                // that includes the generated code.
                Class<?> clazz;
                try {
                    clazz = classLoader.loadClass(s);
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException("Unable to find class: " + s, e);
                }
                // Step two is to ensure that the generated class doesn't have any improperly
                // implemented methods.
                if (!HierarchyHelper.verifyMustOverride(clazz)) {
                    LOG.error("Class ["+clazz+"] does NOT override all @MustOverride methods!");
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

                // the SchemaKey (Schema sans alias) and appendability are how we will
                // uniquely identify a SchemaTupleFactory
                Pair<SchemaKey, Boolean> pr = Pair.make(new SchemaKey(schema), isAppendable);

                schemaTupleFactoriesByPair .put(pr, stf);
                schemaTupleFactoriesById.put(id, stf);

                LOG.info("Successfully resolved class.");
            }
        }
    }

    // This method proxies to the SchemaTupleResolver
    public static void copyAndResolve(Configuration jConf, boolean isLocal) throws IOException {
        schemaTupleResolver.copyAndResolve(jConf, isLocal);
    }
}
