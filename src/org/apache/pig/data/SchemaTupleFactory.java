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

    private static SchemaTupleResolver schemaTupleResolver = new SchemaTupleResolver();

    public static SchemaTupleResolver getSchemaTupleResolver() {
        return schemaTupleResolver;
    }

    public static class SchemaTupleResolver {
        private Set<String> filesToResolve = Sets.newHashSet();

        private URLClassLoader classLoader;
        private Map<Pair<SchemaKey, Boolean>, SchemaTupleFactory> schemaTupleFactories = Maps.newHashMap();
        private Map<Integer, Pair<SchemaKey,Boolean>> identifiers = Maps.newHashMap();

        private SchemaTupleResolver() {
            File fil = SchemaTupleClassGenerator.getGenerateCodeTempDir();
            try {
                classLoader = new URLClassLoader(new URL[] { fil.toURI().toURL() });
            } catch (MalformedURLException e) {
                throw new RuntimeException("Unable to make URLClassLoader for tempDir: " + fil.getAbsolutePath());
            }
        }

        public SchemaTupleFactory newSchemaTupleFactory(Schema s, boolean isAppendable)  {
            return newSchemaTupleFactory(Pair.make(new SchemaKey(s), isAppendable));
        }

        public SchemaTupleFactory newSchemaTupleFactory(int id) {
            Pair<SchemaKey, Boolean> pr = identifiers.get(id);
            if (pr == null) {
                LOG.warn("No SchemaTuple present for given identifier: " + id);
            }
            return newSchemaTupleFactory(pr);
        }

        private SchemaTupleFactory newSchemaTupleFactory(Pair<SchemaKey, Boolean> pr) {
            SchemaTupleFactory stf = schemaTupleFactories.get(pr);
            if (stf == null) {
                LOG.warn("No SchemaTupleFactory present for given SchemaKey/Boolean combination: " + pr);
            }
            return stf;
        }

        public void copyAndResolve(Configuration conf, boolean isLocal) throws IOException {
            String shouldGenerate = conf.get(SchemaTupleClassGenerator.SHOULD_GENERATE_KEY);
            if (shouldGenerate == null || !Boolean.parseBoolean(shouldGenerate)) {
                LOG.info("Key [" + SchemaTupleClassGenerator.SHOULD_GENERATE_KEY +"] was not set... aborting generation");
                return;
            }
            if (!isLocal) {
                SchemaTupleClassGenerator.copyAllFromDistributedCache(conf);
            }
            for (File f : SchemaTupleClassGenerator.getGeneratedFiles()) {
                String name = f.getName().split("\\.")[0];
                if (!name.contains("$")) {
                    filesToResolve.add(name);
                    LOG.info("Added class to list of class to resolve: " + name);
                }
            }
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
                Class<?> clazz;
                try {
                    clazz = classLoader.loadClass(s);
                } catch (ClassNotFoundException e1) {
                    throw new RuntimeException("Unable to find class: " + s);
                }
                if (!HierarchyHelper.verifyMustOverride(clazz)) {
                    LOG.error("Class ["+clazz+"] does NOT override all @MustOverride methods!");
                }
                Object o;
                try {
                    o = clazz.newInstance();
                } catch (InstantiationException e) {
                    throw new RuntimeException("Error instantiating file: " + s, e);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException("Error accessing file: " + s, e);
                }
                if (!(o instanceof SchemaTuple<?>)) {
                    return;
                }
                SchemaTuple<?> st = (SchemaTuple<?>)o;
                boolean isAppendable = st instanceof AppendableSchemaTuple<?>;

                int id = st.getSchemaTupleIdentifier();
                Schema schema = st.getSchema();
                SchemaKey sk = new SchemaKey(schema);

                Pair<SchemaKey, Boolean> pr = Pair.make(sk, isAppendable);

                SchemaTupleFactory stf = new SchemaTupleFactory((Class<SchemaTuple<?>>)clazz, (SchemaTupleQuickGenerator<SchemaTuple<?>>)st.getQuickGenerator());

                schemaTupleFactories .put(pr, stf);
                identifiers.put(id, pr);

                LOG.info("Successfully resolved class.");
            }
        }
    }
}
