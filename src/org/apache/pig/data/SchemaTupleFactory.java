package org.apache.pig.data;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.SchemaTuple.SchemaTupleQuickGenerator;
import org.apache.pig.data.SchemaTupleClassGenerator.SchemaKey;
import org.apache.pig.data.utils.MethodHelper.NotImplemented;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

//TODO Use code generation to avoid having to do clazz.newInstance(), and to be able to directly return the proper SchemaTuple
//TODO can just have a small container class that the generated code extends that has any functionality specific to that
public class SchemaTupleFactory extends TupleFactory {
    private SchemaTupleQuickGenerator<? extends SchemaTuple<?>> generator;
    private Class<SchemaTuple<?>> clazz;

    private static Map<Integer, SchemaTupleFactory> cachedSchemaTupleFactoriesById = Maps.newHashMap();
    private static Map<SchemaKey, SchemaTupleFactory> cachedSchemaTupleFactoriesBySchema = Maps.newHashMap();

    protected SchemaTupleFactory(Class<SchemaTuple<?>> clazz, SchemaTupleQuickGenerator<? extends SchemaTuple<?>> generator) {
        this.clazz = clazz;
        this.generator = generator;
    }

    public static boolean isGeneratable(Schema s) {
        if (s == null)
            return false;

        for (Schema.FieldSchema fs : s.getFields()) {
            if (fs.type == DataType.BAG || fs.type == DataType.MAP)
                return false;

            if (fs.type == DataType.TUPLE && !isGeneratable(fs.schema))
                return false;
        }

        return true;
    }

    @Override
    public Tuple newTuple() {
        return generator.make();
    }

    public static RuntimeException methodNotImplemented() {
        StackTraceElement[] ste = Thread.currentThread().getStackTrace();
        StackTraceElement pre = ste[ste.length - 2];
        return new RuntimeException(pre.getMethodName() + " not implemented in " + pre.getClassName());
    }

    @Override
    @NotImplemented
    public Tuple newTuple(int size) {
        throw methodNotImplemented();
    }

    @Override
    @NotImplemented
    public Tuple newTuple(List c) {
        throw methodNotImplemented();
    }

    @Override
    @NotImplemented
    public Tuple newTupleNoCopy(List c) {
        throw methodNotImplemented();
    }

    @Override
    @NotImplemented
    public Tuple newTuple(Object datum) {
        throw methodNotImplemented();
    }

    @Override
    public Class<SchemaTuple<?>> tupleClass() {
        return clazz;
    }

    public static SchemaTupleFactory getSchemaTupleFactory(Schema s) {
        SchemaKey sk = new SchemaKey(s);
        SchemaTupleFactory stf = cachedSchemaTupleFactoriesBySchema.get(sk);

        if (stf != null) {
            return stf;
        }

        try {
            stf = loadedSchemaTupleClassesHolder.newSchemaTupleFactory(s);
        } catch (ExecException e) {
            throw new RuntimeException("Error making new SchemaTupleFactory by schema: " + s, e);
        }

        cachedSchemaTupleFactoriesById.put(((SchemaTuple<?>)stf.newTuple()).getSchemaTupleIdentifier(), stf);
        cachedSchemaTupleFactoriesBySchema.put(sk, stf);

        return stf;
    }

    public static SchemaTupleFactory getSchemaTupleFactory(int id) {
        SchemaTupleFactory stf = cachedSchemaTupleFactoriesById.get(id);

        if (stf != null) {
            return stf;
        }

        try {
            stf = loadedSchemaTupleClassesHolder.newSchemaTupleFactory(id);
        } catch (ExecException e) {
            throw new RuntimeException("Error making new SchemaTupleFactory by id: " + id, e);
        }

        cachedSchemaTupleFactoriesById.put(id, stf);
        cachedSchemaTupleFactoriesBySchema.put(new SchemaKey(((SchemaTuple<?>)stf.newTuple()).getSchema()), stf);

        return stf;
    }

    private static LoadedSchemaTupleClassesHolder loadedSchemaTupleClassesHolder = new LoadedSchemaTupleClassesHolder();

    public static LoadedSchemaTupleClassesHolder getLoadedSchemaTupleClassesHolder() {
        return loadedSchemaTupleClassesHolder;
    }

    public static class LoadedSchemaTupleClassesHolder {
        private Map<Integer, SchemaTupleQuickGenerator<? extends SchemaTuple<?>>> generators = Maps.newHashMap();
        private Map<Integer, Class<SchemaTuple<?>>> classes = Maps.newHashMap();

        private Map<SchemaKey, Integer> lookup = Maps.newHashMap();
        private List<String> filesToResolve = Lists.newArrayList();

        private LoadedSchemaTupleClassesHolder() {
        }

        public SchemaTupleFactory newSchemaTupleFactory(Schema s) throws ExecException {
            SchemaKey sk = new SchemaKey(s);
            Integer id = lookup.get(sk);
            if (id != null) {
                return newSchemaTupleFactory(id);
            }

            throw new ExecException("No mapping present for given Schema: " + s);
        }

        public SchemaTupleFactory newSchemaTupleFactory(int id) throws ExecException {
            Class<SchemaTuple<?>> clazz = classes.get(id);
            SchemaTupleQuickGenerator<? extends SchemaTuple<?>> stGen = generators.get(id);
            if (clazz == null || stGen == null) {
                throw new ExecException("Could not find matching SchemaTuple for id: " + id); //TODO do something else? Return null? A checked exception?
            }
            return new SchemaTupleFactory(clazz, stGen);
        }

        public void copyAllFromDistributedCache(Configuration conf) throws IOException {
            String toDeserialize = conf.get(SchemaTupleClassGenerator.GENERATED_CLASSES_KEY);
            if (toDeserialize == null) {
                return;
            }
            FileSystem fs = FileSystem.get(conf);
            for (String s : toDeserialize.split("\\.")) {
                copyFromDistributedCache(s, conf, fs);
            }
        }

        public void copyFromDistributedCache(String className, Configuration conf, FileSystem fs) throws IOException {
            Path src = new Path(className + ".class");
            filesToResolve.add(className);
            Path dst = new Path(SchemaTupleClassGenerator.tempFile(className).getAbsolutePath());
            fs.copyToLocalFile(src, dst);
        }

        /**
         * Once all of the files are copied from the distributed cache to the local
         * temp directory, this will attempt to resolve those files and add their information.
         */
        public void resolveClasses() {
            for (String s : filesToResolve) {
                Class<?> clazz = SchemaTupleClassGenerator.getGeneratedClass(s);
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
                int id = st.getSchemaTupleIdentifier();
                Schema schema = st.getSchema();

                classes.put(id, (Class<SchemaTuple<?>>)clazz);
                generators.put(id, (SchemaTupleQuickGenerator<SchemaTuple<?>>)st.getQuickGenerator());
                lookup.put(new SchemaKey(schema), id);
            }
        }

        public boolean isRegistered(String className) {
            return SchemaTupleClassGenerator.tempFile(className).exists();
        }
    }
}
