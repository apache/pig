package org.apache.pig.data;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.SecureClassLoader;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.SchemaTuple.SchemaTupleQuickGenerator;
import org.apache.pig.data.SchemaTupleClassGenerator.SchemaKey;
import org.apache.pig.data.SchemaTupleClassGenerator.SchemaTupleClassSerializer;
import org.apache.pig.data.utils.MethodHelper.NotImplemented;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

//TODO Use code generation to avoid having to do clazz.newInstance(), and to be able to directly return the proper SchemaTuple
//TODO can just have a small container class that the generated code extends that has any functionality specific to that
public class SchemaTupleFactory extends TupleFactory {
    private SchemaTupleQuickGenerator generator;
    private Class<SchemaTuple<?>> clazz;

    private static Map<Integer, SchemaTupleFactory> cachedSchemaTupleFactoriesById = Maps.newHashMap();
    private static Map<SchemaKey, SchemaTupleFactory> cachedSchemaTupleFactoriesBySchema = Maps.newHashMap();

    protected SchemaTupleFactory(Class<SchemaTuple<?>> clazz, SchemaTupleQuickGenerator generator) {
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
        SchemaTupleFactory stf = cachedSchemaTupleFactoriesBySchema.get(new SchemaKey(s));

        if (stf != null) {
            return stf;
        }

        stf = loadedSchemaTupleClassesHolder.newSchemaTupleFactory(s);

        cachedSchemaTupleFactoriesById.put(((SchemaTuple)stf.newTuple()).getSchemaTupleIdentifier(), stf);
        cachedSchemaTupleFactoriesBySchema.put(new SchemaKey(s), stf);

        return stf;
    }

    public static SchemaTupleFactory getSchemaTupleFactory(int id) {
        SchemaTupleFactory stf = cachedSchemaTupleFactoriesById.get(id);

        if (stf != null) {
            return stf;
        }

        stf = loadedSchemaTupleClassesHolder.newSchemaTupleFactory(id);

        cachedSchemaTupleFactoriesById.put(id, stf);
        cachedSchemaTupleFactoriesBySchema.put(new SchemaKey(((SchemaTuple)stf.newTuple()).getSchema()), stf);

        return stf;
    }

    private static LoadedSchemaTupleClassesHolder loadedSchemaTupleClassesHolder = new LoadedSchemaTupleClassesHolder();

    public static LoadedSchemaTupleClassesHolder getLoadedSchemaTupleClassesHolder() {
        return loadedSchemaTupleClassesHolder;
    }

    public static class LoadedSchemaTupleClassesHolder {
        private Map<Integer, SchemaTupleQuickGenerator> generators = Maps.newHashMap();
        private Map<Integer, Class<SchemaTuple<?>>> classes = Maps.newHashMap();

        private Map<SchemaKey, SchemaTupleQuickGenerator> generatorsBySchema = Maps.newHashMap();
        private Map<SchemaKey, Class<SchemaTuple<?>>> classesBySchema = Maps.newHashMap();

        private Set<String> registeredNames = Sets.newHashSet();

        protected LoadedSchemaTupleClassesHolder() {
        }

        public SchemaTupleFactory newSchemaTupleFactory(Schema s) {
            SchemaKey sk = new SchemaKey(s);
            Class<SchemaTuple<?>> clazz = classesBySchema.get(sk);
            SchemaTupleQuickGenerator stGen = generatorsBySchema.get(sk);
            if (clazz == null || stGen == null) {
                throw new RuntimeException("Could not find matching SchemaTuple for Schema: " + s); //TODO do something else? Return null? A checked exception?
            }
            return new SchemaTupleFactory(clazz, stGen);
        }

        public SchemaTupleFactory newSchemaTupleFactory(int id) {
            Class<SchemaTuple<?>> clazz = classes.get(id);
            SchemaTupleQuickGenerator stGen = generators.get(id);
            if (clazz == null || stGen == null) {
                throw new RuntimeException("Could not find matching SchemaTuple for id: " + id); //TODO do something else? Return null? A checked exception?
            }
            return new SchemaTupleFactory(clazz, stGen);
        }

        public void registerFileFromDistributedCache(String className, Configuration conf) throws IOException {
            FileSystem fs = FileSystem.get(conf);
            FSDataInputStream fsdis = fs.open(new Path(className));
            long available = 0;
            long position = 0;
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            while ((available = fsdis.available()) > 0) {
                if (available > Integer.MAX_VALUE) {
                    throw new ExecException("Class file for " + className + " has too many available bytes: " + available);
                }
                byte[] buf = new byte[(int)available];
                long read = fsdis.read(position, buf, 0, (int)available);
                if (read == -1) {
                    break;
                }
                position += read;
                baos.write(buf, 0, (int)read);
            }
            fsdis.close();
            final byte[] buf = baos.toByteArray();
            baos.close();

            registerBytes(buf, className);
        }

        public void registerBytes(final byte[] buf, String className) throws IOException {
            System.out.println("THIS IS THE CLASSNAME ARGUMENT: " + className); //remove

            //TODO if this works, can just have a static temp directory that everything
            //TODO writes to (locally), and the URLClassLoader
            ClassLoader cl = new SecureClassLoader() {
                @Override
                public Class<?> findClass(String name) {
                    return super.defineClass(name, buf, 0, buf.length);
                }
            };

            File f = new File(className + ".class"); //remove
            FileOutputStream fos = new FileOutputStream(f); //remove
            fos.write(buf, 0, buf.length); //remove
            fos.close(); //remove
            f.deleteOnExit(); //remove

            Class<SchemaTuple<?>> clazz;
            try {
                clazz = (Class<SchemaTuple<?>>) cl.loadClass(className);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Unable to load class " + className, e);
            }

            SchemaTuple<?> st;
            try {
                st = clazz.newInstance();
            } catch (InstantiationException e) {
                throw new ExecException("Unable to instantiate class " + clazz, e);
            } catch (IllegalAccessException e) {
                throw new ExecException("Not allowed to instantiate class " + clazz, e);
            }
            SchemaTupleQuickGenerator stGen = st.getQuickGenerator();
            int id = st.getSchemaTupleIdentifier();
            Schema s = st.getSchema();

            generators.put(id, stGen);
            classes.put(id, clazz);
            registeredNames.add(className);

            SchemaKey sk = new SchemaKey(s);
            generatorsBySchema.put(sk, stGen);
            classesBySchema.put(sk, clazz);
        }

        public boolean isRegistered(String className) {
            return registeredNames.contains(className);
        }

        public void registerLocalMode(SchemaTupleClassSerializer stcs) throws IOException {
            registerBytes(stcs.getBytes(), stcs.getName());
        }
    }
}
