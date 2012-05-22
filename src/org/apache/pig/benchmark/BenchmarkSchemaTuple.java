package org.apache.pig.benchmark;

import java.io.File;
import java.io.FileOutputStream;
import java.io.DataOutputStream;
import java.io.DataOutput;

import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.BinInterSedes;
import org.apache.pig.data.TypeAwareTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.data.SchemaTupleClassGenerator;
import org.apache.pig.data.SchemaTuple;
import org.apache.pig.data.SchemaTupleFactory;
import org.apache.pig.backend.executionengine.ExecException;

import com.google.caliper.SimpleBenchmark;
import com.google.caliper.Param;

public class BenchmarkSchemaTuple {
    public static void main(String[] args) {
        serializationSizeBenchmark();
    }

    public static void serializationSizeBenchmark() {
        int maxSize = 100;
        System.out.println("Results for type: int");
        for (int i = 0; i < maxSize; i++) {
            makeSerializationResult(i, DataType.INTEGER);
        }
        System.out.println("Results for type: long");
        for (int i = 0; i < maxSize; i++) {
            makeSerializationResult(i, DataType.LONG);
        }
    }

    public static void makeSerializationResult(int size, byte type) {
        int id = SchemaTupleClassGenerator.generateAndAddToJar(makeSchema(size,type));
        TupleFactory stf = TupleFactory.getInstanceForSchemaId(id);
        TupleFactory tf = TupleFactory.getInstance();
        SchemaTuple st = (SchemaTuple)stf.newTuple();
        Tuple t = tf.newTuple(size);
        Tuple t2 = tf.newTuple();
        byte[] types = new byte[size];
        for (int i = 0; i < size; i++) {
            types[i] = type;
        }
        Tuple pt = tf.newTupleForSchema(types);
        resetmarks();
        try {
            for (int i = 0; i < size; i++) {
                st.set(i, makeData(type));
                t.set(i, makeData(type));
                t2.append(makeData(type));
                pt.set(i, makeData(type));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        // Tuple, PrimitiveTuple, SchemaTuple
        System.out.print(serializeAndGetSize(t) + "\t");
        System.out.print(serializeAndGetSize(pt) + "\t");
        System.out.println(serializeAndGetSize(st));

    }

    private static final BinInterSedes bis = new BinInterSedes();

    public static long serializeAndGetSize(Object o) {
        try {
            File fo = File.createTempFile("testing","testing");
            DataOutput fos = new DataOutputStream(new FileOutputStream(fo));
            bis.writeDatum(fos, o);
            long length = fo.length();
            fo.delete();
            return length;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void memoryBenchmark() {
        int maxSize = 100;
        System.out.println("Results for type: int");
        for (int i = 0; i < maxSize; i++) {
            makeMemoryResult(i, DataType.INTEGER);
        }
        System.out.println("Results for type: long");
        for (int i = 0; i < maxSize; i++) {
            makeMemoryResult(i, DataType.LONG);
        }
    }

    public static void makeMemoryResult(int size, byte type) {
        int id = SchemaTupleClassGenerator.generateAndAddToJar(makeSchema(size,type));
        TupleFactory stf = TupleFactory.getInstanceForSchemaId(id);
        TupleFactory tf = TupleFactory.getInstance();
        SchemaTuple st = (SchemaTuple)stf.newTuple();
        Tuple t = tf.newTuple(size);
        Tuple t2 = tf.newTuple();
        byte[] types = new byte[size];
        for (int i = 0; i < size; i++) {
            types[i] = type;
        }
        Tuple pt = tf.newTupleForSchema(types);
        resetmarks();
        try {
            for (int i = 0; i < size; i++) {
                st.set(i, makeData(type));
                t.set(i, makeData(type));
                t2.append(makeData(type));
                pt.set(i, makeData(type));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        long stSize = ObjectSizeCalculator.getObjectSize(st);
        long tSize = ObjectSizeCalculator.getObjectSize(t);
        long t2Size = ObjectSizeCalculator.getObjectSize(t2);
        long ptSize = ObjectSizeCalculator.getObjectSize(pt);
        //System.out.println("elements, SchemaTuple, Tuple set, Tuple append, primitiveTuple");
        System.out.println(size + "\t" + stSize + "\t" + tSize + "\t" + t2Size + "\t" + ptSize);
    }

    public static long longmark = 0;
    public static int intmark = 0;

    public static void resetmarks() {
        longmark = 0;
        intmark = 0;
    }

    public static Object makeData(byte type) {
        switch (type) {
        case(DataType.LONG): return new Long(longmark++);
        case(DataType.INTEGER): return new Integer(intmark++);
        }
        throw new RuntimeException("Unsupported type: " + type);
    }

    public static Schema makeSchema(int size, byte type) {
        Schema s = new Schema();
        for (int i = 0; i < size; i++) {
            s.add(new Schema.FieldSchema(null, type));
        }
        try {
            return s;
            //return new Schema(new Schema.FieldSchema(null, s, DataType.TUPLE));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static class CaliperBenchmark extends SimpleBenchmark {
        @Param int size;
        private TupleFactory stf;
        private TupleFactory stf2;
        private TupleFactory tf = TupleFactory.getInstance();
        private byte[] types;
        private byte[] typesint;

        @Override protected void setUp() {
            int id = SchemaTupleClassGenerator.generateAndAddToJar(makeSchema(size,DataType.LONG));
            stf = TupleFactory.getInstanceForSchemaId(id);
            id = SchemaTupleClassGenerator.generateAndAddToJar(makeSchema(size,DataType.INTEGER));
            stf2 = TupleFactory.getInstanceForSchemaId(id);
            types = new byte[size];
            typesint = new byte[size];
            for (int i = 0; i < size; i++) {
                types[i] = DataType.LONG;
                typesint[i] = DataType.INTEGER;
            }
        }
/*
        public int timeTupleMake(int reps) throws ExecException {
            int c = 17;
            for (int i = 0; i < reps; i++) {
                Tuple t = tf.newTuple(size);
                c += 31 * t.hashCode();
            }
            return c;
        }

        public int timeTupleMakeAndSet(int reps) throws ExecException {
            int c = 17;
            for (long i = 0; i < reps; i++) {
                Tuple t = tf.newTuple(size);
                for (int j = 0; j < size; j++) {
                    t.set(j, new Long(i));
                }
                c += 31 * t.hashCode();
            }
            return c;
        }

        public int timeTupleSet(int reps) throws ExecException {
            int c = 17;
            Tuple t = tf.newTuple(size);
            for (long i = 0; i < reps; i++) {
                for (int j = 0; j < size; j++) {
                    t.set(j, new Long(i));
                }
                c += 31 * t.hashCode();
            }
            return c;
        }

        public int timePrimitiveTupleMake(int reps) throws ExecException {
            int c = 17;
            for (int i = 0; i < reps; i++) {
                Tuple t = tf.newTupleForSchema(types);
                c += 31 * t.hashCode();
            }
            return c;
        }

        public int timePrimitiveTupleMakeAndSet(int reps) throws ExecException {
            int c = 17;
            for (long i = 0; i < reps; i++) {
                Tuple t = tf.newTupleForSchema(types);
                for (int j = 0; j < size; j++) {
                    t.set(j, new Long(i));
                }
                c += 31 * t.hashCode();
            }
            return c;
        }

        public int timePrimitiveTupleMakeAndSetLong(int reps) throws ExecException {
            int c = 17;
            for (long i = 0; i < reps; i++) {
                TypeAwareTuple t = (TypeAwareTuple)tf.newTupleForSchema(types);
                for (int j = 0; j < size; j++) {
                    t.setLong(j, i);
                }
                c += 31 * t.hashCode();
            }
            return c;
        }

        public int timePrimitiveTupleSet(int reps) throws ExecException {
            int c = 17;
            Tuple t = tf.newTupleForSchema(types);
            for (long i = 0; i < reps; i++) {
                for (int j = 0; j < size; j++) {
                    t.set(j, new Long(i));
                }
                c += 31 * t.hashCode();
            }
            return c;
        }

        public int timePrimitiveTupleSetLong(int reps) throws ExecException {
            int c = 17;
            TypeAwareTuple t = (TypeAwareTuple)tf.newTupleForSchema(types);
            for (long i = 0; i < reps; i++) {
                for (int j = 0; j < size; j++) {
                    t.setLong(j, i);
                }
                c += 31 * t.hashCode();
            }
            return c;
        }

        public int timeSchemaTupleMake(int reps) throws ExecException {
            int c = 17;
            for (int i = 0; i < reps; i++) {
                Tuple t = stf.newTuple();
                c += 31 * t.hashCode();
            }
            return c;
        }

        public int timeSchemaTupleMakeAndSet(int reps) throws ExecException {
            int c = 17;
            for (long i = 0; i < reps; i++) {
                Tuple t = stf.newTuple();
                for (int j = 0; j < size; j++) {
                    t.set(j, new Long(i));
                }
                c += 31 * t.hashCode();
            }
            return c;
        }

        public int timeSchemaTupleMakeAndSetLong(int reps) throws ExecException {
            int c = 17;
            for (long i = 0; i < reps; i++) {
                TypeAwareTuple t = (TypeAwareTuple)stf.newTuple();
                for (int j = 0; j < size; j++) {
                    t.setLong(j, i);
                }
                c += 31 * t.hashCode();
            }
            return c;
        }

        public int timeSchemaTupleSet(int reps) throws ExecException {
            int c = 17;
            Tuple t = stf.newTuple();
            for (long i = 0; i < reps; i++) {
                for (int j = 0; j < size; j++) {
                    t.set(j, new Long(i));
                }
                c += 31 * t.hashCode();
            }
            return c;
        }

        public int timeSchemaTupleSetLong(int reps) throws ExecException {
            int c = 17;
            TypeAwareTuple t = (TypeAwareTuple)stf.newTuple();
            for (long i = 0; i < reps; i++) {
                for (int j = 0; j < size; j++) {
                    t.setLong(j, i);
                }
                c += 31 * t.hashCode();
            }
            return c;
        }

        public void timeTupleSerializeLong(int reps) throws Exception {
            File fo = File.createTempFile("testing","testing");
            DataOutput fos = new DataOutputStream(new FileOutputStream(fo));
            Tuple t = tf.newTuple(size);
            for (int i = 0; i < size; i++) {
                 t.set(i, new Long((long)i));
            }
            for (int i = 0; i < reps; i++) {
                bis.writeDatum(fos, t);
            }
            fo.delete();
        }

        public void timePrimitiveTupleSerializeLong(int reps) throws Exception {
            File fo = File.createTempFile("testing","testing");
            DataOutput fos = new DataOutputStream(new FileOutputStream(fo));
            Tuple t = tf.newTupleForSchema(types);
            for (int i = 0; i < size; i++) {
                 t.set(i, new Long((long)i));
            }
            for (int i = 0; i < reps; i++) {
                bis.writeDatum(fos, t);
            }
            fo.delete();
        }

        public void timeSchemaTupleSerializeLong(int reps) throws Exception {
            File fo = File.createTempFile("testing","testing");
            DataOutput fos = new DataOutputStream(new FileOutputStream(fo));
            Tuple t = stf.newTuple();
            for (int i = 0; i < size; i++) {
                 t.set(i, new Long((long)i));
            }
            for (int i = 0; i < reps; i++) {
                bis.writeDatum(fos, t);
            }
            fo.delete();
        }
*/
        public void timeTupleSerializeInt(int reps) throws Exception {
            File fo = File.createTempFile("testing","testing");
            DataOutput fos = new DataOutputStream(new FileOutputStream(fo));
            Tuple t = tf.newTuple(size);
            for (int i = 0; i < size; i++) {
                 t.set(i, new Integer(i));
            }
            for (int i = 0; i < reps; i++) {
                bis.writeDatum(fos, t);
            }
            fo.delete();
        }

        public void timePrimitiveTupleSerializeInt(int reps) throws Exception {
            File fo = File.createTempFile("testing","testing");
            DataOutput fos = new DataOutputStream(new FileOutputStream(fo));
            Tuple t = tf.newTupleForSchema(typesint);
            for (int i = 0; i < size; i++) {
                 t.set(i, new Integer(i));
            }
            for (int i = 0; i < reps; i++) {
                bis.writeDatum(fos, t);
            }
            fo.delete();
        }

        public void timeSchemaTupleSerializeInt(int reps) throws Exception {
            File fo = File.createTempFile("testing","testing");
            DataOutput fos = new DataOutputStream(new FileOutputStream(fo));
            Tuple t = stf2.newTuple();
            for (int i = 0; i < size; i++) {
                 t.set(i, new Integer(i));
            }
            for (int i = 0; i < reps; i++) {
                bis.writeDatum(fos, t);
            }
            fo.delete();
        }
    }
}
