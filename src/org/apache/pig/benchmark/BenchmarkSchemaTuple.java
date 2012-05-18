package org.apache.pig.benchmark;

import org.apache.pig.data.TupleFactory;
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
        memoryBenchmark();
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
        //System.out.println("size, SchemaTuple, Tuple set, Tuple append");
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
        private TupleFactory tf = TupleFactory.getInstance();
        private byte[] types;

        @Override protected void setUp() {
            int id = SchemaTupleClassGenerator.generateAndAddToJar(makeSchema(size,DataType.LONG));
            stf = TupleFactory.getInstanceForSchemaId(id);
            types = new byte[size];
            for (int i = 0; i < size; i++) {
                types[i] = DataType.LONG;
            }
        }

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
    }
}
