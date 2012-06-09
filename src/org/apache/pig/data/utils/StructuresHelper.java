package org.apache.pig.data.utils;

import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class StructuresHelper {

    /**
     * This encapsulates a Schema and allows it to be used in such a way that
     * any aliases are ignored in equality.
     */
    public static class SchemaKey {
        private Schema s;

        public SchemaKey(Schema s) {
            this.s = s;
        }

        private static int[] primeList = { 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37,
                                           41, 43, 47, 53, 59, 61, 67, 71, 73, 79,
                                           83, 89, 97, 101, 103, 107, 109, 1133};

        /**
         * The hashcode logic is taken from the Schema class, including how fields
         * are handled. The difference is that aliases are ignored.
         */
        @Override
        public int hashCode() {
            if (s == null) {
                return 0;
            }
            int idx = 0 ;
            int hashCode = 0 ;
            for(FieldSchema fs : s.getFields()) {
                hashCode += hashCode(fs) * (primeList[idx % primeList.length]) ;
                idx++ ;
            }
            return hashCode ;
        }

        private int hashCode(FieldSchema fs) {
            return (fs.type * 17) + ( (fs.schema == null? 0 : fs.schema.hashCode()) * 23 );
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof SchemaKey)) {
                return false;
            }
            Schema other = ((SchemaKey)o).get();
            return (s == null && other == null) || Schema.equals(s, other, false, true);
        }

        public Schema get() {
            return s;
        }
    }

    /**
     * This is a helper class which makes it easy to have pairs of values,
     * and to use them as keys and values in Maps.
     */
    public static class Pair<T1, T2> {
        private T1 t1;
        private T2 t2;

        public Pair(T1 t1, T2 t2) {
            this.t1 = t1;
            this.t2 = t2;
        }

        public T1 getFirst() {
            return t1;
        }

        public T2 getSecond() {
            return t2;
        }

        public static <A,B> Pair<A,B> make(A t1, B t2) {
            return new Pair<A,B>(t1, t2);
        }

        @Override
        public int hashCode() {
            return (t1 == null ? 0 : t1.hashCode()) + (t2 == null ? 0 : 31 * t2.hashCode());
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Pair<?,?>)) {
                return false;
            }
            Pair<?,?> pr = (Pair<?,?>)o;
            return (t1 == null ? pr.getFirst() == null : t1.equals(pr.getFirst())) && (t2 == null ? pr.getSecond() == null : t2.equals(pr.getSecond()));
        }

        @Override
        public String toString() {
            return new StringBuilder().append("[").append(t1).append(",").append(t2).append("]").toString();
        }
    }

}
