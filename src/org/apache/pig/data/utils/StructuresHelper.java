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
package org.apache.pig.data.utils;

import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class StructuresHelper {
    private StructuresHelper() {
    }

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
            return hashCode(s);
        }

        public static int hashCode(Schema s) {
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

        private static int hashCode(FieldSchema fs) {
            return (fs.type * 17) + ( (fs.schema == null? 0 : hashCode(fs.schema)) * 23 );
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

        public String toString() {
            return s.toString();
        }
    }

    /**
     * This is a helper class which makes it easy to have pairs of values,
     * and to use them as keys and values in Maps.
     */
    public static class Pair<T1, T2> {
        private final T1 t1;
        private final T2 t2;

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
            if (t1 == null) {
                return pr.getFirst() == null;
            }
            if (!t1.equals(pr.getFirst())) {
                return false;
            }
            if (t2 == null) {
                return pr.getSecond() == null;
            }
            return t2.equals(pr.getSecond());
        }

        @Override
        public String toString() {
            return new StringBuilder()
                    .append("[")
                    .append(t1)
                    .append(",")
                    .append(t2)
                    .append("]")
                    .toString();
        }
    }

    public static class Triple<T1, T2, T3> {
        private final T1 t1;
        private final T2 t2;
        private final T3 t3;

        public Triple(T1 t1, T2 t2, T3 t3) {
            this.t1 = t1;
            this.t2 = t2;
            this.t3 = t3;
        }

        public T1 getFirst() {
            return t1;
        }

        public T2 getSecond() {
            return t2;
        }

        public T3 getThird() {
            return t3;
        }

        public static <A,B,C> Triple<A,B,C> make(A t1, B t2, C t3) {
            return new Triple<A,B,C>(t1, t2, t3);
        }

        @Override
        public int hashCode() {
            return (t1 == null ? 0 : t1.hashCode())
                 + (t2 == null ? 0 : 31 * t2.hashCode())
                 + (t3 == null ? 0 : 527 * t3.hashCode());
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Triple<?,?,?>)) {
                return false;
            }
            Triple<?,?,?> tr = (Triple<?,?,?>)o;
            if (t1 == null) {
                return tr.getFirst() == null;
            }
            if (!t1.equals(tr.getFirst())) {
                return false;
            }
            if (t2 == null) {
                return tr.getSecond() == null;
            }
            if (!t2.equals(tr.getSecond())) {
                return false;
            }
            if (t3 == null) {
                return tr.getThird() ==null;
            }
            if (!t3.equals(tr.getThird())) {
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return new StringBuilder()
                    .append("[")
                    .append(t1)
                    .append(",")
                    .append(t2)
                    .append(",")
                    .append(t3)
                    .append("]")
                    .toString();
        }
    }
}
