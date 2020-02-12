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
package org.apache.pig.hive;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.udf.generic.Collector;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.SimpleGenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.shims.HadoopShimsSecure;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapreduce.Job;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile.Version;

import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;

import org.joda.time.DateTime;


public class HiveShims {
    public static String normalizeOrcVersionName(String version) {
        return Version.byName(version).getName();
    }

    public static void addLessThanOpToBuilder(SearchArgument.Builder builder,
            String columnName, PredicateLeaf.Type columnType, Object value) {
        builder.lessThan(columnName, columnType, value);
    }

    public static void addLessThanEqualsOpToBuilder(SearchArgument.Builder builder,
            String columnName, PredicateLeaf.Type columnType, Object value) {
        builder.lessThanEquals(columnName, columnType, value);
    }

    public static void addEqualsOpToBuilder(SearchArgument.Builder builder,
            String columnName, PredicateLeaf.Type columnType, Object value) {
        builder.equals(columnName, columnType, value);
    }

    public static void addBetweenOpToBuilder(SearchArgument.Builder builder,
            String columnName, PredicateLeaf.Type columnType, Object low, Object high) {
        builder.between(columnName, columnType, low, high);
    }

    public static void addIsNullOpToBuilder(SearchArgument.Builder builder,
            String columnName, PredicateLeaf.Type columnType) {
        builder.isNull(columnName, columnType);
    }

    public static Class[] getOrcDependentClasses(Class hadoopVersionShimsClass) {
        return new Class[]{OrcFile.class, HiveConf.class, AbstractSerDe.class,
                org.apache.hadoop.hive.shims.HadoopShims.class, HadoopShimsSecure.class, DateWritable.class,
                hadoopVersionShimsClass, Input.class, org.apache.orc.OrcFile.class,
                com.esotericsoftware.minlog.Log.class};
    }

    public static Class[] getHiveUDFDependentClasses(Class hadoopVersionShimsClass) {
        return new Class[]{GenericUDF.class,
                PrimitiveObjectInspector.class, HiveConf.class, Serializer.class, ShimLoader.class,
                hadoopVersionShimsClass, HadoopShimsSecure.class, Collector.class, HiveDecimalWritable.class};
    }

    public static Object getSearchArgObjValue(Object value) {
        if (value instanceof Integer) {
            return new Long((Integer) value);
        } else if (value instanceof Float) {
            return new Double((Float) value);
        } else if (value instanceof BigInteger) {
            return new HiveDecimalWritable(HiveDecimal.create((BigInteger) value));
        } else if (value instanceof BigDecimal) {
            return new HiveDecimalWritable(HiveDecimal.create((BigDecimal) value));
        } else if (value instanceof DateTime) {
            return new java.sql.Date(((DateTime) value).getMillis());
        } else {
            return value;
        }
    }

    public static void setOrcConfigOnJob(Job job, Long stripeSize, Integer rowIndexStride, Integer bufferSize,
            Boolean blockPadding, CompressionKind compress, String versionName) {
        if (stripeSize != null) {
            job.getConfiguration().setLong(OrcConf.STRIPE_SIZE.getAttribute(), stripeSize);
        }
        if (rowIndexStride != null) {
            job.getConfiguration().setInt(OrcConf.ROW_INDEX_STRIDE.getAttribute(), rowIndexStride);
        }
        if (bufferSize != null) {
            job.getConfiguration().setInt(OrcConf.BUFFER_SIZE.getAttribute(), bufferSize);
        }
        if (blockPadding != null) {
            job.getConfiguration().setBoolean(OrcConf.BLOCK_PADDING.getAttribute(), blockPadding);
        }
        if (compress != null) {
            job.getConfiguration().set(OrcConf.COMPRESS.getAttribute(), compress.toString());
        }
        if (versionName != null) {
            job.getConfiguration().set(OrcConf.WRITE_FORMAT.getAttribute(), versionName);
        }
    }

    public static class PigJodaTimeStampObjectInspector extends
            AbstractPrimitiveJavaObjectInspector implements TimestampObjectInspector {

        public PigJodaTimeStampObjectInspector() {
            super(TypeInfoFactory.timestampTypeInfo);
        }

        private static Timestamp getHiveTimeStampFromDateTime(Object o) {
            if (o == null) {
                return null;
            }
            Timestamp ts = new Timestamp();
            ts.setTimeInMillis(((DateTime) o).getMillis());
            return ts;
        }

        @Override
        public TimestampWritableV2 getPrimitiveWritableObject(Object o) {
            return o == null ? null : new TimestampWritableV2(getHiveTimeStampFromDateTime(o));
        }

        @Override
        public Timestamp getPrimitiveJavaObject(Object o) {
            return o == null ? null : new Timestamp(getHiveTimeStampFromDateTime(o));
        }
    }

    public static GenericUDAFParameterInfo newSimpleGenericUDAFParameterInfo(ObjectInspector[] arguments,
            boolean distinct, boolean allColumns) {
        return new SimpleGenericUDAFParameterInfo(arguments, false, distinct, allColumns);
    }

    public static class TimestampShim {

        public static Timestamp cast(Object ts) {
            return (Timestamp) ts;
        }

        public static long millisFromTimestamp(Object ts) {
            return cast(ts).toEpochMilli();
        }
    }

    public static class TimestampWritableShim {

        public static boolean isAssignableFrom(Object object) {
            return object instanceof TimestampWritableV2;
        }

        public static TimestampWritableV2 cast(Object ts) {
            return (TimestampWritableV2) ts;
        }

        public static long millisFromTimestampWritable(Object ts) {
            return cast(ts).getTimestamp().toEpochMilli();
        }
    }

}