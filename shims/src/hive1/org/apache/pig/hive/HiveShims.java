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
import java.sql.Date;
import java.sql.Timestamp;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcFile.Version;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.udf.generic.Collector;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.SimpleGenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.shims.HadoopShimsSecure;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapreduce.Job;

import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;

import org.joda.time.DateTime;

public class HiveShims {
    public static String normalizeOrcVersionName(String version) {
        return Version.byName(version).getName();
    }

    public static void addLessThanOpToBuilder(SearchArgument.Builder builder,
            String columnName, PredicateLeaf.Type columnType, Object value) {
        builder.lessThan(columnName, value);
    }

    public static void addLessThanEqualsOpToBuilder(SearchArgument.Builder builder,
            String columnName, PredicateLeaf.Type columnType, Object value) {
        builder.lessThanEquals(columnName, value);
    }

    public static void addEqualsOpToBuilder(SearchArgument.Builder builder,
            String columnName, PredicateLeaf.Type columnType, Object value) {
        builder.equals(columnName, value);
    }

    public static void addBetweenOpToBuilder(SearchArgument.Builder builder,
            String columnName, PredicateLeaf.Type columnType, Object low, Object high) {
        builder.between(columnName, low, high);
    }

    public static void addIsNullOpToBuilder(SearchArgument.Builder builder,
            String columnName, PredicateLeaf.Type columnType) {
        builder.isNull(columnName);
    }

    public static Class[] getOrcDependentClasses(Class hadoopVersionShimsClass) {
        return new Class[]{OrcFile.class, HiveConf.class, AbstractSerDe.class,
                org.apache.hadoop.hive.shims.HadoopShims.class, HadoopShimsSecure.class, hadoopVersionShimsClass,
                Input.class};
    }

    public static Class[] getHiveUDFDependentClasses(Class hadoopVersionShimsClass) {
        return new Class[]{GenericUDF.class,
                PrimitiveObjectInspector.class, HiveConf.class, Serializer.class, ShimLoader.class,
                hadoopVersionShimsClass, HadoopShimsSecure.class, Collector.class};
    }

    public static Object getSearchArgObjValue(Object value) {
        if (value instanceof BigInteger) {
            return new BigDecimal((BigInteger) value);
        } else if (value instanceof DateTime) {
            return new Timestamp(((DateTime) value).getMillis());
        } else {
            return value;
        }
    }

    public static void setOrcConfigOnJob(Job job, Long stripeSize, Integer rowIndexStride, Integer bufferSize, Boolean blockPadding, CompressionKind compress, String versionName) {
        if (stripeSize != null) {
            job.getConfiguration().setLong(HiveConf.ConfVars.HIVE_ORC_DEFAULT_STRIPE_SIZE.varname, stripeSize);
        }
        if (rowIndexStride != null) {
            job.getConfiguration().setInt(HiveConf.ConfVars.HIVE_ORC_DEFAULT_ROW_INDEX_STRIDE.varname, rowIndexStride);
        }
        if (bufferSize != null) {
            job.getConfiguration().setInt(HiveConf.ConfVars.HIVE_ORC_DEFAULT_BUFFER_SIZE.varname, bufferSize);
        }
        if (blockPadding != null) {
            job.getConfiguration().setBoolean(HiveConf.ConfVars.HIVE_ORC_DEFAULT_BLOCK_PADDING.varname, blockPadding);
        }
        if (compress != null) {
            job.getConfiguration().set(HiveConf.ConfVars.HIVE_ORC_DEFAULT_COMPRESS.varname, compress.toString());
        }
        if (versionName != null) {
            job.getConfiguration().set(HiveConf.ConfVars.HIVE_ORC_WRITE_FORMAT.varname, versionName);
        }
    }

    public static class PigJodaTimeStampObjectInspector extends
            AbstractPrimitiveJavaObjectInspector implements TimestampObjectInspector {

        public PigJodaTimeStampObjectInspector() {
            super(TypeInfoFactory.timestampTypeInfo);
        }

        @Override
        public TimestampWritable getPrimitiveWritableObject(Object o) {
            return o == null ? null : new TimestampWritable(new Timestamp(((DateTime) o).getMillis()));
        }

        @Override
        public Timestamp getPrimitiveJavaObject(Object o) {
            return o == null ? null : new Timestamp(((DateTime) o).getMillis());
        }
    }

    public static GenericUDAFParameterInfo newSimpleGenericUDAFParameterInfo(ObjectInspector[] arguments,
            boolean distinct, boolean allColumns) {
        return new SimpleGenericUDAFParameterInfo(arguments, distinct, allColumns);
    }

    public static class DateShim {

        public static Date cast(Object ts) {
            return (Date) ts;
        }

        public static long millisFromDate(Object ts) {
            return cast(ts).getTime();
        }
    }

    public static class TimestampShim {

        public static Timestamp cast(Object ts) {
            return (Timestamp) ts;
        }

        public static long millisFromTimestamp(Object ts) {
            return cast(ts).getTime();
        }
    }

    public static class TimestampWritableShim {

        public static boolean isAssignableFrom(Object object) {
            return object instanceof TimestampWritable;
        }

        public static TimestampWritable cast(Object ts) {
            return (TimestampWritable) ts;
        }

        public static long millisFromTimestampWritable(Object ts) {
            return cast(ts).getTimestamp().getTime();
        }
    }
}