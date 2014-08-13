/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.backend.hadoop.accumulo;

import com.google.common.base.Preconditions;

/**
 * Extracts necessary information from a user provide column "specification":
 * colf[[*]:[colq[*]]]
 * 
 * Removes any trailing asterisk on colfam or colqual, and appropriately sets
 * the {#link Column.Type}
 */
public class Column {

    public static enum Type {
        LITERAL, COLFAM_PREFIX, COLQUAL_PREFIX,
    }

    private final Type columnType;
    private String columnFamily;
    private String columnQualifier;

    public Column(String col) {
        Preconditions.checkNotNull(col);

        int index = col.indexOf(AbstractAccumuloStorage.COLON);
        if (-1 == index) {
            columnFamily = col;
            columnQualifier = null;

            if (columnFamily.endsWith(AbstractAccumuloStorage.ASTERISK)) {
                columnFamily = columnFamily.substring(0,
                        columnFamily.length() - 1);
                columnType = Type.COLFAM_PREFIX;
            } else {
                columnType = Type.LITERAL;
            }
        } else {
            if (1 == col.length()) {
                throw new IllegalArgumentException("Cannot parse '" + col + "'");
            }

            columnFamily = col.substring(0, index);
            columnQualifier = col.substring(index + 1);

            // TODO Handle colf*:colq* ?
            if (columnFamily.endsWith(AbstractAccumuloStorage.ASTERISK)) {
                columnType = Type.COLFAM_PREFIX;
                columnFamily = columnFamily.substring(0,
                        columnFamily.length() - 1);
            } else if (columnQualifier.isEmpty()) {
                columnType = Type.COLQUAL_PREFIX;
            } else if (columnQualifier
                    .endsWith(AbstractAccumuloStorage.ASTERISK)) {
                columnType = Type.COLQUAL_PREFIX;
                columnQualifier = columnQualifier.substring(0,
                        columnQualifier.length() - 1);
            } else {
                columnType = Type.LITERAL;
            }
        }
    }

    public Type getType() {
        return columnType;
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public String getColumnQualifier() {
        return columnQualifier;
    }

    public boolean matchAll() {
        return Type.COLFAM_PREFIX.equals(columnType) && "".equals(columnFamily);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof Column) {
            Column other = (Column) o;

            if (null != columnFamily) {
                if (null == other.columnFamily) {
                    return false;
                } else if (!columnFamily.equals(other.columnFamily)) {
                    return false;
                }
            }

            if (null != columnQualifier) {
                if (null == other.columnQualifier) {
                    return false;
                } else if (!columnQualifier.equals(other.columnQualifier)) {
                    return false;
                }
            }

            return columnType == other.columnType;
        }

        return false;
    }

    @Override
    public String toString() {
        return columnType + " " + columnFamily + ":" + columnQualifier;
    }
}
