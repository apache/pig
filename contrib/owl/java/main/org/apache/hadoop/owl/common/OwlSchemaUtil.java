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
package org.apache.hadoop.owl.common;

import org.apache.hadoop.owl.protocol.ColumnType;
import org.apache.hadoop.owl.protocol.OwlColumnSchema;
import org.apache.hadoop.owl.protocol.OwlSchema;

/** Utility functions for working with OwlSchema objects */
public class OwlSchemaUtil {

    /**
     * Gets the string representation for the OwlSchema.
     * @param schema the schema
     * @return the schema string
     * @throws OwlException the owl exception
     */
    public static String getSchemaString(OwlSchema schema) throws OwlException {
        StringBuilder sb = new StringBuilder();

        if( schema == null ) {
            return null;
        }

        getSchemaString(sb, schema, ColumnType.COLLECTION, true);
        return sb.toString();
    }

    /**
     * Gets the string representation for the OwlSchema.
     * @param sb the string buffer to append to
     * @param schema the schema
     * @param type the type
     * @param top if this the top level schema
     * @return the schema string
     * @throws OwlException the owl exception
     */
    private static void getSchemaString(StringBuilder sb, OwlSchema schema, ColumnType type, boolean top) throws OwlException {

        if( type == ColumnType.RECORD || type == ColumnType.COLLECTION ) {
            if( schema == null ) {
                throw new OwlException(ErrorType.ERROR_MISSING_SUBSCHEMA);
            }
        }

        if( schema == null ) {
            return;
        }

        if (!top) {
            if (type == ColumnType.RECORD) {
                sb.append("(");
            }
            else if (type == ColumnType.COLLECTION) {
                sb.append("(");
            }
            else if (type == ColumnType.MAP) {
                sb.append("(");
            }
        }

        boolean isFirst = true;

        int count = schema.getColumnCount();
        for (int i = 0; i < count; i++) {

            if (!isFirst) {
                sb.append(",");
            }
            else {
                isFirst = false;
            }

            OwlColumnSchema fs = schema.columnAt(i);

            if (fs == null) {
                continue;
            }

            if (fs.getName() != null && !fs.getName().equals("")) {
                sb.append(fs.getName());
                sb.append(":");
            }

            sb.append(ColumnType.findTypeName(fs.getType()));
            if ((fs.getType() == ColumnType.RECORD) || (fs.getType() == ColumnType.MAP)
                    || (fs.getType() == ColumnType.COLLECTION)) {
                // safety net
                if (schema != fs.getSchema()) {
                    getSchemaString(sb, fs.getSchema(), fs.getType(), false);
                }
                else {
                    throw new OwlException(ErrorType.INVALID_OWL_SCHEMA, "Schema refers to itself as inner schema");
                }
            }
        }

        if (!top) {
            if (type == ColumnType.RECORD) {
                sb.append(")");
            }
            else if (type == ColumnType.COLLECTION) {
                sb.append(")");
            }
            else if (type == ColumnType.MAP) {
                sb.append(")");
            }
        }
    }
}
