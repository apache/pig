/**
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

package org.apache.hadoop.zebra.schema;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.zebra.tfile.Utils;
import org.apache.pig.data.DataType;

/**
 * Zebra Column Type
 */
public enum ColumnType {
  /**
   * Any type
   */
  ANY("any") {
	  public byte pigDataType() {
		  return DataType.BYTEARRAY;
	  }
  },
  /**
   * Integer
   */
  INT("int") {
    public byte pigDataType() {
      return DataType.INTEGER;
    }
  },
  /**
   * Long
   */
  LONG("long") {
    public byte pigDataType() {
      return DataType.LONG;
    }
  },
  /**
   * Float
   */
  FLOAT("float") {
    public byte pigDataType() {
      return DataType.FLOAT;
    }
  },
  /**
   * Double
   */
  DOUBLE("double") {
    public byte pigDataType() {
      return DataType.DOUBLE;
    }
  },
  /**
   * Boolean
   */
  BOOL("bool") {
    public byte pigDataType() {
      return DataType.BOOLEAN;
    }
  },
  /**
   * Collection
   */
  COLLECTION("collection") {
    public byte pigDataType() {
      return DataType.BAG;
    }
  },
  /**
   * Map
   */
  MAP("map") {
    public byte pigDataType() {
      return DataType.MAP;
    }
  },
  /**
   * Record
   */
  RECORD("record") {
    public byte pigDataType() {
      return DataType.TUPLE;
    }
  },
  /**
   * String
   */
  STRING("string") {
    public byte pigDataType() {
      return DataType.CHARARRAY;
    }
  },
  /**
   * Bytes
   */
  BYTES("bytes") {
    public byte pigDataType() {
      return DataType.BYTEARRAY;
    }
  };

  private String name;

  private ColumnType(String name) {
    this.name = name;
  }

  public abstract byte pigDataType();

  /**
   * To get the type based on the type name string.
   * 
   * @param name name of the type
   * 
   * @return ColumnType Enum for the type
   */
  public static ColumnType getTypeByName(String name) {
    return ColumnType.valueOf(name.toUpperCase());
  }

  /**
   * Get the Zebra type from a Pig type
   *
   * @param dt Pig type
   *
   * @return Zebra type
   */
  public static ColumnType getTypeByPigDataType(byte dt) {
    for (ColumnType ct : ColumnType.values()) {
      if (ct.pigDataType() == dt) {
        return ct;
      }
    }
    return null;
  }

  /**
   * Get the Zebra type name for the passed in Zebra column type
   *
   * @param columntype Zebra column type
   *
   * @return string representation of the Zebra column type
   */
  public static String findTypeName(ColumnType columntype) {
	  return columntype.getName();
  }

  /**
   * Get the Zebra type name for this Zebra column type
   *
   * @return string representation of the Zebra column type
   */
  public String getName() {
    return name;
  }

  /**
   * Same as getType
   */
  public String toString() {
    return name;
  }

  /**
   * check if a column type contains internal schema
   *
   * @param columnType Zebra column type
   *
   * @return true if the type is RECORD, MAP or COLLECTION
   */
  public static boolean isSchemaType(ColumnType columnType) {
	  return ((columnType == RECORD) || (columnType == MAP) || (columnType == COLLECTION));
  }
}
