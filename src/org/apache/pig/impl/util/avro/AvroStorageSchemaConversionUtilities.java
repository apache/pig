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

package org.apache.pig.impl.util.avro;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.pig.LoadPushDown.RequiredField;
import org.apache.pig.ResourceSchema;
import org.apache.pig.LoadPushDown.RequiredFieldList;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.mortbay.log.Log;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Static methods for converting from Avro Schema object to Pig Schema objects,
 * and vice versa.
 */
public class AvroStorageSchemaConversionUtilities {

  /**
   * Determines the pig object type of the Avro schema.
   * @param s The avro schema for which to determine the type
   * @return the byte representing the schema type
   * @throws ExecException
   * @see org.apache.avro.Schema.Type
   */
  public static byte getPigType(final Schema s) throws ExecException {
    switch (s.getType()) {
    case ARRAY:
      return DataType.BAG;
    case BOOLEAN:
      return DataType.BOOLEAN;
    case BYTES:
      return DataType.BYTEARRAY;
    case DOUBLE:
      return DataType.DOUBLE;
    case ENUM:
      return DataType.CHARARRAY;
    case FIXED:
      return DataType.BYTEARRAY;
    case FLOAT:
      return DataType.FLOAT;
    case INT:
      return DataType.INTEGER;
    case LONG:
      return DataType.LONG;
    case MAP:
      return DataType.MAP;
    case NULL:
      return DataType.NULL;
    case RECORD:
      return DataType.TUPLE;
    case STRING:
      return DataType.CHARARRAY;
    case UNION:
      List<Schema> types = s.getTypes();
      if (types.size() == 1) {
        return getPigType(types.get(0));
      } else if (types.size() == 2 && types.get(0).getType() == Type.NULL) {
          return getPigType(types.get(1));
      } else if (types.size() == 2 && types.get(1).getType() == Type.NULL) {
          return getPigType(types.get(0));
      } else if (isUnionOfSimpleTypes(s)) {
          return DataType.BYTEARRAY;
      }
      throw new ExecException(
          "Currently only supports element unions of a type and null (" + s.toString() +")");
    default:
      throw new ExecException("Unknown type: " + s.getType().toString());
    }
  }

  public static boolean isUnionOfSimpleTypes(Schema s) {
      List<Schema> types = s.getTypes();
      if (types == null) {
          return false;
      }
      for (Schema subSchema : types) {
          switch (subSchema.getType()) {
          case BOOLEAN: 
          case BYTES: 
          case DOUBLE:
          case ENUM:
          case FIXED:
          case FLOAT:
          case INT:
          case LONG:
          case NULL:
          case STRING:
              continue;
          default:    
              return false;
          }
      }
      return true;
  }
  
  /**
   * Translates an Avro schema to a Resource Schema (for Pig).
   * @param s The avro schema for which to determine the type
   * @param allowRecursiveSchema Flag indicating whether to
   * throw an error if a recursive schema definition is found
   * @throws IOException
   * @return the corresponding pig schema
   */
  public static ResourceSchema avroSchemaToResourceSchema(
      final Schema s, final Boolean allowRecursiveSchema)
      throws IOException {
    return avroSchemaToResourceSchema(s, Sets.<Schema> newHashSet(),
        Maps.<String, ResourceSchema> newHashMap(),
        allowRecursiveSchema);
  }

  /**
   * Translates a field schema (avro) to a resource field schema (pig).
   * @param f
   *          The avro schema field for which to determine the type
   * @param namesInStack
   *          Set of already defined object names
   * @param alreadyDefinedSchemas
   *          Map of schema names to resourceschema objects
   * @param allowRecursiveSchema
   *          controls whether to throw an erro if the schema is recursive
   * @throws IOException
   * @return the corresponding pig resource schema field
   */
  private static ResourceSchema.ResourceFieldSchema fieldToResourceFieldSchema(
      final Field f, final Set<Schema> schemasInStack,
      final Map<String, ResourceSchema> alreadyDefinedSchemas,
      final Boolean allowRecursiveSchema) throws IOException {

    ResourceSchema.ResourceFieldSchema rf =
        new ResourceSchema.ResourceFieldSchema();
    rf.setName(f.name());
    Schema fieldSchema = f.schema();
    if (isNullableUnion(fieldSchema)) {
      fieldSchema = removeSimpleUnion(fieldSchema);
    }
    // if this is a fixed schema, save the schema into the description to
    // use later when writing out the field
    if (fieldSchema.getType() == Type.FIXED) {
      rf.setDescription(fieldSchema.toString());
    } else {
      rf.setDescription(f.doc());
    }
    byte pigType = getPigType(fieldSchema);
    rf.setType(pigType);
    switch (pigType) {
    case DataType.BAG: {
      ResourceSchema bagSchema = new ResourceSchema();
      ResourceSchema.ResourceFieldSchema[] bagSchemaFields
        = new ResourceSchema.ResourceFieldSchema[1];
      bagSchemaFields[0] = new ResourceSchema.ResourceFieldSchema();
      bagSchemaFields[0].setType(DataType.TUPLE);
      bagSchemaFields[0].setDescription(fieldSchema.getDoc());
      ResourceSchema innerResourceSchema = null;
      Schema elementSchema = fieldSchema.getElementType();
      if (isNullableUnion(elementSchema)) {
        elementSchema = removeSimpleUnion(elementSchema);
      }
      switch (elementSchema.getType()) {
      case RECORD:
      case MAP:
      case ARRAY:
        innerResourceSchema = avroSchemaToResourceSchema(elementSchema,
            schemasInStack, alreadyDefinedSchemas, allowRecursiveSchema);
        bagSchemaFields[0].setName(elementSchema.getName());
        break;
      case UNION:
        throw new IOException(
            "Pig cannot translate avro schemas for complex unions");
      default:
        innerResourceSchema = new ResourceSchema();
        ResourceSchema.ResourceFieldSchema[] tupleSchemaFields =
            new ResourceSchema.ResourceFieldSchema[1];
        tupleSchemaFields[0] = new ResourceSchema.ResourceFieldSchema();
        tupleSchemaFields[0].setType(getPigType(elementSchema));
        innerResourceSchema.setFields(tupleSchemaFields);
      }

      bagSchemaFields[0].setSchema(innerResourceSchema);
      bagSchema.setFields(bagSchemaFields);
      rf.setSchema(bagSchema);
    }
      break;
    case DataType.MAP: {
      Schema mapAvroSchema = fieldSchema.getValueType();
      if (isNullableUnion(mapAvroSchema)) {
        mapAvroSchema = removeSimpleUnion(mapAvroSchema);
      }
      ResourceSchema mapSchema = new ResourceSchema();
      ResourceSchema.ResourceFieldSchema[] mapSchemaFields =
          new ResourceSchema.ResourceFieldSchema[1];
      if (mapAvroSchema.getType() == Type.RECORD) {
        ResourceSchema innerResourceSchema =
            avroSchemaToResourceSchema(fieldSchema.getValueType(), schemasInStack,
            alreadyDefinedSchemas, allowRecursiveSchema);
        mapSchemaFields[0] = new ResourceSchema.ResourceFieldSchema();
        mapSchemaFields[0].setType(DataType.TUPLE);
        mapSchemaFields[0].setName(mapAvroSchema.getName());
        mapSchemaFields[0].setSchema(innerResourceSchema);
        mapSchemaFields[0].setDescription(fieldSchema.getDoc());
      } else {
        mapSchemaFields[0] = new ResourceSchema.ResourceFieldSchema();
        mapSchemaFields[0].setType(getPigType(mapAvroSchema));
      }
      mapSchema.setFields(mapSchemaFields);
      rf.setSchema(mapSchema);
    }
      break;
    case DataType.TUPLE:
      if (alreadyDefinedSchemas.containsKey(fieldSchema.getFullName())) {
        rf.setSchema(alreadyDefinedSchemas.get(fieldSchema.getFullName()));
      } else {
        ResourceSchema innerResourceSchema =
            avroSchemaToResourceSchema(fieldSchema, schemasInStack,
            alreadyDefinedSchemas, allowRecursiveSchema);
        rf.setSchema(innerResourceSchema);
        alreadyDefinedSchemas.put(
            fieldSchema.getFullName(), innerResourceSchema);
      }
      break;
    }

    return rf;
  }

  /**
   * Translates an Avro schema to a Resource Schema (for Pig). Internal method.
   * @param s The avro schema for which to determine the type
   * @param namesInStack Set of already defined object names
   * @param alreadyDefinedSchemas Map of schema names to resourceschema objects
   * @param allowRecursiveSchema controls whether to throw an error
   * if the schema is recursive
   * @throws IOException
   * @return the corresponding pig schema
   */
  private static ResourceSchema avroSchemaToResourceSchema(final Schema s,
      final Set<Schema> schemasInStack,
      final Map<String, ResourceSchema> alreadyDefinedSchemas,
      final Boolean allowRecursiveSchema) throws IOException {

    ResourceSchema.ResourceFieldSchema[] resourceFields = null;
    switch (s.getType()) {
    case RECORD:
      if (schemasInStack.contains(s)) {
        if (allowRecursiveSchema) {
          break; // don't define further fields in the schema
        } else {
          throw new IOException(
              "Pig found recursive schema definition while processing"
              + s.toString() + " encountered " + s.getFullName()
              + " which was already seen in this stack: "
              + schemasInStack.toString() + "\n");
        }
      }
      schemasInStack.add(s);
      resourceFields =
            new ResourceSchema.ResourceFieldSchema[s.getFields().size()];
      for (Field f : s.getFields()) {
        resourceFields[f.pos()] = fieldToResourceFieldSchema(f,
            schemasInStack, alreadyDefinedSchemas, allowRecursiveSchema);
      }
      schemasInStack.remove(s);
      break;
    default:
      // wrap in a tuple
      resourceFields = new ResourceSchema.ResourceFieldSchema[1];
      Field f = new Schema.Field(s.getName(), s, s.getDoc(), null);
      resourceFields[0] = fieldToResourceFieldSchema(f,
          schemasInStack, alreadyDefinedSchemas, allowRecursiveSchema);
    }
    ResourceSchema rs = new ResourceSchema();
    rs.setFields(resourceFields);

    return rs;
  }
  
  /**
   * Translated a ResourceSchema to an Avro Schema.
   * @param rs Input schema.
   * @param recordName Record name
   * @param recordNameSpace Namespace
   * @param definedRecordNames Map of already defined record names
   * to schema objects
   * @return the translated schema
   * @throws IOException
   */
  public static Schema resourceSchemaToAvroSchema(final ResourceSchema rs,
      String recordName, final String recordNameSpace,
      final Map<String, List<Schema>> definedRecordNames,
      final Boolean doubleColonsToDoubleUnderscores) throws IOException {

    if (rs == null) {
      return null;
    }

    recordName = toAvroName(recordName, doubleColonsToDoubleUnderscores);

    List<Schema.Field> fields = new ArrayList<Schema.Field>();
    Schema newSchema = Schema.createRecord(
        recordName, null, recordNameSpace, false);
    if (rs.getFields() != null) {
      Integer i = 0;
      for (ResourceSchema.ResourceFieldSchema rfs : rs.getFields()) {
        String rfsName = toAvroName(rfs.getName(),
            doubleColonsToDoubleUnderscores);
        Schema fieldSchema = resourceFieldSchemaToAvroSchema(
            rfsName, recordNameSpace, rfs.getType(),
            rfs.getDescription().equals("autogenerated from Pig Field Schema")
              ? null : rfs.getDescription(),
            rfs.getSchema(), definedRecordNames,
            doubleColonsToDoubleUnderscores);
        fields.add(new Schema.Field((rfsName != null)
              ? rfsName : recordName + "_" + i.toString(),
            fieldSchema,
            rfs.getDescription().equals(
                "autogenerated from Pig Field Schema")
                ? null : rfs.getDescription(), null));
        i++;

      }
      newSchema.setFields(fields);
    }

    return newSchema;
  }

  /**
   * Translates a name in a pig schema to an acceptable Avro name, or
   * throws an error if the name can't be translated.
   * @param name The variable name to translate.
   * @param doubleColonsToDoubleUnderscores Indicates whether to translate
   * double colons to underscores or throw an error if they are encountered.
   * @return A name usable by Avro.
   * @throws IOException If the name is not compatible with Avro.
   */
  private static String toAvroName(String name,
      final Boolean doubleColonsToDoubleUnderscores) throws IOException {

    if (name == null) {
      return null;
    }

    if (doubleColonsToDoubleUnderscores) {
      name = name.replace("::", "__");
    }

    if (name.matches("[A-Za-z_][A-Za-z0-9_]*")) {
      return name;
    } else {
      throw new IOException(
          "Pig Schema contains a name that is not allowed in Avro");
    }
  }

  /**
   * Returns a Union Schema composed of {@in} and null.
   * @param in the schema to combine with null
   * @return the new union schema
   */
  private static Schema createNullableUnion(final Schema in) {
    return Schema.createUnion(Lists.newArrayList(Schema.create(Type.NULL), in));
  }

  /**
   * Returns a Union Schema composed of {@in} and null.
   * @param t the avro raw type to combine with null
   * @return the new union schema
   */
  private static Schema createNullableUnion(final Type t) {
    return createNullableUnion(Schema.create(t));
  }

  /**
   * Creates a new Avro schema object from the input ResouceSchema.
   * @param name object name
   * @param nameSpace namespace for avro object
   * @param type type of avro object
   * @param description description for new avro object
   * @param schema pig schema for the object
   * @param definedRecordNames already defined record names
   * @param doubleColonsToDoubleUnderscores whether to translate double
   *   colons in Pig  names to underscores in avro names.
   * @return the avro schema object
   * @throws IOException If there is a translation error.
   */
  private static Schema resourceFieldSchemaToAvroSchema(
      final String name, final String nameSpace,
      final byte type, final String description,
      final ResourceSchema schema,
      final Map<String, List<Schema>> definedRecordNames,
      final Boolean doubleColonsToDoubleUnderscores)
          throws IOException {

    switch (type) {
    case DataType.BAG:
      Schema innerBagSchema = resourceSchemaToAvroSchema(
          schema.getFields()[0].getSchema(), name, null,
          definedRecordNames,
          doubleColonsToDoubleUnderscores);
      if (innerBagSchema == null) {
        throw new IOException("AvroStorage can't save bags with untyped values; please specify a value type or a schema.");
      }
      return createNullableUnion(Schema.createArray(innerBagSchema));
    case DataType.BIGCHARARRAY:
      return createNullableUnion(Type.STRING);
    case DataType.BOOLEAN:
      return createNullableUnion(Type.BOOLEAN);
    case DataType.BYTEARRAY:
      Schema fixedSchema;
      try {
        fixedSchema = (new Schema.Parser()).parse(description);
      } catch (Exception e) {
        fixedSchema = null;
      }
      if (fixedSchema == null) {
        return createNullableUnion(Type.BYTES);
      } else {
        return createNullableUnion(fixedSchema);
      }
    case DataType.CHARARRAY:
      return createNullableUnion(Type.STRING);
    case DataType.DATETIME:
        return createNullableUnion(Type.LONG);
    case DataType.DOUBLE:
      return createNullableUnion(Type.DOUBLE);
    case DataType.FLOAT:
      return createNullableUnion(Type.FLOAT);
    case DataType.INTEGER:
      return createNullableUnion(Type.INT);
    case DataType.LONG:
      return createNullableUnion(Type.LONG);
    case DataType.MAP:
      if (schema == null) {
        throw new IOException("AvroStorage can't save maps with untyped values; please specify a value type or a schema.");
      }
      byte innerType = schema.getFields()[0].getType();
      String desc = schema.getFields()[0].getDescription();
      if (desc != null) {
        if (desc.equals("autogenerated from Pig Field Schema")) {
          desc = null;
        }
      }
      Schema innerSchema;
      if (DataType.isComplex(innerType)) {
        innerSchema = createNullableUnion(
            Schema.createMap(resourceSchemaToAvroSchema(
                schema.getFields()[0].getSchema(),
                name, nameSpace, definedRecordNames,
                doubleColonsToDoubleUnderscores)));
      } else {
        innerSchema = createNullableUnion(
            Schema.createMap(resourceFieldSchemaToAvroSchema(
                name, nameSpace, innerType,
                desc, null, definedRecordNames,
                doubleColonsToDoubleUnderscores)));
      }
      return innerSchema;
    case DataType.NULL:
      return Schema.create(Type.NULL);
    case DataType.TUPLE:
      if (schema == null) {
        throw new IOException("AvroStorage can't save tuples with untyped values; please specify a value type or a schema.");
      }
      Schema returnSchema = createNullableUnion(
          resourceSchemaToAvroSchema(schema, name, null,
              definedRecordNames, doubleColonsToDoubleUnderscores));
      if (definedRecordNames.containsKey(name)) {
        List<Schema> schemaList = definedRecordNames.get(name);
        boolean notfound = true;
        for (Schema cachedSchema : schemaList) {
          if (returnSchema.equals(cachedSchema)) {
            notfound = false;
          }
          break;
        }
        if (notfound) {
          returnSchema = createNullableUnion(resourceSchemaToAvroSchema(
              schema, name + "_" + new Integer(schemaList.size()).toString(),
              null, definedRecordNames, doubleColonsToDoubleUnderscores));
          definedRecordNames.get(name).add(returnSchema);
        }
      } else {
        definedRecordNames.put(name, Lists.newArrayList(returnSchema));
      }
      return returnSchema;
    case DataType.BYTE:
    case DataType.ERROR:
    case DataType.GENERIC_WRITABLECOMPARABLE:
    case DataType.INTERNALMAP:
    case DataType.UNKNOWN:
    default:
      throw new IOException(
          "Don't know how to encode type "
              + DataType.findTypeName(type) + " in schema "
              + ((schema == null) ? "" : schema.toString()) 
              + "\n");
    }
  }

  /**
   * Checks to see if an avro schema is a combination of
   * null and another object.
   * @param s The object to check
   * @return whether it's a nullable union
   */
  public static boolean isNullableUnion(final Schema s) {
    return (
        s.getType() == Type.UNION
        && ((s.getTypes().size() == 1)
            || (s.getTypes().size() == 2
             && (s.getTypes().get(0).getType() == Type.NULL
                || s.getTypes().get(1).getType() == Type.NULL))));
  }

  /**
   * Given an input schema that is a union of an avro schema
   * and null (or just a union with one type), return the avro schema.
   * @param s The input schema object
   * @return The non-null part of the union
   */
  public static Schema removeSimpleUnion(final Schema s) {
    if (s.getType() == Type.UNION) {
      List<Schema> types = s.getTypes();
      for (Schema t : types) {
        if (t.getType() != Type.NULL) {
          return t;
        }
      }
    }
    return s;
  }

  /**
   * Takes an Avro Schema and a Pig RequiredFieldList and returns a new schema
   * with only the requried fields, or no if the function can't extract only
   * those fields. Useful for push down projections.
   * @param oldSchema The avro schema from which to extract the schema
   * @param rfl the Pig required field list
   * @return the new schema, or null
   */
  public static Schema newSchemaFromRequiredFieldList(
      final Schema oldSchema, final RequiredFieldList rfl) {
    return newSchemaFromRequiredFieldList(oldSchema, rfl.getFields());
  }

  /**
   * Takes an Avro Schema and a Pig RequiredFieldList and returns a new schema
   * with only the required fields, or no if the function can't extract only
   * those fields. Useful for push down projections.
   * @param oldSchema The avro schema from which to extract the schema
   * @param rfl List of required fields
   * @return the new schema
   */
  public static Schema newSchemaFromRequiredFieldList(
      final Schema oldSchema, final List<RequiredField> rfl) {
    List<Schema.Field> fields = Lists.newArrayList();
    for (RequiredField rf : rfl) {
      try {
        Schema.Field f = oldSchema.getField(rf.getAlias());
        if (f == null) {
          return null;
        }
        try {
          if (getPigType(f.schema()) != rf.getType()) {
            return null;
          }
        } catch (ExecException e) {
          Log.warn("ExecException caught in newSchemaFromRequiredFieldList", e);
          return null;
        }
        if (rf.getSubFields() == null) {
          fields.add(
              new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultValue()));
        } else {
          Schema innerSchema =
              newSchemaFromRequiredFieldList(f.schema(), rf.getSubFields());
          if (innerSchema == null) {
            return null;
          } else {
            fields.add(
                new Schema.Field(
                    f.name(), innerSchema, f.doc(), f.defaultValue()));
          }
        }
      }
      catch (AvroRuntimeException e){
        return oldSchema;
      }
    }

    Schema newSchema = Schema.createRecord(
        oldSchema.getName(),
        "subset of fields from " + oldSchema.getName()
            + "; " + oldSchema.getDoc(),
        oldSchema.getNamespace(), false);

    newSchema.setFields(fields);
    return newSchema;
  }

}
