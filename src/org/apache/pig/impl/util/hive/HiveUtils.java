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
package org.apache.pig.impl.util.hive;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantFloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantLongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantFloatObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.pig.PigWarning;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.tools.pigstats.PigStatusReporter;
import org.joda.time.DateTime;

public class HiveUtils {

    static TupleFactory tf = TupleFactory.getInstance();

    public static Object convertHiveToPig(Object obj, ObjectInspector oi, boolean[] includedColumns) {
        Object result = null;
        if (obj == null) {
            return result;
        }
        switch (oi.getCategory()) {
        case PRIMITIVE:
            PrimitiveObjectInspector poi = (PrimitiveObjectInspector)oi;
            result = getPrimaryFromHive(obj, poi);
            break;
        case STRUCT:
            StructObjectInspector soi = (StructObjectInspector)oi;
            List<StructField> elementFields = (List<StructField>) soi.getAllStructFieldRefs();
            List<Object> items = soi.getStructFieldsDataAsList(obj);
            Tuple t = tf.newTuple();
            for (int i=0;i<items.size();i++) {
                if (includedColumns==null || includedColumns[i]) {
                    Object convertedItem = convertHiveToPig(items.get(i), elementFields.get(i).getFieldObjectInspector(), null);
                    t.append(convertedItem);
                }
            }
            result = t;
            break;
        case MAP:
            MapObjectInspector moi = (MapObjectInspector)oi;
            ObjectInspector keyObjectInspector = moi.getMapKeyObjectInspector();
            ObjectInspector valueObjectInspector = moi.getMapValueObjectInspector();
            Map<Object, Object> m = (Map<Object, Object>)obj;
            result = new HashMap();
            for (Map.Entry<Object, Object> entry : m.entrySet()) {
                Object convertedKey = convertHiveToPig(entry.getKey(), keyObjectInspector, null);
                Object convertedValue = convertHiveToPig(entry.getValue(), valueObjectInspector, null);
                if (convertedKey!=null) {
                    ((Map)result).put(convertedKey.toString(), convertedValue);
                } else {
                    PigStatusReporter reporter = PigStatusReporter.getInstance();
                    if (reporter != null) {
                       reporter.incrCounter(PigWarning.UDF_WARNING_1, 1);
                    }
                }
            }
            break;
        case LIST:
            ListObjectInspector loi = (ListObjectInspector)oi;
            result = BagFactory.getInstance().newDefaultBag();
            ObjectInspector itemObjectInspector = loi.getListElementObjectInspector();
            for (Object item : loi.getList(obj)) {
                Object convertedItem = convertHiveToPig(item, itemObjectInspector, null);
                Tuple innerTuple;
                // Hive array contains a single item of any type, if it is not tuple, 
                // need to wrap it in tuple
                if (convertedItem instanceof Tuple) {
                    innerTuple = (Tuple)convertedItem;
                } else {
                    innerTuple = tf.newTuple(1);
                    try {
                        innerTuple.set(0, convertedItem);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                ((DataBag)result).add(innerTuple);
            }
            break;
        default:
            throw new IllegalArgumentException("Unknown type " +
                oi.getCategory());
        }
        return result;
    }

    public static Object getPrimaryFromHive(Object obj, PrimitiveObjectInspector poi) {
        Object result = null;
        if (obj == null) {
            return result;
        }
        switch (poi.getPrimitiveCategory()) {
        case FLOAT:
        case DOUBLE:
        case BOOLEAN:
        case INT:
        case LONG:
        case STRING:
            result = poi.getPrimitiveJavaObject(obj);
            break;
        case CHAR:
            result = ((HiveChar)poi.getPrimitiveJavaObject(obj)).getValue();
            break;
        case VARCHAR:
            result = ((HiveVarchar)poi.getPrimitiveJavaObject(obj)).getValue();
            break;
        case BYTE:
            result = (int)(Byte)poi.getPrimitiveJavaObject(obj);
            break;
        case SHORT:
            result = (int)(Short)poi.getPrimitiveJavaObject(obj);
            break;
        case BINARY:
            byte[] b = (byte[])poi.getPrimitiveJavaObject(obj);
            // Make a copy
            result = new DataByteArray(b, 0, b.length);
            break;
        case TIMESTAMP:
            java.sql.Timestamp origTimeStamp = (java.sql.Timestamp)poi.getPrimitiveJavaObject(obj);
            result = new DateTime(origTimeStamp.getTime());
            break;
        case DATE:
            java.sql.Date origDate = (java.sql.Date)poi.getPrimitiveJavaObject(obj);
            result = new DateTime(origDate.getTime());
            break;
        case DECIMAL:
            org.apache.hadoop.hive.common.type.HiveDecimal origDecimal =
                (org.apache.hadoop.hive.common.type.HiveDecimal)poi.getPrimitiveJavaObject(obj);
            result = origDecimal.bigDecimalValue();
            break;
        default:
            throw new IllegalArgumentException("Unknown primitive type " +
              (poi).getPrimitiveCategory());
        }
        return result;
    }

    public static ResourceFieldSchema getResourceFieldSchema(TypeInfo ti) throws IOException {
        ResourceFieldSchema fieldSchema = new ResourceFieldSchema();
        ResourceFieldSchema[] innerFs;
        ResourceSchema innerSchema;
        switch (ti.getCategory()) {
        case STRUCT:
            StructTypeInfo sti = (StructTypeInfo)ti;
            fieldSchema.setType(DataType.TUPLE);
            List<TypeInfo> typeInfos = sti.getAllStructFieldTypeInfos();
            List<String> names = sti.getAllStructFieldNames();
            innerFs = new ResourceFieldSchema[typeInfos.size()];
            for (int i=0;i<typeInfos.size();i++) {
                innerFs[i] = getResourceFieldSchema(typeInfos.get(i));
                innerFs[i].setName(names.get(i));
            }
            innerSchema = new ResourceSchema();
            innerSchema.setFields(innerFs);
            fieldSchema.setSchema(innerSchema);
            break;
        case LIST:
            ListTypeInfo lti = (ListTypeInfo)ti;
            fieldSchema.setType(DataType.BAG);
            innerFs = new ResourceFieldSchema[1];
            ResourceFieldSchema itemSchema = getResourceFieldSchema(lti.getListElementTypeInfo());
            if (itemSchema.getType() == DataType.TUPLE) {
                innerFs[0] = itemSchema;
            } else {
                // If item is not tuple, wrap it into tuple
                ResourceFieldSchema tupleFieldSchema = new ResourceFieldSchema();
                tupleFieldSchema.setType(DataType.TUPLE);
                ResourceSchema tupleSchema = new ResourceSchema();
                tupleSchema.setFields(new ResourceFieldSchema[] {itemSchema});
                innerFs[0] = tupleFieldSchema;
            }

            innerSchema = new ResourceSchema();
            innerSchema.setFields(innerFs);
            fieldSchema.setSchema(innerSchema);
            break;
        case MAP:
            MapTypeInfo mti = (MapTypeInfo)ti;
            fieldSchema.setType(DataType.MAP);
            innerFs = new ResourceFieldSchema[1];
            innerFs[0] = getResourceFieldSchema(mti.getMapValueTypeInfo());
            innerSchema = new ResourceSchema();
            innerSchema.setFields(innerFs);
            fieldSchema.setSchema(innerSchema);
            break;
        case PRIMITIVE:
            switch (((PrimitiveTypeInfo)ti).getPrimitiveCategory()) {
            case FLOAT:
                fieldSchema.setType(DataType.FLOAT);
                break;
            case DOUBLE:
                fieldSchema.setType(DataType.DOUBLE);
                break;
            case BOOLEAN:
                fieldSchema.setType(DataType.BOOLEAN);
                break;
            case BYTE:
                fieldSchema.setType(DataType.INTEGER);
                break;
            case SHORT:
                fieldSchema.setType(DataType.INTEGER);
                break;
            case INT:
                fieldSchema.setType(DataType.INTEGER);
                break;
            case LONG:
                fieldSchema.setType(DataType.LONG);
                break;
            case BINARY:
                fieldSchema.setType(DataType.BYTEARRAY);
                break;
            case STRING:
                fieldSchema.setType(DataType.CHARARRAY);
                break;
            case VARCHAR:
                fieldSchema.setType(DataType.CHARARRAY);
                break;
            case CHAR:
                fieldSchema.setType(DataType.CHARARRAY);
                break;
            case TIMESTAMP:
                fieldSchema.setType(DataType.DATETIME);
                break;
            case DATE:
                fieldSchema.setType(DataType.DATETIME);
                break;
            case DECIMAL:
                fieldSchema.setType(DataType.BIGDECIMAL);
                break;
            default:
                throw new IllegalArgumentException("Unknown primitive type " +
                    ((PrimitiveTypeInfo)ti).getPrimitiveCategory());
            }
            break;
        }

        return fieldSchema;
    }

    public static TypeInfo getTypeInfo(ResourceFieldSchema fs) throws IOException {
        TypeInfo ti;
        switch (fs.getType()) {
        case DataType.TUPLE:
            ti = new StructTypeInfo();
            ArrayList<String> names = new ArrayList<String>();
            ArrayList<TypeInfo> typeInfos = new ArrayList<TypeInfo>();
            for (ResourceFieldSchema subFs : fs.getSchema().getFields()) {
                TypeInfo info = getTypeInfo(subFs);
                names.add(subFs.getName());
                typeInfos.add(info);
            }
            ((StructTypeInfo)ti).setAllStructFieldNames(names);
            ((StructTypeInfo)ti).setAllStructFieldTypeInfos(typeInfos);
            break;
        case DataType.BAG:
            ti = new ListTypeInfo();
            if (fs.getSchema()==null || fs.getSchema().getFields().length!=1) {
                throw new IOException("Wrong bag inner schema");
            }
            ResourceFieldSchema tupleSchema = fs.getSchema().getFields()[0];
            ResourceFieldSchema itemSchema = tupleSchema;
            // If single item tuple, remove the tuple, put the inner item into list directly
            if (tupleSchema.getSchema().getFields().length == 1) {
                itemSchema = tupleSchema.getSchema().getFields()[0];
            }
            TypeInfo elementField = getTypeInfo(itemSchema);
            ((ListTypeInfo)ti).setListElementTypeInfo(elementField);
            break;
        case DataType.MAP:
            ti = new MapTypeInfo();
            TypeInfo valueField;
            if (fs.getSchema() == null || fs.getSchema().getFields().length != 1) {
                valueField = TypeInfoFactory.binaryTypeInfo;
            } else {
                valueField = getTypeInfo(fs.getSchema().getFields()[0]);
            }
            ((MapTypeInfo)ti).setMapKeyTypeInfo(TypeInfoFactory.stringTypeInfo);
            ((MapTypeInfo)ti).setMapValueTypeInfo(valueField);
            break;
        case DataType.BOOLEAN:
            ti = TypeInfoFactory.booleanTypeInfo;
            break;
        case DataType.INTEGER:
            ti = TypeInfoFactory.intTypeInfo;
            break;
        case DataType.LONG:
            ti = TypeInfoFactory.longTypeInfo;
            break;
        case DataType.FLOAT:
            ti = TypeInfoFactory.floatTypeInfo;
            break;
        case DataType.DOUBLE:
            ti = TypeInfoFactory.doubleTypeInfo;
            break;
        case DataType.CHARARRAY:
            ti = TypeInfoFactory.stringTypeInfo;
            break;
        case DataType.DATETIME:
            ti = TypeInfoFactory.timestampTypeInfo;
            break;
        case DataType.BIGDECIMAL:
            ti = TypeInfoFactory.decimalTypeInfo;
            break;
        case DataType.BIGINTEGER:
            ti = TypeInfoFactory.decimalTypeInfo;
            break;
        case DataType.BYTEARRAY:
            ti = TypeInfoFactory.binaryTypeInfo;
            break;
        default:
            throw new IllegalArgumentException("Unknown data type " +
                DataType.findTypeName(fs.getType()));
        }
        return ti;
    }

    static public class Field implements StructField {
        private final String name;
        private final ObjectInspector inspector;
        private final int offset;

        public Field(String name, ObjectInspector inspector, int offset) {
          this.name = name;
          this.inspector = inspector;
          this.offset = offset;
        }

        @Override
        public String getFieldName() {
          return name;
        }

        @Override
        public ObjectInspector getFieldObjectInspector() {
          return inspector;
        }

        @Override
        public int getFieldID() {
            return offset;
        }

        @Override
        public String getFieldComment() {
          return null;
        }
      }

    static class PigStructInspector extends StructObjectInspector {
        private List<StructField> fields;

        PigStructInspector(StructTypeInfo info) {
            ArrayList<String> fieldNames = info.getAllStructFieldNames();
            ArrayList<TypeInfo> fieldTypes = info.getAllStructFieldTypeInfos();
            fields = new ArrayList<StructField>(fieldNames.size());
            for (int i = 0; i < fieldNames.size(); ++i) {
                fields.add(new Field(fieldNames.get(i),
                        createObjectInspector(fieldTypes.get(i)), i));
            }
        }

        PigStructInspector(List<StructField> fields) {
            this.fields = fields;
        }

        @Override
        public List<StructField> getAllStructFieldRefs() {
            return fields;
        }

        @Override
        public StructField getStructFieldRef(String s) {
            for (StructField field : fields) {
                if (field.getFieldName().equals(s)) {
                    return field;
                }
            }
            return null;
        }

        @Override
        public Object getStructFieldData(Object object, StructField field) {
            Object result = null;
            try {
                result = ((Tuple) object).get(((Field) field).offset);
            } catch (ExecException e) {
                throw new RuntimeException(e);
            }
            return result;
        }

        @Override
        public List<Object> getStructFieldsDataAsList(Object object) {
            return ((Tuple) object).getAll();
        }

        @Override
        public String getTypeName() {
            StringBuilder buffer = new StringBuilder();
            buffer.append("struct<");
            for (int i = 0; i < fields.size(); ++i) {
                StructField field = fields.get(i);
                if (i != 0) {
                    buffer.append(",");
                }
                buffer.append(field.getFieldName());
                buffer.append(":");
                buffer.append(field.getFieldObjectInspector().getTypeName());
            }
            buffer.append(">");
            return buffer.toString();
        }

        @Override
        public Category getCategory() {
            return Category.STRUCT;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || o.getClass() != getClass()) {
                return false;
            } else if (o == this) {
                return true;
            } else {
                List<StructField> other = ((PigStructInspector) o).fields;
                if (other.size() != fields.size()) {
                    return false;
                }
                for (int i = 0; i < fields.size(); ++i) {
                    StructField left = other.get(i);
                    StructField right = fields.get(i);
                    if (!(left.getFieldName().equals(right.getFieldName()) && left
                            .getFieldObjectInspector().equals(
                                    right.getFieldObjectInspector()))) {
                        return false;
                    }
                }
                return true;
            }
        }
    }

    static class PigMapObjectInspector implements MapObjectInspector {
        private ObjectInspector key;
        private ObjectInspector value;

        PigMapObjectInspector(MapTypeInfo info) {
            key = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
            value = createObjectInspector(info.getMapValueTypeInfo());
        }

        @Override
        public ObjectInspector getMapKeyObjectInspector() {
            return key;
        }

        @Override
        public ObjectInspector getMapValueObjectInspector() {
            return value;
        }

        @Override
        public Object getMapValueElement(Object map, Object key) {
            return ((Map) map).get(key);
        }

        @Override
        public Map<Object, Object> getMap(Object map) {
            return (Map) map;
        }

        @Override
        public int getMapSize(Object map) {
            return ((Map) map).size();
        }

        @Override
        public String getTypeName() {
            return "map<" + key.getTypeName() + "," + value.getTypeName() + ">";
        }

        @Override
        public Category getCategory() {
            return Category.MAP;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || o.getClass() != getClass()) {
                return false;
            } else if (o == this) {
                return true;
            } else {
                PigMapObjectInspector other = (PigMapObjectInspector) o;
                return other.key.equals(key) && other.value.equals(value);
            }
        }
    }

    static class PigListObjectInspector implements ListObjectInspector {
        private ObjectInspector child;
        private Object cachedObject;
        private int index;
        private Iterator<Tuple> iter;

        PigListObjectInspector(ListTypeInfo info) {
            child = createObjectInspector(info.getListElementTypeInfo());
        }

        @Override
        public ObjectInspector getListElementObjectInspector() {
            return child;
        }

        @Override
        public Object getListElement(Object list, int i) {
            if (i==0 || list!=cachedObject) {
                cachedObject = list;
                index = -1;
                DataBag db = (DataBag)list;
                iter = db.iterator();
            }
            if (i==index+1) {
                index++;
                try {
                    Tuple t = iter.next();
                    // If single item tuple, take the item directly from list
                    if (t.size() == 1) {
                        return t.get(0);
                    } else {
                        return t;
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } else {
                throw new RuntimeException("Only sequential read is supported");
            }
        }

        @Override
        public int getListLength(Object list) {
            return (int)((DataBag)list).size();
        }

        @Override
        @SuppressWarnings("unchecked")
        public List<?> getList(Object list) {
            List<Object> result = new ArrayList<Object>();
            DataBag bag = (DataBag)list;
            for (Tuple t : bag) {
                if (t.size() == 1) {
                    try {
                        result.add(t.get(0));
                    } catch (ExecException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    result.add(t);
                }
            }
            return result;
        }

        @Override
        public String getTypeName() {
            return "array<" + child.getTypeName() + ">";
        }

        @Override
        public Category getCategory() {
            return Category.LIST;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || o.getClass() != getClass()) {
                return false;
            } else if (o == this) {
                return true;
            } else {
                ObjectInspector other = ((PigListObjectInspector) o).child;
                return other.equals(child);
            }
        }
    }

    static class PigDataByteArrayObjectInspector extends AbstractPrimitiveJavaObjectInspector
            implements BinaryObjectInspector {

        PigDataByteArrayObjectInspector() {
            super(TypeInfoFactory.binaryTypeInfo);
        }

        @Override
        public BytesWritable getPrimitiveWritableObject(Object o) {
            return o == null ? null : (o instanceof DataByteArray
                            ? new BytesWritable(((DataByteArray) o).get())
                            : new BytesWritable((byte[]) o));
        }

        @Override
        public byte[] getPrimitiveJavaObject(Object o) {
            return ((DataByteArray) o).get();
        }

    }

    static class PigJodaTimeStampObjectInspector extends
            AbstractPrimitiveJavaObjectInspector implements TimestampObjectInspector {

        protected PigJodaTimeStampObjectInspector() {
            super(TypeInfoFactory.timestampTypeInfo);
        }

        @Override
        public TimestampWritable getPrimitiveWritableObject(Object o) {
            return o == null ? null : new TimestampWritable(new Timestamp(((DateTime)o).getMillis()));
        }

        @Override
        public Timestamp getPrimitiveJavaObject(Object o) {
            return o == null ? null : new Timestamp(((DateTime)o).getMillis());
        }
    }

    static class PigDecimalObjectInspector extends
            AbstractPrimitiveJavaObjectInspector implements HiveDecimalObjectInspector {

        protected PigDecimalObjectInspector() {
            super(TypeInfoFactory.decimalTypeInfo);
        }

        @Override
        public HiveDecimalWritable getPrimitiveWritableObject(Object o) {
            if (o instanceof BigDecimal) {
                return o == null ? null : new HiveDecimalWritable(HiveDecimal.create((BigDecimal)o));
            } else { // BigInteger
                return o == null ? null : new HiveDecimalWritable(HiveDecimal.create((BigInteger)o));
            }
        }

        @Override
        public HiveDecimal getPrimitiveJavaObject(Object o) {
            if (o instanceof BigDecimal) {
                return o == null ? null : HiveDecimal.create((BigDecimal)o);
            } else { // BigInteger
                return o == null ? null : HiveDecimal.create((BigInteger)o);
            }
        }
    }

    public static ObjectInspector createObjectInspector(TypeInfo info) {
        switch (info.getCategory()) {
        case PRIMITIVE:
          switch (((PrimitiveTypeInfo) info).getPrimitiveCategory()) {
            case FLOAT:
              return PrimitiveObjectInspectorFactory.javaFloatObjectInspector;
            case DOUBLE:
              return PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
            case BOOLEAN:
              return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
            case INT:
              return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
            case LONG:
              return PrimitiveObjectInspectorFactory.javaLongObjectInspector;
            case STRING:
              return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
            case TIMESTAMP:
              return new PigJodaTimeStampObjectInspector();
            case DECIMAL:
              return new PigDecimalObjectInspector();
            case BINARY:
              return new PigDataByteArrayObjectInspector();
            case DATE:
            case VARCHAR:
            case BYTE:
            case SHORT:
                throw new IllegalArgumentException("Should never happen, " + 
                        (((PrimitiveTypeInfo) info).getPrimitiveCategory()) +
                        "is not valid Pig primitive data type");
            default:
                throw new IllegalArgumentException("Unknown primitive type " +
                        ((PrimitiveTypeInfo) info).getPrimitiveCategory());
          }
        case STRUCT:
          return new PigStructInspector((StructTypeInfo) info);
        case MAP:
          return new PigMapObjectInspector((MapTypeInfo) info);
        case LIST:
          return new PigListObjectInspector((ListTypeInfo) info);
        default:
          throw new IllegalArgumentException("Unknown type " +
            info.getCategory());
        }
    }

    public static ConstantObjectInspector getConstantObjectInspector(Object obj) {
        switch (DataType.findType(obj)) {
        case DataType.FLOAT:
            return new JavaConstantFloatObjectInspector((Float)obj);
        case DataType.DOUBLE:
            return new JavaConstantDoubleObjectInspector((Double)obj);
        case DataType.BOOLEAN:
            return new JavaConstantBooleanObjectInspector((Boolean)obj);
        case DataType.INTEGER:
            return new JavaConstantIntObjectInspector((Integer)obj);
        case DataType.LONG:
            return new JavaConstantLongObjectInspector((Long)obj);
        case DataType.CHARARRAY:
            return new JavaConstantStringObjectInspector((String)obj);
        default:
            throw new IllegalArgumentException("Not implemented " + obj.getClass().getName());
        }
    }
}
