/*
 * Copyright © 2021 ProStore
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ru.datamart.prostore.query.execution.core.base.utils;

import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.EntityField;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificData;

public class AvroUtils {
    static {
        SpecificData.get().addLogicalTypeConversion(new TimeConversions.TimeMicrosConversion());
        SpecificData.get().addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion());
        SpecificData.get().addLogicalTypeConversion(new TimeConversions.DateConversion());
    }

    public static Schema.Field createSysOpField() {
        return new Schema.Field("sys_op", Schema.create(Schema.Type.INT), null, 0);
    }

    public static Schema.Field toSchemaField(EntityField column) {
        return column.getNullable() ? genNullableField(column) : genNonNullableField(column);
    }

    public static Schema metadataColumnTypeToAvroSchema(ColumnType columnType) {
        Schema schema;
        switch (columnType) {
            case UUID:
            case VARCHAR:
            case LINK:
            case CHAR:
                schema = Schema.create(Schema.Type.STRING);
                GenericData.setStringType(schema, GenericData.StringType.String);
                break;
            case INT32:
                schema = Schema.create(Schema.Type.INT);
                break;
            case INT:
            case BIGINT:
                schema = Schema.create(Schema.Type.LONG);
                break;
            case DOUBLE:
                schema = Schema.create(Schema.Type.DOUBLE);
                break;
            case FLOAT:
                schema = Schema.create(Schema.Type.FLOAT);
                break;
            case DATE:
                schema = SpecificData.get().getConversionFor(LogicalTypes.date()).getRecommendedSchema();
                break;
            case TIME:
                schema = SpecificData.get().getConversionFor(LogicalTypes.timeMicros()).getRecommendedSchema();
                break;
            case TIMESTAMP:
                schema = SpecificData.get().getConversionFor(LogicalTypes.timestampMicros()).getRecommendedSchema();
                break;
            case BOOLEAN:
                schema = Schema.create(Schema.Type.BOOLEAN);
                break;
            default:
                throw new IllegalArgumentException("Unsupported data type: " + columnType);
        }
        return schema;
    }

    private static Schema.Field genNullableField(EntityField column) {
        Schema.Field field = new Schema.Field(column.getName(),
            Schema.createUnion(Schema.create(Schema.Type.NULL), metadataColumnTypeToAvroSchema(column.getType())),
            null, Schema.Field.NULL_DEFAULT_VALUE);
        return field;
    }

    private static Schema.Field genNonNullableField(EntityField column) {
        return new Schema.Field(column.getName(),
            metadataColumnTypeToAvroSchema(column.getType()),
            null);
    }
}
