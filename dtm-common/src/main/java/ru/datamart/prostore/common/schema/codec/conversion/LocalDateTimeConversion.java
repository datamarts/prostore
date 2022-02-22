/*
 * Copyright © 2022 DATAMART LLC
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
package ru.datamart.prostore.common.schema.codec.conversion;

import ru.datamart.prostore.common.schema.codec.type.LocalDateTimeLogicalType;
import ru.datamart.prostore.common.util.DateTimeUtils;
import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.regex.Pattern;

public class LocalDateTimeConversion extends Conversion<LocalDateTime> {
    private static final Pattern TIME_ZONE_PATTERN
            = Pattern.compile("(?:Z|[+-](?:2[0-3]|[01][0-9]):[0-5][0-9]|\\w+/\\w+)$");

    private LocalDateTimeConversion() {
        super();
    }

    public static LocalDateTimeConversion getInstance() {
        return LocalDateTimeConversionHolder.INSTANCE;
    }

    @Override
    public Class<LocalDateTime> getConvertedType() {
        return LocalDateTime.class;
    }

    @Override
    public String getLogicalTypeName() {
        return LocalDateTimeLogicalType.INSTANCE.getName();
    }

    @Override
    public Schema getRecommendedSchema() {
        return LocalDateTimeLogicalType.INSTANCE.addToSchema(Schema.create(Schema.Type.LONG));
    }

    @Override
    public Long toLong(LocalDateTime value, Schema schema, LogicalType type) {
        return DateTimeUtils.toMicros(value);
    }

    @Override
    public LocalDateTime fromLong(Long value, Schema schema, LogicalType type) {
        return DateTimeUtils.toLocalDateTime(value);
    }

    @Override
    public LocalDateTime fromCharSequence(CharSequence value, Schema schema, LogicalType type) {
        return TIME_ZONE_PATTERN.matcher(value).find() ? ZonedDateTime.parse(value).toLocalDateTime() : LocalDateTime.parse(value);
    }

    @Override
    public CharSequence toCharSequence(LocalDateTime value, Schema schema, LogicalType type) {
        return value.toString();
    }

    private static class LocalDateTimeConversionHolder {
        private static final LocalDateTimeConversion INSTANCE = new LocalDateTimeConversion();
    }
}
