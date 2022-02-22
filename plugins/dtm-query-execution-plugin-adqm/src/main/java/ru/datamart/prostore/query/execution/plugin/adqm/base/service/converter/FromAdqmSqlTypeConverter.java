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
package ru.datamart.prostore.query.execution.plugin.adqm.base.service.converter;

import lombok.val;
import org.springframework.stereotype.Component;
import ru.datamart.prostore.common.converter.SqlTypeConverter;
import ru.datamart.prostore.common.converter.transformer.ColumnTransformer;
import ru.datamart.prostore.common.converter.transformer.impl.*;
import ru.datamart.prostore.common.model.ddl.ColumnType;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.HashMap;
import java.util.Map;

@Component("fromAdqmSqlTypeConverter")
public class FromAdqmSqlTypeConverter implements SqlTypeConverter {
    private static final DateTimeFormatter DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd")
            .optionalStart()
            .appendLiteral("T")
            .optionalEnd()
            .appendPattern("HH:mm:ss")
            .optionalStart()
            .appendLiteral("Z")
            .optionalEnd()
            .toFormatter();

    private final Map<ColumnType, Map<Class<?>, ColumnTransformer>> transformerMap;

    public FromAdqmSqlTypeConverter() {
        Map<ColumnType, Map<Class<?>, ColumnTransformer>> transformerMap = new HashMap<>();
        val longFromNumberTransformerMap = ColumnTransformer.getTransformerMap(new LongFromNumberTransformer());
        transformerMap.put(ColumnType.INT, longFromNumberTransformerMap);
        transformerMap.put(ColumnType.INT32, longFromNumberTransformerMap);
        val toStringTransformerMap = ColumnTransformer.getTransformerMap(new ToStringFromStringTransformer());
        transformerMap.put(ColumnType.VARCHAR, toStringTransformerMap);
        transformerMap.put(ColumnType.CHAR, toStringTransformerMap);
        transformerMap.put(ColumnType.LINK, toStringTransformerMap);
        transformerMap.put(ColumnType.BIGINT, ColumnTransformer.getTransformerMap(new BigintFromNumberTransformer()));
        transformerMap.put(ColumnType.DOUBLE, ColumnTransformer.getTransformerMap(new DoubleFromNumberTransformer()));
        transformerMap.put(ColumnType.FLOAT, ColumnTransformer.getTransformerMap(new FloatFromNumberTransformer()));
        transformerMap.put(ColumnType.DATE, ColumnTransformer.getTransformerMap(
                new DateFromLongTransformer(),
                new DateFromStringTransformer()
        ));
        transformerMap.put(ColumnType.TIME, ColumnTransformer.getTransformerMap(new TimeFromNumberTransformer()));
        transformerMap.put(ColumnType.TIMESTAMP, ColumnTransformer.getTransformerMap(
                new TimestampFromStringTransformer(DATE_TIME_FORMATTER),
                new TimestampFromLongTransformer()
        ));
        transformerMap.put(ColumnType.BOOLEAN, ColumnTransformer.getTransformerMap(new BooleanFromBooleanTransformer(), new BooleanFromNumericTransformer()));
        transformerMap.put(ColumnType.UUID, ColumnTransformer.getTransformerMap(new UuidFromStringTransformer()));
        transformerMap.put(ColumnType.BLOB, ColumnTransformer.getTransformerMap(new BlobFromObjectTransformer()));
        transformerMap.put(ColumnType.ANY, ColumnTransformer.getTransformerMap(new IdentityObjectTransformer()));
        this.transformerMap = transformerMap;
    }

    @Override
    public Map<ColumnType, Map<Class<?>, ColumnTransformer>> getTransformerMap() {
        return this.transformerMap;
    }

}
