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
package ru.datamart.prostore.query.execution.plugin.adb.base.service.converter;

import lombok.val;
import org.springframework.stereotype.Component;
import ru.datamart.prostore.common.converter.SqlTypeConverter;
import ru.datamart.prostore.common.converter.transformer.ColumnTransformer;
import ru.datamart.prostore.common.converter.transformer.impl.*;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.query.execution.plugin.adb.base.service.converter.transformer.AdbBigintFromNumberTransformer;
import ru.datamart.prostore.query.execution.plugin.adb.base.service.converter.transformer.AdbDoubleFromNumberTransformer;
import ru.datamart.prostore.query.execution.plugin.adb.base.service.converter.transformer.AdbFloatFromNumberTransformer;
import ru.datamart.prostore.query.execution.plugin.adb.base.service.converter.transformer.AdbLongFromNumericTransformer;

import java.util.HashMap;
import java.util.Map;

@Component("fromAdbSqlTypeConverter")
public class FromAdbSqlTypeConverter implements SqlTypeConverter {

    private final Map<ColumnType, Map<Class<?>, ColumnTransformer>> transformerMap;

    public FromAdbSqlTypeConverter() {
        Map<ColumnType, Map<Class<?>, ColumnTransformer>> transformerMap = new HashMap<>();
        Map<Class<?>, ColumnTransformer> adbLongFromNumericTransformerMap = ColumnTransformer.getTransformerMap(new AdbLongFromNumericTransformer());
        transformerMap.put(ColumnType.INT, adbLongFromNumericTransformerMap);
        transformerMap.put(ColumnType.INT32, adbLongFromNumericTransformerMap);
        val toStringTransformerMap = ColumnTransformer.getTransformerMap(new ToStringFromStringTransformer());
        transformerMap.put(ColumnType.VARCHAR, toStringTransformerMap);
        transformerMap.put(ColumnType.LINK, toStringTransformerMap);
        transformerMap.put(ColumnType.CHAR, toStringTransformerMap);
        transformerMap.put(ColumnType.BIGINT, ColumnTransformer.getTransformerMap(new AdbBigintFromNumberTransformer()));
        transformerMap.put(ColumnType.DOUBLE, ColumnTransformer.getTransformerMap(new AdbDoubleFromNumberTransformer()));
        transformerMap.put(ColumnType.FLOAT, ColumnTransformer.getTransformerMap(new AdbFloatFromNumberTransformer()));
        transformerMap.put(ColumnType.DATE, ColumnTransformer.getTransformerMap(
                new DateFromNumericTransformer(),
                new DateFromLongTransformer(),
                new DateFromLocalDateTransformer()
        ));
        transformerMap.put(ColumnType.TIME, ColumnTransformer.getTransformerMap(new TimeFromLocalTimeTransformer()));
        transformerMap.put(ColumnType.TIMESTAMP, ColumnTransformer.getTransformerMap(new TimestampFromLocalDateTimeTransformer()));
        transformerMap.put(ColumnType.BOOLEAN, ColumnTransformer.getTransformerMap(new BooleanFromBooleanTransformer()));
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
