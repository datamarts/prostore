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
package ru.datamart.prostore.query.execution.core.base.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.datamart.prostore.common.converter.transformer.ColumnTransformer;
import ru.datamart.prostore.common.converter.transformer.impl.*;
import ru.datamart.prostore.common.model.ddl.ColumnType;

import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import static ru.datamart.prostore.common.converter.transformer.ColumnTransformer.getTransformerMap;

@Configuration
public class ConverterConfiguration {

    @Bean("coreTransformerMap")
    public Map<ColumnType, Map<Class<?>, ColumnTransformer>> transformerMap() {
        Map<ColumnType, Map<Class<?>, ColumnTransformer>> transformerMap = new HashMap<>();
        transformerMap.put(ColumnType.INT, getTransformerMap(new ToStringFromStringTransformer()));
        transformerMap.put(ColumnType.VARCHAR, getTransformerMap(new ToStringFromStringTransformer()));
        transformerMap.put(ColumnType.CHAR, transformerMap.get(ColumnType.VARCHAR));
        transformerMap.put(ColumnType.BIGINT, getTransformerMap(new BigintFromNumberTransformer()));
        transformerMap.put(ColumnType.DOUBLE, getTransformerMap(new DoubleFromNumberTransformer()));
        transformerMap.put(ColumnType.FLOAT, getTransformerMap(new FloatFromNumberTransformer()));
        transformerMap.put(ColumnType.DATE, getTransformerMap(new DateFromLocalDateTransformer()));
        transformerMap.put(ColumnType.TIME, getTransformerMap(new TimeFromLocalTimeTransformer()));
        transformerMap.put(ColumnType.TIMESTAMP,
                getTransformerMap(
                        new TimestampFromStringTransformer(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                        new TimestampFromLocalDateTimeTransformer()
            ));
        transformerMap.put(ColumnType.BOOLEAN, getTransformerMap(new BooleanFromBooleanTransformer()));
        transformerMap.put(ColumnType.UUID, getTransformerMap(new UuidFromStringTransformer()));
        transformerMap.put(ColumnType.BLOB, getTransformerMap(new BlobFromObjectTransformer()));
        transformerMap.put(ColumnType.ANY, getTransformerMap(new IdentityObjectTransformer()));
        return transformerMap;
    }
}
