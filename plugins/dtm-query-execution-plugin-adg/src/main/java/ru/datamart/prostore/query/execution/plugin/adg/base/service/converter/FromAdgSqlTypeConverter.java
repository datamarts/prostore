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
package ru.datamart.prostore.query.execution.plugin.adg.base.service.converter;

import lombok.val;
import org.springframework.stereotype.Component;
import ru.datamart.prostore.common.converter.SqlTypeConverter;
import ru.datamart.prostore.common.converter.transformer.ColumnTransformer;
import ru.datamart.prostore.common.converter.transformer.impl.IdentityObjectTransformer;
import ru.datamart.prostore.common.converter.transformer.impl.UuidFromStringTransformer;
import ru.datamart.prostore.common.model.ddl.ColumnType;

import java.util.HashMap;
import java.util.Map;

@Component("fromAdgSqlTypeConverter")
public class FromAdgSqlTypeConverter implements SqlTypeConverter {

    private final Map<ColumnType, Map<Class<?>, ColumnTransformer>> transformerMap;

    public FromAdgSqlTypeConverter() {
        Map<ColumnType, Map<Class<?>, ColumnTransformer>> transformerMap = new HashMap<>();
        val identityTransformer = ColumnTransformer.getTransformerMap(new IdentityObjectTransformer());
        for (ColumnType columnType : ColumnType.values()) {
            if (columnType == ColumnType.UUID) {
                transformerMap.put(columnType, ColumnTransformer.getTransformerMap(new UuidFromStringTransformer()));
            } else {
                transformerMap.put(columnType, identityTransformer);
            }
        }

        this.transformerMap = transformerMap;
    }

    @Override
    public Map<ColumnType, Map<Class<?>, ColumnTransformer>> getTransformerMap() {
        return transformerMap;
    }
}
