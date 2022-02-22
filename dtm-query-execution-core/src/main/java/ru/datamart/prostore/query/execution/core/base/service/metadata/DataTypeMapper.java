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
package ru.datamart.prostore.query.execution.core.base.service.metadata;

import ru.datamart.prostore.common.model.ddl.ColumnType;
import lombok.Getter;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Component
@Getter
public class DataTypeMapper {

    private final Map<String, String> hsqlToLogicalSchemaMapping;

    public DataTypeMapper() {
        Map<String, String> typesMapping = new HashMap<>();

        typesMapping.put("CHARACTER VARYING", ColumnType.VARCHAR.name());
        typesMapping.put("CHARACTER", ColumnType.CHAR.name());
        typesMapping.put("LONGVARCHAR", ColumnType.VARCHAR.name());
        typesMapping.put("INTEGER", ColumnType.INT.name());
        typesMapping.put("SMALLINT", ColumnType.INT32.name());
        typesMapping.put("TINYINT", ColumnType.INT32.name());
        typesMapping.put("DOUBLE PRECISION", ColumnType.DOUBLE.name());
        typesMapping.put("DECIMAL", ColumnType.DOUBLE.name());
        typesMapping.put("DEC", ColumnType.DOUBLE.name());
        typesMapping.put("NUMERIC", ColumnType.DOUBLE.name());

        hsqlToLogicalSchemaMapping = Collections.unmodifiableMap(typesMapping);
    }

}
