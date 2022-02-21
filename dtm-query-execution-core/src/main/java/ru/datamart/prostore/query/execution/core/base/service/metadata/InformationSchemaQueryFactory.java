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
package ru.datamart.prostore.query.execution.core.base.service.metadata;

import ru.datamart.prostore.common.reader.InformationSchemaView;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.stream.Collectors;

@Component
@AllArgsConstructor
public class InformationSchemaQueryFactory {

    private final DataTypeMapper dataTypeMapper;

    public String createInitEntitiesQuery() {
        return String.format("SELECT TABLE_NAME, ORDINAL_POSITION, COLUMN_NAME, %s, IS_NULLABLE" +
                        " FROM information_schema.columns WHERE TABLE_SCHEMA = '%s' and TABLE_NAME in (%s);",
                selectDataType(),
                InformationSchemaView.DTM_SCHEMA_NAME,
                Arrays.stream(InformationSchemaView.values())
                        .map(view -> String.format("'%s'", view.getRealName().toUpperCase()))
                        .collect(Collectors.joining(",")));
    }

    private String selectDataType() {
        StringBuilder result = new StringBuilder();

        result.append(" case ");
        dataTypeMapper.getHsqlToLogicalSchemaMapping()
                .forEach((key, value) -> result.append(String.format(" when DATA_TYPE = '%s' then '%s' ", key, value)));
        result.append(" else DATA_TYPE end as DATA_TYPE");

        return result.toString();
    }

}
