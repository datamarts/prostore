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
package ru.datamart.prostore.query.execution.core.base.service.avro;

import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.query.execution.core.base.utils.AvroUtils;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.avro.Schema;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

@Component
@Slf4j
public class AvroSchemaGenerator {

    public Schema generateTableSchema(Entity table) {
        return generateTableSchema(table, true);
    }

    public Schema generateTableSchema(Entity table, boolean withSysOpField) {
        List<Schema.Field> fields = getFields(table, withSysOpField);
        return Schema.createRecord(table.getName(), null, table.getSchema(), false, fields);
    }

    @NotNull
    private List<Schema.Field> getFields(Entity table, boolean withSysOpField) {
        val fields = table.getFields().stream()
            .sorted(Comparator.comparing(EntityField::getOrdinalPosition))
            .map(AvroUtils::toSchemaField)
            .collect(Collectors.toList());

        boolean hasAlreadySysOpField = table.getFields().stream()
                .anyMatch(f -> f.getName().equalsIgnoreCase("sys_op"));

        if (withSysOpField && !hasAlreadySysOpField) {
            fields.add(AvroUtils.createSysOpField());
        }

        return fields;
    }
}
