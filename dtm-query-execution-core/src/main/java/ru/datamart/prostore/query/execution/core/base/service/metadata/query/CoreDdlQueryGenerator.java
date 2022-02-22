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
package ru.datamart.prostore.query.execution.core.base.service.metadata.query;

import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.EntityFieldUtils;
import lombok.val;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Component("coreDdlQueryGenerator")
public class CoreDdlQueryGenerator implements DdlQueryGenerator {
    @Override
    public String generateCreateTableQuery(Entity entity) {
        final List<EntityField> entityFields = entity.getFields();
        return "CREATE TABLE " +
                entity.getNameWithSchema().toUpperCase() +
                " (" +
                getTableFields(entityFields) +
                ", PRIMARY KEY (" +
                getPrimaryFields(entityFields) +
                ")) DISTRIBUTED BY (" +
                getDistributedByFields(entityFields) +
                ") DATASOURCE_TYPE (" +
                getDestination(entity) +
                ')';
    }

    @Override
    public String generateCreateViewQuery(Entity entity, String namePrefix) {
        return "CREATE VIEW " +
                entity.getNameWithSchema().toUpperCase() +
                " AS " +
                entity.getViewQuery().toUpperCase();
    }

    @Override
    public String generateCreateMaterializedView(Entity entity) {
        final List<EntityField> entityFields = entity.getFields();
        return "CREATE MATERIALIZED VIEW " +
                entity.getNameWithSchema().toUpperCase() +
                " (" +
                getTableFields(entityFields) +
                ", PRIMARY KEY (" +
                getPrimaryFields(entityFields) +
                ")) DISTRIBUTED BY (" +
                getDistributedByFields(entityFields) +
                ") DATASOURCE_TYPE (" +
                getDestination(entity) +
                ") AS " +
                entity.getViewQuery().toUpperCase() +
                " DATASOURCE_TYPE = '" +
                entity.getMaterializedDataSource() +
                '\'';
    }

    private String getTableFields(List<EntityField> entityFields) {
        return entityFields.stream()
                .map(this::generateFieldCreateScript)
                .collect(Collectors.joining(", "));
    }

    private String generateFieldCreateScript(EntityField field) {
        val fieldCreateSql = new StringBuilder(field.getName().toUpperCase())
                .append(' ')
                .append(field.getType().name());
        if (field.getSize() != null) {
            fieldCreateSql.append("(")
                    .append(field.getSize())
                    .append(")");
        } else if (field.getAccuracy() != null) {
            fieldCreateSql.append("(")
                    .append(field.getAccuracy())
                    .append(")");
        }
        if (field.getNullable()) {
            fieldCreateSql.append(" NULL");
        } else {
            fieldCreateSql.append(" NOT NULL");
        }
        return fieldCreateSql.toString();
    }

    private String getPrimaryFields(List<EntityField> entityFields) {
        return EntityFieldUtils.getPrimaryKeyList(entityFields).stream()
                .map(e -> e.getName().toUpperCase())
                .collect(Collectors.joining(", "));
    }

    private String getDistributedByFields(List<EntityField> entityFields) {
        return EntityFieldUtils.getShardingKeyList(entityFields).stream()
                .map(e -> e.getName().toUpperCase())
                .collect(Collectors.joining(", "));
    }

    private String getDestination(Entity entity) {
        return entity.getDestination().stream()
                .map(Enum::name)
                .sorted()
                .collect(Collectors.joining(", "));
    }
}
