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
package ru.datamart.prostore.query.execution.core.base.service.metadata.query;

import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.EntityFieldUtils;
import lombok.val;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

@Component("hsqlDdlQueryGenerator")
public class HsqlDdlQueryGenerator implements DdlQueryGenerator {

    public static final String DELIMITER = ", ";

    @Override
    public String generateCreateViewQuery(Entity entity, String namePrefix) {
        val sb = new StringBuilder()
                .append("CREATE VIEW ")
                .append(entity.getSchema()).append('.').append(namePrefix).append(entity.getName())
                .append(" AS ")
                .append(entity.getViewQuery());
        return sb.toString();
    }

    @Override
    public String generateCreateTableQuery(Entity entity) {
        val tableName = entity.getNameWithSchema();
        val sb = new StringBuilder()
                .append("CREATE TABLE ").append(tableName)
                .append(" (");
        appendClassTableFields(sb, entity.getFields());

        List<EntityField> pkList = EntityFieldUtils.getPrimaryKeyList(entity.getFields());
        if (!pkList.isEmpty()) {
            appendPrimaryKeys(sb, tableName, pkList);
        }
        sb.append(")");
        return sb.toString();
    }

    private void appendClassTableFields(StringBuilder builder, List<EntityField> fields) {
        val columns = fields.stream()
                .map(this::getColumnDDLByField)
                .collect(Collectors.joining(DELIMITER));
        builder.append(columns);
    }

    private String getColumnDDLByField(EntityField field) {
        val sb = new StringBuilder();
        sb.append(field.getName())
                .append(" ")
                .append(getColumnType(field));
        if (!field.getNullable()) {
            sb.append(" NOT NULL");
        }
        return sb.toString();
    }

    private String getColumnType(EntityField field) {
        switch (field.getType()) {
            case INT32:
            case INT:
                return "INTEGER";
            case LINK:
            case UUID:
                return "LONGVARCHAR";
            case CHAR:
            case VARCHAR:
                return field.getSize() == null ? "LONGVARCHAR" : getFieldTypeWithSize(field.getSize(), field.getType());
            case TIME:
            case TIMESTAMP:
                return getFieldTypeWithSize(field.getAccuracy(), field.getType());
            default:
                return field.getType().toString();
        }
    }

    private String getFieldTypeWithSize(Integer fieldSize, ColumnType type) {
        val size = fieldSize == null ? "" : "(" + fieldSize + ")";
        return type.toString() + size;
    }

    private void appendPrimaryKeys(StringBuilder builder, String tableName, Collection<EntityField> pkList) {
        List<String> pkFields = pkList.stream().map(EntityField::getName).collect(Collectors.toList());
        builder.append(DELIMITER)
                .append("constraint ")
                .append("pk_")
                .append(tableName.replace('.', '_'))
                .append(" primary key (")
                .append(String.join(DELIMITER, pkFields))
                .append(")");
    }
}
