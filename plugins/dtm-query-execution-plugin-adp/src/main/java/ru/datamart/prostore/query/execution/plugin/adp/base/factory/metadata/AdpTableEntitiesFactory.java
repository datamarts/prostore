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
package ru.datamart.prostore.query.execution.plugin.adp.base.factory.metadata;

import org.springframework.stereotype.Component;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.EntityFieldUtils;
import ru.datamart.prostore.query.execution.plugin.adp.base.dto.metadata.AdpTableColumn;
import ru.datamart.prostore.query.execution.plugin.adp.base.dto.metadata.AdpTableEntity;
import ru.datamart.prostore.query.execution.plugin.adp.base.dto.metadata.AdpTables;
import ru.datamart.prostore.query.execution.plugin.adp.base.utils.AdpTypeUtil;
import ru.datamart.prostore.query.execution.plugin.api.factory.TableEntitiesFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static ru.datamart.prostore.query.execution.plugin.adp.base.Constants.*;

@Component("adpTableEntitiesFactory")
public class AdpTableEntitiesFactory implements TableEntitiesFactory<AdpTables<AdpTableEntity>> {
    public static final String TABLE_POSTFIX_DELIMITER = "_";

    private static final List<AdpTableColumn> SYSTEM_COLUMNS = Arrays.asList(
            new AdpTableColumn(SYS_FROM_ATTR, "int8", true),
            new AdpTableColumn(SYS_TO_ATTR, "int8", true),
            new AdpTableColumn(SYS_OP_ATTR, "int4", true)
    );

    @Override
    public AdpTables<AdpTableEntity> create(Entity entity, String envName) {
        return new AdpTables<>(
                createTableEntity(entity, ACTUAL_TABLE),
                createTableEntity(entity, HISTORY_TABLE),
                createTableEntity(entity, STAGING_TABLE)
        );
    }

    private AdpTableEntity createTableEntity(Entity entity, String tablePostfix) {
        AdpTableEntity tableEntity;
        List<String> pkTableColumnKeys;
        switch (tablePostfix) {
            case ACTUAL_TABLE:
            case HISTORY_TABLE:
                tableEntity = createEntity(entity, getTableName(entity, tablePostfix));
                pkTableColumnKeys = createPkKeys(entity.getFields());
                pkTableColumnKeys.add(SYS_FROM_ATTR);
                tableEntity.setPrimaryKeys(pkTableColumnKeys);
                return tableEntity;
            case STAGING_TABLE:
                tableEntity = createEntity(entity, getTableName(entity, tablePostfix), true);
                tableEntity.setPrimaryKeys(Collections.emptyList());
                return tableEntity;
            default:
                throw new DtmException(String.format("Incorrect table postfix %s", tablePostfix));
        }
    }

    private AdpTableEntity createEntity(Entity entity, String tableName) {
        return createEntity(entity, tableName, false);
    }

    private AdpTableEntity createEntity(Entity entity, String tableName, boolean overrideNullabilityOfLogicalColumns) {
        List<EntityField> entityFields = entity.getFields();
        AdpTableEntity AdpTableEntity = new AdpTableEntity();
        AdpTableEntity.setSchema(entity.getSchema());
        AdpTableEntity.setName(tableName);
        List<AdpTableColumn> columns = entityFields.stream()
                .sorted(Comparator.comparingInt(EntityField::getOrdinalPosition))
                .map(field -> transformColumn(field, overrideNullabilityOfLogicalColumns))
                .collect(Collectors.toList());
        columns.addAll(SYSTEM_COLUMNS);
        AdpTableEntity.setColumns(columns);
        return AdpTableEntity;
    }

    private String getTableName(Entity entity,
                                String tablePostfix) {
        return entity.getName() + TABLE_POSTFIX_DELIMITER + tablePostfix;
    }

    private List<String> createPkKeys(List<EntityField> entityFields) {
        return EntityFieldUtils.getPrimaryKeyList(entityFields).stream()
                .map(EntityField::getName)
                .collect(Collectors.toList());
    }

    private AdpTableColumn transformColumn(EntityField field, boolean overrideNullability) {
        return new AdpTableColumn(field.getName(), AdpTypeUtil.adpTypeFromDtmType(field), overrideNullability || field.getNullable());
    }
}
