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
package ru.datamart.prostore.query.execution.plugin.adb.base.factory.metadata;

import org.springframework.stereotype.Service;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.EntityFieldUtils;
import ru.datamart.prostore.query.execution.plugin.adb.base.dto.metadata.AdbTableColumn;
import ru.datamart.prostore.query.execution.plugin.adb.base.dto.metadata.AdbTableEntity;
import ru.datamart.prostore.query.execution.plugin.adb.base.dto.metadata.AdbTables;
import ru.datamart.prostore.query.execution.plugin.adb.base.utils.AdbTypeUtil;
import ru.datamart.prostore.query.execution.plugin.api.factory.TableEntitiesFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static ru.datamart.prostore.query.execution.plugin.adb.base.factory.Constants.*;

@Service("adbTableEntitiesFactory")
public class AdbTableEntitiesFactory implements TableEntitiesFactory<AdbTables<AdbTableEntity>> {
    public static final String TABLE_POSTFIX_DELIMITER = "_";

    private static final List<AdbTableColumn> SYSTEM_COLUMNS = Arrays.asList(
            new AdbTableColumn(SYS_FROM_ATTR, "int8", true),
            new AdbTableColumn(SYS_TO_ATTR, "int8", true),
            new AdbTableColumn(SYS_OP_ATTR, "int4", true)
    );

    @Override
    public AdbTables<AdbTableEntity> create(Entity entity, String envName) {
        return new AdbTables<>(
                createTableEntity(entity, AdbTables.ACTUAL_TABLE_POSTFIX),
                createTableEntity(entity, AdbTables.HISTORY_TABLE_POSTFIX),
                createTableEntity(entity, AdbTables.STAGING_TABLE_POSTFIX)
        );
    }

    private AdbTableEntity createTableEntity(Entity entity, String tablePostfix) {
        AdbTableEntity tableEntity;
        List<String> pkTableColumnKeys;
        switch (tablePostfix) {
            case AdbTables.ACTUAL_TABLE_POSTFIX:
            case AdbTables.HISTORY_TABLE_POSTFIX:
                tableEntity = createEntity(entity, getTableName(entity, tablePostfix));
                pkTableColumnKeys = createPkKeys(entity.getFields());
                pkTableColumnKeys.add(SYS_FROM_ATTR);
                tableEntity.setPrimaryKeys(pkTableColumnKeys);
                return tableEntity;
            case AdbTables.STAGING_TABLE_POSTFIX:
                tableEntity = createEntity(entity, getTableName(entity, tablePostfix), true);
                tableEntity.setPrimaryKeys(Collections.emptyList());
                return tableEntity;
            default:
                throw new DtmException(String.format("Incorrect table postfix %s", tablePostfix));
        }
    }

    private AdbTableEntity createEntity(Entity entity, String tableName) {
        return createEntity(entity, tableName, false);
    }

    private AdbTableEntity createEntity(Entity entity, String tableName, boolean overrideNullabilityOfLogicalColumns) {
        List<EntityField> entityFields = entity.getFields();
        AdbTableEntity adbTableEntity = new AdbTableEntity();
        adbTableEntity.setSchema(entity.getSchema());
        adbTableEntity.setName(tableName);
        List<AdbTableColumn> columns = entityFields.stream()
                .sorted(Comparator.comparingInt(EntityField::getOrdinalPosition))
                .map(field -> transformColumn(field, overrideNullabilityOfLogicalColumns))
                .collect(Collectors.toList());
        columns.addAll(SYSTEM_COLUMNS);
        adbTableEntity.setColumns(columns);
        adbTableEntity.setShardingKeys(EntityFieldUtils.getShardingKeyList(entityFields).stream()
                .map(EntityField::getName)
                .collect(Collectors.toList()));
        return adbTableEntity;
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

    private AdbTableColumn transformColumn(EntityField field, boolean overrideNullability) {
        return new AdbTableColumn(field.getName(), AdbTypeUtil.adbTypeFromDtmType(field), overrideNullability || field.getNullable());
    }
}
