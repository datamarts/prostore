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
package ru.datamart.prostore.query.execution.plugin.adp.ddl.factory;

import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.query.execution.plugin.adp.base.dto.metadata.AdpTableColumn;
import ru.datamart.prostore.query.execution.plugin.adp.base.dto.metadata.AdpTableEntity;
import ru.datamart.prostore.query.execution.plugin.adp.base.dto.metadata.AdpTables;
import ru.datamart.prostore.query.execution.plugin.api.factory.CreateTableQueriesFactory;
import ru.datamart.prostore.query.execution.plugin.api.factory.TableEntitiesFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Component("adpCreateTableQueriesFactory")
public class AdpCreateTableQueriesFactory implements CreateTableQueriesFactory<AdpTables<String>> {

    public static final String CREATE_PATTERN = "CREATE TABLE %s.%s (%s%s)";
    public static final String PRIMARY_KEY_PATTERN = ", constraint pk_%s primary key (%s)";

    private final TableEntitiesFactory<AdpTables<AdpTableEntity>> tableEntitiesFactory;

    public AdpCreateTableQueriesFactory(@Qualifier("adpTableEntitiesFactory") TableEntitiesFactory<AdpTables<AdpTableEntity>> tableEntitiesFactory) {
        this.tableEntitiesFactory = tableEntitiesFactory;
    }

    @Override
    public AdpTables create(Entity entity, String envName) {
        AdpTables<AdpTableEntity> tableEntities = tableEntitiesFactory.create(entity, envName);
        return new AdpTables<>(createTableQuery(tableEntities.getActual()),
                createTableQuery(tableEntities.getHistory()),
                createTableQuery(tableEntities.getStaging()));
    }

    private String createTableQuery(AdpTableEntity table) {
        return String.format(CREATE_PATTERN, table.getSchema(), table.getName(),
                getColumnsQuery(table), getPrimaryKeyQuery(table));
    }

    private String getColumnsQuery(AdpTableEntity AdpTableEntity) {
        return AdpTableEntity.getColumns().stream()
                .map(this::getColumnQuery)
                .collect(Collectors.joining(", "));
    }

    private String getColumnQuery(AdpTableColumn column) {
        return String.format("%s %s%s", column.getName(), column.getType(), column.getNullable() ? "" : " NOT NULL");
    }

    private String getPrimaryKeyQuery(AdpTableEntity AdpTableEntity) {
        List<String> primaryKeys = AdpTableEntity.getPrimaryKeys();
        String pkTableName = String.format("%s_%s", AdpTableEntity.getSchema(), AdpTableEntity.getName());
        String pkKeys = String.join(", ", primaryKeys);
        return primaryKeys.isEmpty() ? "" : String.format(PRIMARY_KEY_PATTERN, pkTableName, pkKeys);
    }

}
