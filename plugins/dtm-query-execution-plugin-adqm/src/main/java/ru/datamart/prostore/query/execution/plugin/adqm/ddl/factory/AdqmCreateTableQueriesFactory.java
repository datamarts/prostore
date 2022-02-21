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
package ru.datamart.prostore.query.execution.plugin.adqm.ddl.factory;

import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.query.execution.plugin.adqm.base.dto.metadata.AdqmTableColumn;
import ru.datamart.prostore.query.execution.plugin.adqm.base.dto.metadata.AdqmTableEntity;
import ru.datamart.prostore.query.execution.plugin.adqm.base.dto.metadata.AdqmTables;
import ru.datamart.prostore.query.execution.plugin.adqm.ddl.configuration.properties.DdlProperties;
import ru.datamart.prostore.query.execution.plugin.api.factory.CreateTableQueriesFactory;
import ru.datamart.prostore.query.execution.plugin.api.factory.TableEntitiesFactory;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

import static ru.datamart.prostore.query.execution.plugin.adqm.base.utils.AdqmDdlUtil.NOT_NULLABLE_FIELD;
import static ru.datamart.prostore.query.execution.plugin.adqm.base.utils.AdqmDdlUtil.NULLABLE_FIELD;

@Service("adqmCreateTableQueriesFactory")
public class AdqmCreateTableQueriesFactory implements CreateTableQueriesFactory<AdqmTables<String>> {

    private static final String CREATE_SHARD_TABLE_TEMPLATE =
            "CREATE TABLE %s__%s.%s ON CLUSTER %s\n" +
                    "(%s)\n" +
                    "ENGINE = CollapsingMergeTree(sign)\n" +
                    "ORDER BY (%s)";

    private static final String CREATE_DISTRIBUTED_TABLE_TEMPLATE =
            "CREATE TABLE %s__%s.%s ON CLUSTER %s\n" +
                    "(%s)\n" +
                    "Engine = Distributed(%s, %s__%s, %s, %s)";

    private final DdlProperties ddlProperties;
    private final TableEntitiesFactory<AdqmTables<AdqmTableEntity>> tableEntitiesFactory;

    @Autowired
    public AdqmCreateTableQueriesFactory(DdlProperties ddlProperties,
                                         TableEntitiesFactory<AdqmTables<AdqmTableEntity>> tableEntitiesFactory) {
        this.ddlProperties = ddlProperties;
        this.tableEntitiesFactory = tableEntitiesFactory;
    }

    @Override
    public AdqmTables<String> create(Entity entity, String envName) {
        String cluster = ddlProperties.getCluster();

        val tables = tableEntitiesFactory.create(entity, envName);
        val shard = tables.getShard();
        val distributed = tables.getDistributed();
        val shardingKeyExpr = ddlProperties.getShardingKeyExpr().getValue(distributed.getShardingKeys());
        return new AdqmTables<>(
                String.format(CREATE_SHARD_TABLE_TEMPLATE,
                        shard.getEnv(),
                        shard.getSchema(),
                        shard.getName(),
                        cluster,
                        getColumnsQuery(shard.getColumns()),
                        String.join(", ", shard.getSortedKeys())),
                String.format(CREATE_DISTRIBUTED_TABLE_TEMPLATE,
                        distributed.getEnv(),
                        distributed.getSchema(),
                        distributed.getName(),
                        cluster,
                        getColumnsQuery(distributed.getColumns()),
                        cluster,
                        distributed.getEnv(),
                        distributed.getSchema(),
                        shard.getName(),
                        shardingKeyExpr)
        );
    }

    private String getColumnsQuery(List<AdqmTableColumn> columns) {
        return columns.stream()
                .map(col -> String.format(col.getNullable() ? NULLABLE_FIELD : NOT_NULLABLE_FIELD,
                        col.getName(), col.getType()))
                .collect(Collectors.joining(", "));
    }
}
