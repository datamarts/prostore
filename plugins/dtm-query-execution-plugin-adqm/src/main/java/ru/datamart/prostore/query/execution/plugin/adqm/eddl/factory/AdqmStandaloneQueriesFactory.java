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
package ru.datamart.prostore.query.execution.plugin.adqm.eddl.factory;

import lombok.val;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.EntityFieldUtils;
import ru.datamart.prostore.query.execution.plugin.adqm.base.utils.AdqmDdlUtil;
import ru.datamart.prostore.query.execution.plugin.adqm.ddl.configuration.properties.DdlProperties;

import java.util.Comparator;
import java.util.stream.Collectors;

import static ru.datamart.prostore.query.execution.plugin.adqm.base.utils.AdqmDdlUtil.NOT_NULLABLE_FIELD;
import static ru.datamart.prostore.query.execution.plugin.adqm.base.utils.AdqmDdlUtil.NULLABLE_FIELD;

@Service
public class AdqmStandaloneQueriesFactory {
    private static final String CREATE_SHARD_TABLE =
            "CREATE TABLE %s_shard ON CLUSTER %s\n" +
                    "(%s)\n" +
                    "ENGINE = MergeTree()\n" +
                    "ORDER BY (%s)";

    private static final String CREATE_DISTRIBUTED_TABLE =
            "CREATE TABLE %s ON CLUSTER %s AS %s_shard \n" +
                    "ENGINE = Distributed(%s, %s, %s_shard, %s)";

    private static final String DROP_DISTRIBUTED_TABLE = "DROP TABLE %s ON CLUSTER %s;";
    private static final String DROP_SHARD_TABLE = "DROP TABLE %s_shard ON CLUSTER %s;";

    private final DdlProperties ddlProperties;

    public AdqmStandaloneQueriesFactory(DdlProperties ddlProperties) {
        this.ddlProperties = ddlProperties;
    }

    public String createShardQuery(Entity entity) {
        val cluster = ddlProperties.getCluster();
        val columnList = entity.getFields().stream()
                .sorted(Comparator.comparingInt(EntityField::getOrdinalPosition))
                .map(field -> String.format(field.getNullable() ? NULLABLE_FIELD : NOT_NULLABLE_FIELD,
                        field.getName(), AdqmDdlUtil.classTypeToNative(field.getType())))
                .collect(Collectors.joining(", "));
        val pkFields = EntityFieldUtils.getPrimaryKeyList(entity.getFields()).stream()
                .map(EntityField::getName)
                .collect(Collectors.joining(", "));
        return String.format(CREATE_SHARD_TABLE, entity.getExternalTableLocationPath(), cluster, columnList, pkFields);
    }

    public String createDistributedQuery(Entity entity) {
        val cluster = ddlProperties.getCluster();
        val shardingKeys = EntityFieldUtils.getShardingKeyNames(entity);
        val shardingKeyExpr = ddlProperties.getShardingKeyExpr().getValue(shardingKeys);
        val locationPath = entity.getExternalTableLocationPath();
        val dbAndTableName = locationPath.split("\\.");
        return String.format(CREATE_DISTRIBUTED_TABLE, locationPath, cluster, locationPath, cluster,
                dbAndTableName[0], dbAndTableName[1], shardingKeyExpr);
    }

    public String dropDistributedTable(String externalLocationPath) {
        return String.format(DROP_DISTRIBUTED_TABLE, externalLocationPath, ddlProperties.getCluster());
    }

    public String dropShardTable(String externalLocationPath) {
        return String.format(DROP_SHARD_TABLE, externalLocationPath, ddlProperties.getCluster());
    }
}
