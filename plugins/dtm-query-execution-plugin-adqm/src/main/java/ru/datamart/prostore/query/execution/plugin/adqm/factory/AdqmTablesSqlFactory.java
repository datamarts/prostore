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
package ru.datamart.prostore.query.execution.plugin.adqm.factory;

import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.EntityFieldUtils;
import ru.datamart.prostore.query.execution.plugin.adqm.base.utils.AdqmDdlUtil;
import ru.datamart.prostore.query.execution.plugin.adqm.ddl.configuration.properties.DdlProperties;
import lombok.val;
import org.springframework.stereotype.Service;

import java.util.stream.Collectors;

import static ru.datamart.prostore.query.execution.plugin.adqm.base.utils.Constants.*;
import static java.lang.String.format;

@Service
public class AdqmTablesSqlFactory {

    private static final String BUFFER_LOADER_SHARD_TEMPLATE =
            "CREATE MATERIALIZED VIEW IF NOT EXISTS %s_buffer_loader_shard ON CLUSTER %s TO %s\n" +
            "  AS SELECT %s FROM %s WHERE sys_op = 1";
    private static final String ACTUAL_LOADER_SHARD_TEMPLATE =
            "CREATE MATERIALIZED VIEW IF NOT EXISTS %s_actual_loader_shard ON CLUSTER %s TO %s\n" +
            "AS SELECT %s, %d AS sys_from, 9223372036854775807 as sys_to, 0 as sys_op_load, '9999-12-31 00:00:00' as sys_close_date, 1 AS sign " +
            " FROM %s es WHERE es.sys_op <> 1";
    private static final String BUFFER_SHARD_TEMPLATE =
            "CREATE TABLE %s_buffer_shard ON CLUSTER %s (%s) ENGINE = MergeTree ORDER BY (%s)";
    private static final String BUFFER_TEMPLATE =
            "CREATE TABLE %s_buffer ON CLUSTER %s AS %s_buffer_shard ENGINE = Distributed (%s, %s__%s, %s_buffer_shard, %s)";
    private static final String DROP_TABLE_BUFFER_TEMPLATE = "DROP TABLE IF EXISTS %s__%s.%s_buffer ON CLUSTER %s";
    private static final String DROP_TABLE_BUFFER_SHARD_TEMPLATE = "DROP TABLE IF EXISTS %s__%s.%s_buffer_shard ON CLUSTER %s";
    private static final String TABLE_NAME_TEMPLATE = "%s__%s.%s";

    private final DdlProperties ddlProperties;

    public AdqmTablesSqlFactory(DdlProperties ddlProperties) {
        this.ddlProperties = ddlProperties;
    }

    public String getCreateBufferLoaderShardSql(String env, String datamart, Entity entity) {
        val primaryKeys = String.join(", ", EntityFieldUtils.getPkFieldNames(entity));
        val tableName = getTableName(env, datamart, entity);
        return format(BUFFER_LOADER_SHARD_TEMPLATE, tableName, ddlProperties.getCluster(), tableName + BUFFER_POSTFIX, primaryKeys, tableName + EXT_SHARD_POSTFIX);
    }

    public String getCreateActualLoaderShardSql(String env, String datamart, Entity entity, long sysCn) {
        val columns = EntityFieldUtils.getFieldNames(entity).stream()
                .map(c -> "es." + c)
                .collect(Collectors.joining(", "));
        val tableName = getTableName(env, datamart, entity);
        return format(ACTUAL_LOADER_SHARD_TEMPLATE, tableName, ddlProperties.getCluster(),
                tableName + ACTUAL_POSTFIX,
                columns, sysCn,
                tableName + EXT_SHARD_POSTFIX);
    }

    public String getCreateBufferShardSql(String env, String datamart, Entity entity) {
        val primaryKeyList = EntityFieldUtils.getPrimaryKeyList(entity.getFields());
        val primaryKeysWithType = primaryKeyList.stream()
                .map(entityField -> entityField.getName() + " " + AdqmDdlUtil.classTypeToNative(entityField.getType()))
                .collect(Collectors.joining(","));
        val primaryKeys = primaryKeyList.stream().map(EntityField::getName).collect(Collectors.joining(","));
        val tableName = getTableName(env, datamart, entity);
        return format(BUFFER_SHARD_TEMPLATE, tableName, ddlProperties.getCluster(), primaryKeysWithType, primaryKeys);
    }

    public String getCreateBufferSql(String env, String datamart, Entity entity) {
        val tableName = getTableName(env, datamart, entity);
        val cluster = ddlProperties.getCluster();
        val shardingKeys = EntityFieldUtils.getShardingKeyNames(entity);
        val shardingExpr = ddlProperties.getShardingKeyExpr().getValue(shardingKeys);
        return format(BUFFER_TEMPLATE, tableName, cluster, tableName, cluster, env, datamart, entity.getName(), shardingExpr);
    }

    public String getDropBufferSql(String env, String datamart, Entity entity) {
        return format(DROP_TABLE_BUFFER_TEMPLATE, env, datamart, entity.getName(), ddlProperties.getCluster());
    }

    public String getDropBufferShardSql(String env, String datamart, Entity entity) {
        return format(DROP_TABLE_BUFFER_SHARD_TEMPLATE, env, datamart, entity.getName(), ddlProperties.getCluster());
    }

    private String getTableName(String env, String datamart, Entity entity) {
        return format(TABLE_NAME_TEMPLATE, env, datamart, entity.getName());
    }
}
