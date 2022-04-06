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

import ru.datamart.prostore.common.configuration.core.CoreConstants;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityFieldUtils;
import ru.datamart.prostore.query.execution.plugin.adqm.ddl.configuration.properties.DdlProperties;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
public class AdqmProcessingSqlFactory {
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final String FLUSH_SQL_TEMPLATE = "SYSTEM FLUSH DISTRIBUTED %s";
    private static final String OPTIMIZE_SQL_TEMPLATE = "OPTIMIZE TABLE %s ON CLUSTER %s FINAL";
    private static final String CLOSE_VERSIONS_BY_TABLE_ACTUAL_TEMPLATE = "INSERT INTO ${table_name}_actual (${logical_columns}, sys_from, sys_to, sys_op, sys_close_date, sign)\n" +
            "  SELECT ${logical_columns}, sys_from, ${prev_sys_cn}, 0, '${now}', arrayJoin([-1, 1])\n" +
            "  FROM ${table_name}_actual\n" +
            "  WHERE sys_from < ${sys_cn} AND sys_to > ${sys_cn} AND ${pk_keys_for_in} IN (\n" +
            "    SELECT ${pk_keys}\n" +
            "    FROM ${table_name}_actual_shard\n" +
            "    WHERE sys_from = ${sys_cn}\n" +
            "  )";

    private static final String CLOSE_VERSIONS_BY_TABLE_BUFFER_TEMPLATE = "INSERT INTO ${table_name}_actual\n" +
            "  SELECT ${logical_columns}, sys_from, ${prev_sys_cn}, 1, '${now}', arrayJoin([-1, 1])\n" +
            "  FROM ${table_name}_actual\n" +
            "  WHERE sys_from < ${sys_cn} AND sys_to > ${sys_cn} AND ${pk_keys_for_in} IN (\n" +
            "    SELECT ${pk_keys}\n" +
            "    FROM ${table_name}_buffer_shard\n" +
            "  )";

    private static final String TABLE_NAME_PLACEHOLDER = "${table_name}";
    private static final String LOGICAL_COLUMNS_PLACEHOLDER = "${logical_columns}";
    private static final String PREV_SYS_CN_PLACEHOLDER = "${prev_sys_cn}";
    private static final String NOW_PLACEHOLDER = "${now}";
    private static final String PK_KEYS_PLACEHOLDER = "${pk_keys}";
    private static final String PK_KEYS_FOR_IN_PLACEHOLDER = "${pk_keys_for_in}";
    private static final String SYS_CN_PLACEHOLDER = "${sys_cn}";

    private final DdlProperties ddlProperties;
    private final SqlDialect sqlDialect;

    public AdqmProcessingSqlFactory(DdlProperties ddlProperties,
                                    @Qualifier("adqmSqlDialect") SqlDialect sqlDialect) {
        this.ddlProperties = ddlProperties;
        this.sqlDialect = sqlDialect;
    }

    public String getFlushActualSql(String env, String datamart, String entityName) {
        val actualName = String.format("%s__%s.%s_actual", env, datamart, entityName);
        return String.format(FLUSH_SQL_TEMPLATE, actualName);
    }

    public String getFlushSql(String table) {
        return String.format(FLUSH_SQL_TEMPLATE, table);
    }

    public String getOptimizeActualSql(String env, String datamart, String entityName) {
        val actualShardName = String.format("%s__%s.%s_actual_shard", env, datamart, entityName);
        return String.format(OPTIMIZE_SQL_TEMPLATE, actualShardName, ddlProperties.getCluster());
    }

    public String getOptimizeSql(String table) {
        return String.format(OPTIMIZE_SQL_TEMPLATE, table, ddlProperties.getCluster());
    }

    public String getSqlFromNodes(SqlNode... sqlNodes) {
        return Stream.of(sqlNodes)
                .map(sqlNode -> sqlNode.toSqlString(sqlDialect).getSql().replaceAll("\r\n|\r|\n", " "))
                .collect(Collectors.joining(" "));
    }

    public String getCloseVersionSqlByTableActual(String env, String datamart, Entity entity, long sysCn) {
        val tableName = String.format("%s__%s.%s", env, datamart, entity.getName());
        val fieldNames = EntityFieldUtils.getFieldNames(entity);
        val columnNames = String.join(", ", fieldNames);
        val now = LocalDateTime.now(CoreConstants.CORE_ZONE_ID).format(DATE_TIME_FORMATTER);
        val pkKeysList = EntityFieldUtils.getPkFieldNames(entity);
        val pkKeys = String.join(", ", pkKeysList);
        val pkKeysForIn = pkKeysList.size() == 1 ? pkKeys : "(" + pkKeys + ")";

        return CLOSE_VERSIONS_BY_TABLE_ACTUAL_TEMPLATE
                .replace(TABLE_NAME_PLACEHOLDER, tableName)
                .replace(LOGICAL_COLUMNS_PLACEHOLDER, columnNames)
                .replace(PREV_SYS_CN_PLACEHOLDER, Long.toString(sysCn - 1L))
                .replace(NOW_PLACEHOLDER, now)
                .replace(PK_KEYS_PLACEHOLDER, pkKeys)
                .replace(PK_KEYS_FOR_IN_PLACEHOLDER, pkKeysForIn)
                .replace(SYS_CN_PLACEHOLDER, Long.toString(sysCn));
    }

    public String getCloseVersionSqlByTableActual(String tableName, String columnNames, String pkKeys, long sysCn) {
        val now = LocalDateTime.now(CoreConstants.CORE_ZONE_ID).format(DATE_TIME_FORMATTER);
        val pkKeysForIn = pkKeys.contains(",") ? "(" + pkKeys + ")" : pkKeys;

        return CLOSE_VERSIONS_BY_TABLE_ACTUAL_TEMPLATE
                .replace(TABLE_NAME_PLACEHOLDER, tableName)
                .replace(LOGICAL_COLUMNS_PLACEHOLDER, columnNames)
                .replace(PREV_SYS_CN_PLACEHOLDER, Long.toString(sysCn - 1L))
                .replace(NOW_PLACEHOLDER, now)
                .replace(PK_KEYS_PLACEHOLDER, pkKeys)
                .replace(PK_KEYS_FOR_IN_PLACEHOLDER, pkKeysForIn)
                .replace(SYS_CN_PLACEHOLDER, Long.toString(sysCn));
    }

    public String getCloseVersionSqlByTableBuffer(String tableName, String columnNames, String pkKeys, long sysCn) {
        val now = LocalDateTime.now(CoreConstants.CORE_ZONE_ID).format(DATE_TIME_FORMATTER);
        val pkKeysForIn = pkKeys.contains(",") ? "(" + pkKeys + ")" : pkKeys;

        return CLOSE_VERSIONS_BY_TABLE_BUFFER_TEMPLATE
                .replace(TABLE_NAME_PLACEHOLDER, tableName)
                .replace(LOGICAL_COLUMNS_PLACEHOLDER, columnNames)
                .replace(PREV_SYS_CN_PLACEHOLDER, Long.toString(sysCn - 1L))
                .replace(NOW_PLACEHOLDER, now)
                .replace(PK_KEYS_PLACEHOLDER, pkKeys)
                .replace(PK_KEYS_FOR_IN_PLACEHOLDER, pkKeysForIn)
                .replace(SYS_CN_PLACEHOLDER, Long.toString(sysCn));
    }
}
