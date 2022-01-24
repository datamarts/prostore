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
package io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.factory.impl;

import io.arenadata.dtm.common.plugin.sql.PreparedStatementRequest;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.dto.AdbKafkaMppwTransferRequest;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.dto.TransferDataRequest;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.factory.MppwRequestFactory;

import java.util.Arrays;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.plugin.adb.base.factory.Constants.SYS_FROM_ATTR;

public class MppwWithHistoryTableRequestFactory extends MppwRequestFactory {

    private static final String INSERT_HISTORY_SQL = "INSERT INTO %s.%s_history (%s)\n" +
        "SELECT %s\n" +
        "FROM %s.%s_actual a\n" +
        "         INNER JOIN (SELECT DISTINCT * FROM %s.%s_staging) s ON\n" +
        "    %s";

    private static final String DELETE_ACTUAL_SQL = "DELETE\n" +
        "FROM %s.%s_actual a USING %s.%s_staging s\n" +
        "WHERE %s";

    private static final String INSERT_ACTUAL_SQL = "INSERT INTO %s.%s_actual (%s)\n" +
        "SELECT DISTINCT %s\n" +
        "FROM %s.%s_staging\n" +
        "WHERE %s.%s_staging.sys_op <> 1";

    private static final String TRUNCATE_STAGING_SQL = "TRUNCATE %s.%s_staging";

    @Override
    public AdbKafkaMppwTransferRequest create(TransferDataRequest request) {
        String actualColumns = request.getColumnList().stream()
                .map(s -> "a." + s)
                .map(cn -> ("a.sys_to".equals(cn)) ? request.getHotDelta() - 1 + "" : cn)
                .map(cn -> ("a.sys_op".equals(cn)) ? "s.sys_op" : cn)
                .collect(Collectors.joining(","));
        String joinConditionInsert = request.getKeyColumnList().stream()
                .filter(columnName -> !SYS_FROM_ATTR.equals(columnName))
                .map(key -> "s." + key + "=" + "a." + key)
                .collect(Collectors.joining(" AND "));

        String joinConditionDelete = request.getKeyColumnList().stream()
            .filter(columnName -> !SYS_FROM_ATTR.equals(columnName))
            .map(key -> "a." + key + "=" + "s." + key)
            .collect(Collectors.joining(" AND "));

        String columnsString = String.join(",", request.getColumnList());

        String insertHistorySql = String.format(INSERT_HISTORY_SQL,
            request.getDatamart(), request.getTableName(), columnsString,
            actualColumns,
            request.getDatamart(), request.getTableName(),
            request.getDatamart(), request.getTableName(),
            joinConditionInsert);

        String deleteActualSql = String.format(DELETE_ACTUAL_SQL,
            request.getDatamart(), request.getTableName(), request.getDatamart(), request.getTableName(),
            joinConditionDelete);

        String stagingColumnsString = String.join(",", getStagingColumnList(request));

        String insertActualSql = String.format(INSERT_ACTUAL_SQL,
            request.getDatamart(), request.getTableName(), columnsString,
            stagingColumnsString,
            request.getDatamart(), request.getTableName(),
            request.getDatamart(), request.getTableName());

        String truncateStagingSql = String.format(TRUNCATE_STAGING_SQL,
            request.getDatamart(), request.getTableName());

        return new AdbKafkaMppwTransferRequest(
            Arrays.asList(
                PreparedStatementRequest.onlySql(insertHistorySql),
                PreparedStatementRequest.onlySql(deleteActualSql)),
            Arrays.asList(
                PreparedStatementRequest.onlySql(insertActualSql),
                PreparedStatementRequest.onlySql(truncateStagingSql)
            )
        );
    }
}
