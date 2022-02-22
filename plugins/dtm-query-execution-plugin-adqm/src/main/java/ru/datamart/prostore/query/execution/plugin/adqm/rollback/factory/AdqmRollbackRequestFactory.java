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
package ru.datamart.prostore.query.execution.plugin.adqm.rollback.factory;

import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.plugin.sql.PreparedStatementRequest;
import ru.datamart.prostore.query.execution.plugin.adqm.base.utils.Constants;
import ru.datamart.prostore.query.execution.plugin.adqm.ddl.configuration.properties.DdlProperties;
import ru.datamart.prostore.query.execution.plugin.adqm.factory.AdqmProcessingSqlFactory;
import ru.datamart.prostore.query.execution.plugin.adqm.rollback.dto.AdqmRollbackRequest;
import ru.datamart.prostore.query.execution.plugin.api.dto.RollbackRequest;
import ru.datamart.prostore.query.execution.plugin.api.factory.RollbackRequestFactory;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
public class AdqmRollbackRequestFactory implements RollbackRequestFactory<AdqmRollbackRequest> {

    private static final String DROP_TABLE_TEMPLATE = "DROP TABLE IF EXISTS %s.%s_%s ON CLUSTER %s";
    private static final String INSERT_INTO_TEMPLATE = "INSERT INTO <dbname>.<tablename>_actual\n" +
        "  SELECT <fields>, sys_from, sys_to, sys_op, sys_close_date, -1\n" +
        "  FROM <dbname>.<tablename>_actual FINAL\n" +
        "  WHERE sys_from = <sys_cn> AND sign = 1\n" +
        "  UNION ALL\n" +
        "  SELECT <fields>, sys_from, toInt64(<maxLong>) AS sys_to, 0 AS sys_op, toDateTime('9999-12-31 00:00:00') AS sys_close_date, arrayJoin([-1, 1])\n" +
        "  FROM <dbname>.<tablename>_actual a FINAL\n" +
        "  WHERE a.sys_to = <prev_sys_cn> AND sign = 1";

    private final DdlProperties ddlProperties;
    private final AdqmProcessingSqlFactory adqmProcessingSqlFactory;

    @Override
    public AdqmRollbackRequest create(RollbackRequest rollbackRequest) {
        val cluster = ddlProperties.getCluster();
        Entity entity = rollbackRequest.getEntity();
        val entityName = entity.getName();
        val dbName = Constants.getDbName(rollbackRequest.getEnvName(), rollbackRequest.getDatamartMnemonic());
        val sysCn = rollbackRequest.getSysCn();
        return new AdqmRollbackRequest(
            Arrays.asList(
                    PreparedStatementRequest.onlySql(getDropTableSql(dbName, entityName, "ext_shard", cluster)),
                    PreparedStatementRequest.onlySql(getDropTableSql(dbName, entityName, "actual_loader_shard", cluster)),
                    PreparedStatementRequest.onlySql(getDropTableSql(dbName, entityName, "buffer_loader_shard", cluster)),
                    PreparedStatementRequest.onlySql(getDropTableSql(dbName, entityName, "buffer", cluster)),
                    PreparedStatementRequest.onlySql(getDropTableSql(dbName, entityName, "buffer_shard", cluster)),
                    PreparedStatementRequest.onlySql(adqmProcessingSqlFactory.getFlushActualSql(rollbackRequest.getEnvName(), rollbackRequest.getDatamartMnemonic(), entityName)),
                    PreparedStatementRequest.onlySql(gerInsertSql(dbName, entity, sysCn)),
                    PreparedStatementRequest.onlySql(adqmProcessingSqlFactory.getFlushActualSql(rollbackRequest.getEnvName(), rollbackRequest.getDatamartMnemonic(), entityName)),
                    PreparedStatementRequest.onlySql(adqmProcessingSqlFactory.getOptimizeActualSql(rollbackRequest.getEnvName(), rollbackRequest.getDatamartMnemonic(), entityName))
            )
        );
    }

    private String gerInsertSql(String datamart, Entity entity, long sysCn) {
        val fields = entity.getFields().stream()
            .map(EntityField::getName)
            .collect(Collectors.joining(","));
        return INSERT_INTO_TEMPLATE
            .replace("<dbname>", datamart)
            .replace("<tablename>", entity.getName())
            .replace("<fields>", fields)
            .replace("<maxLong>", String.valueOf(Long.MAX_VALUE))
            .replace("<sys_cn>", String.valueOf(sysCn))
            .replace("<prev_sys_cn>", String.valueOf(sysCn - 1));
    }

    private String getDropTableSql(String datamart, String entity, String tableSuffix, String cluster) {
        return String.format(DROP_TABLE_TEMPLATE, datamart, entity, tableSuffix, cluster);
    }
}
