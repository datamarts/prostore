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
package ru.datamart.prostore.query.execution.plugin.adb.dml.service.upsert.values;

import io.vertx.core.Future;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.common.model.ddl.EntityFieldUtils;
import ru.datamart.prostore.common.plugin.sql.PreparedStatementRequest;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.request.UpsertValuesRequest;

import java.util.ArrayList;

import static ru.datamart.prostore.query.execution.plugin.adb.dml.factory.AdbStandaloneUpsertSqlFactory.*;
import static ru.datamart.prostore.query.execution.plugin.api.dml.LlwUtils.getFilteredLogicalFields;
import static ru.datamart.prostore.query.execution.plugin.api.dml.LlwUtils.replaceDynamicParams;


@Service
public class AdbStandaloneUpsertValuesService {
    private final SqlDialect sqlDialect;
    private final DatabaseExecutor executor;

    public AdbStandaloneUpsertValuesService(@Qualifier("adbSqlDialect") SqlDialect sqlDialect,
                                            DatabaseExecutor executor) {
        this.sqlDialect = sqlDialect;
        this.executor = executor;
    }

    public Future<Void> execute(UpsertValuesRequest request) {
        return Future.future(promise -> {
            val entity = request.getEntity();
            val destination = entity.getExternalTableLocationPath();
            val targetColumns = getFilteredLogicalFields(entity, request.getQuery().getTargetColumnList());
            val newValues = replaceDynamicParams(request.getQuery().getSource());
            val tempTable = tempTableName(request.getRequestId());
            val entityPk = EntityFieldUtils.getPkFieldNames(entity);
            val entitySk = EntityFieldUtils.getShardingKeyNames(entity);

            val createTempTableSql = createTempTableSql(targetColumns, entitySk, tempTable);
            val insertTempSql = insertIntoTempSql(targetColumns, tempTable, newValues.toSqlString(sqlDialect).toString());
            val updateSql = updateSql(targetColumns, entityPk, destination, tempTable);
            val insertDestinationSql = insertIntoDestinationSql(targetColumns, destination, tempTable, entityPk);

            val preparedStatementRequests = new ArrayList<PreparedStatementRequest>(4);
            preparedStatementRequests.add(PreparedStatementRequest.onlySql(createTempTableSql));
            preparedStatementRequests.add(new PreparedStatementRequest(insertTempSql, request.getParameters()));
            if (updateSql != null) {
                preparedStatementRequests.add(PreparedStatementRequest.onlySql(updateSql));
            }
            preparedStatementRequests.add(PreparedStatementRequest.onlySql(insertDestinationSql));

            executor.executeInTransaction(preparedStatementRequests)
                    .onComplete(promise);
        });
    }

}
