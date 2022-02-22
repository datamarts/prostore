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
package ru.datamart.prostore.query.execution.plugin.adb.dml.service;

import io.vertx.core.Future;
import lombok.val;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityFieldUtils;
import ru.datamart.prostore.query.execution.plugin.adb.base.factory.Constants;
import ru.datamart.prostore.query.execution.plugin.adb.dml.dto.UpsertTransferRequest;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.request.UpsertValuesRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.UpsertValuesService;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static ru.datamart.prostore.query.calcite.core.util.SqlNodeTemplates.*;
import static ru.datamart.prostore.query.execution.plugin.api.dml.LlwUtils.*;

@Service("adbUpsertValuesService")
public class AdbUpsertValuesService implements UpsertValuesService {
    private static final SqlLiteral ZERO_SYS_OP = longLiteral(0);
    private static final List<SqlLiteral> SYSTEM_ROW_VALUES = singletonList(ZERO_SYS_OP);
    private static final SqlIdentifier SYS_OP_IDENTIFIER = identifier("sys_op");
    private static final List<SqlIdentifier> SYSTEM_COLUMNS = singletonList(SYS_OP_IDENTIFIER);
    private final SqlDialect sqlDialect;
    private final DatabaseExecutor executor;
    private final AdbUpsertDataTransferService dataTransferService;

    public AdbUpsertValuesService(@Qualifier("adbSqlDialect") SqlDialect sqlDialect,
                                  DatabaseExecutor executor,
                                  AdbUpsertDataTransferService dataTransferService) {
        this.sqlDialect = sqlDialect;
        this.executor = executor;
        this.dataTransferService = dataTransferService;
    }

    @Override
    public Future<Void> execute(UpsertValuesRequest request) {
        return Future.future(promise -> {
            val source = (SqlCall) request.getQuery().getSource();
            val logicalFields = getFilteredLogicalFields(request.getEntity(), request.getQuery().getTargetColumnList());
            val actualColumnList = request.getQuery().getTargetColumnList() == null ? null : getExtendedColumns(logicalFields, SYSTEM_COLUMNS);
            val newValues = replaceDynamicParams(getExtendRowsOfValues(source, SYSTEM_ROW_VALUES, actualColumnList == null));
            val actualInsert = new SqlInsert(SqlParserPos.ZERO, SqlNodeList.EMPTY, getStagingIdentifier(request), newValues, actualColumnList);
            val sql = actualInsert.toSqlString(sqlDialect).getSql();
            executor.executeWithParams(sql, request.getParameters(), emptyList())
                    .compose(ignored -> executeTransfer(request, getColumnList(actualColumnList, request.getEntity())))
                    .onComplete(promise);
        });
    }

    private List<String> getColumnList(SqlNodeList queryColumnList, Entity entity) {
        if (queryColumnList != null) {
            List<SqlNode> nodeList = queryColumnList.getList();
            if (nodeList != null) {
                return nodeList.stream()
                        .map(sqlNode -> {
                            val identifier = (SqlIdentifier) sqlNode;
                            return identifier.getSimple();
                        })
                        .collect(Collectors.toList());
            }
        }

        return EntityFieldUtils.getFieldNames(entity);
    }

    private Future<Void> executeTransfer(UpsertValuesRequest request, List<String> columnList) {
        val upsertTransferRequest = UpsertTransferRequest.builder()
                .sysCn(request.getSysCn())
                .entity(request.getEntity())
                .targetColumnList(columnList)
                .build();
        return dataTransferService.transfer(upsertTransferRequest);
    }

    private SqlNode getStagingIdentifier(UpsertValuesRequest request) {
        return identifier(request.getDatamartMnemonic(), request.getEntity().getName() + Constants.STAGING_TABLE_SUFFIX);
    }
}
