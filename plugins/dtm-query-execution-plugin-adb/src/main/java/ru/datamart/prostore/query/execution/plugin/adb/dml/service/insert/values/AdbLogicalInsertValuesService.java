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
package ru.datamart.prostore.query.execution.plugin.adb.dml.service.insert.values;

import io.vertx.core.Future;
import lombok.val;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.common.model.ddl.EntityFieldUtils;
import ru.datamart.prostore.query.execution.plugin.adb.base.factory.Constants;
import ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.dto.TransferDataRequest;
import ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.service.executor.AdbMppwDataTransferService;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.request.InsertValuesRequest;

import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static ru.datamart.prostore.query.calcite.core.util.SqlNodeTemplates.identifier;
import static ru.datamart.prostore.query.calcite.core.util.SqlNodeTemplates.longLiteral;
import static ru.datamart.prostore.query.execution.plugin.api.dml.LlwUtils.*;
import static ru.datamart.prostore.query.execution.plugin.api.dml.LlwUtils.getExtendedColumns;

@Service
public class AdbLogicalInsertValuesService {
    private static final SqlLiteral ZERO_SYS_OP = longLiteral(0);
    private static final List<SqlLiteral> SYSTEM_ROW_VALUES = singletonList(ZERO_SYS_OP);
    private static final SqlIdentifier SYS_OP_IDENTIFIER = identifier("sys_op");
    private static final List<SqlIdentifier> SYSTEM_COLUMNS = singletonList(SYS_OP_IDENTIFIER);

    private final SqlDialect sqlDialect;
    private final DatabaseExecutor executor;
    private final AdbMppwDataTransferService dataTransferService;

    public AdbLogicalInsertValuesService(@Qualifier("adbSqlDialect") SqlDialect sqlDialect,
                                         DatabaseExecutor executor,
                                         AdbMppwDataTransferService dataTransferService) {
        this.sqlDialect = sqlDialect;
        this.executor = executor;
        this.dataTransferService = dataTransferService;
    }

    public Future<Void> execute(InsertValuesRequest request) {
        return Future.future(promise -> {
            val source = (SqlCall) request.getQuery().getSource();
            val logicalFields = getFilteredLogicalFields(request.getEntity(), request.getQuery().getTargetColumnList());
            val newValues = replaceDynamicParams(getExtendRowsOfValues(source, logicalFields, SYSTEM_ROW_VALUES));
            val actualColumnList = getExtendedColumns(logicalFields, SYSTEM_COLUMNS);
            val actualInsert = new SqlInsert(SqlParserPos.ZERO, SqlNodeList.EMPTY, getStagingIdentifier(request), newValues, actualColumnList);
            val sql = actualInsert.toSqlString(sqlDialect).getSql();
            executor.executeWithParams(sql, request.getParameters(), emptyList())
                    .compose(ignored -> executeTransfer(request))
                    .onComplete(promise);
        });
    }

    private Future<Void> executeTransfer(InsertValuesRequest request) {
        val transferDataRequest = TransferDataRequest.builder()
                .datamart(request.getDatamartMnemonic())
                .hotDelta(request.getSysCn())
                .tableName(request.getEntity().getName())
                .columnList(EntityFieldUtils.getFieldNames(request.getEntity()))
                .keyColumnList(EntityFieldUtils.getPkFieldNames(request.getEntity()))
                .build();
        return dataTransferService.execute(transferDataRequest);
    }

    private SqlNode getStagingIdentifier(InsertValuesRequest request) {
        return identifier(request.getDatamartMnemonic(), request.getEntity().getName() + Constants.STAGING_TABLE_SUFFIX);
    }

}
