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
package ru.datamart.prostore.query.execution.plugin.adp.dml;

import io.vertx.core.Future;
import lombok.val;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.common.model.ddl.EntityFieldUtils;
import ru.datamart.prostore.query.calcite.core.util.SqlNodeTemplates;
import ru.datamart.prostore.query.execution.plugin.adp.base.Constants;
import ru.datamart.prostore.query.execution.plugin.adp.db.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.adp.mppw.dto.AdpTransferDataRequest;
import ru.datamart.prostore.query.execution.plugin.adp.mppw.transfer.AdpTransferDataService;
import ru.datamart.prostore.query.execution.plugin.api.request.InsertValuesRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.InsertValuesService;

import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static ru.datamart.prostore.query.calcite.core.util.SqlNodeTemplates.identifier;
import static ru.datamart.prostore.query.calcite.core.util.SqlNodeTemplates.longLiteral;
import static ru.datamart.prostore.query.execution.plugin.api.dml.LlwUtils.*;

@Service("adpInsertValuesService")
public class AdpInsertValuesService implements InsertValuesService {
    private static final SqlLiteral ZERO_SYS_OP = longLiteral(0);
    private static final List<SqlLiteral> SYSTEM_ROW_VALUES = singletonList(ZERO_SYS_OP);
    private static final SqlIdentifier SYS_OP_IDENTIFIER = identifier("sys_op");
    private static final List<SqlIdentifier> SYSTEM_COLUMNS = singletonList(SYS_OP_IDENTIFIER);
    private final SqlDialect sqlDialect;
    private final DatabaseExecutor executor;
    private final AdpTransferDataService dataTransferService;

    public AdpInsertValuesService(@Qualifier("adpSqlDialect") SqlDialect sqlDialect,
                                  DatabaseExecutor executor,
                                  AdpTransferDataService dataTransferService) {
        this.sqlDialect = sqlDialect;
        this.executor = executor;
        this.dataTransferService = dataTransferService;
    }

    @Override
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
        val transferDataRequest = AdpTransferDataRequest.builder()
                .datamart(request.getDatamartMnemonic())
                .sysCn(request.getSysCn())
                .tableName(request.getEntity().getName())
                .allFields(EntityFieldUtils.getFieldNames(request.getEntity()))
                .primaryKeys(EntityFieldUtils.getPkFieldNames(request.getEntity()))
                .build();
        return dataTransferService.transferData(transferDataRequest);
    }

    private SqlNode getStagingIdentifier(InsertValuesRequest request) {
        return SqlNodeTemplates.identifier(request.getDatamartMnemonic(), String.format("%s_%s", request.getEntity().getName(), Constants.STAGING_TABLE));
    }
}
