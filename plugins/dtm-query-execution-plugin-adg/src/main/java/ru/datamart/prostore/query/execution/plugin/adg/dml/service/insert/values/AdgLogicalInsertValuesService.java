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
package ru.datamart.prostore.query.execution.plugin.adg.dml.service.insert.values;

import io.vertx.core.Future;
import lombok.val;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.query.calcite.core.util.SqlNodeTemplates;
import ru.datamart.prostore.query.execution.plugin.adg.base.factory.AdgHelperTableNamesFactory;
import ru.datamart.prostore.query.execution.plugin.adg.base.model.cartridge.request.AdgTransferDataEtlRequest;
import ru.datamart.prostore.query.execution.plugin.adg.base.service.client.AdgCartridgeClient;
import ru.datamart.prostore.query.execution.plugin.adg.query.service.AdgQueryExecutorService;
import ru.datamart.prostore.query.execution.plugin.api.dml.LlwUtils;
import ru.datamart.prostore.query.execution.plugin.api.request.InsertValuesRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.PluginSpecificLiteralConverter;

import java.util.List;

import static java.util.Collections.singletonList;
import static ru.datamart.prostore.query.calcite.core.util.SqlNodeTemplates.identifier;

@Service
public class AdgLogicalInsertValuesService {
    private static final SqlLiteral ZERO_SYS_OP = SqlNodeTemplates.longLiteral(0);
    private static final List<SqlLiteral> SYSTEM_ROW_VALUES = singletonList(ZERO_SYS_OP);
    private static final SqlIdentifier SYS_OP_IDENTIFIER = identifier("sys_op");
    private static final List<SqlIdentifier> SYSTEM_COLUMNS = singletonList(SYS_OP_IDENTIFIER);
    private final SqlDialect sqlDialect;
    private final AdgQueryExecutorService executor;
    private final AdgCartridgeClient cartridgeClient;
    private final AdgHelperTableNamesFactory adgHelperTableNamesFactory;
    private final PluginSpecificLiteralConverter specificLiteralConverter;

    public AdgLogicalInsertValuesService(@Qualifier("adgSqlDialect") SqlDialect sqlDialect,
                                         AdgQueryExecutorService executor,
                                         AdgCartridgeClient cartridgeClient,
                                         AdgHelperTableNamesFactory adgHelperTableNamesFactory,
                                         @Qualifier("adgPluginSpecificLiteralConverter") PluginSpecificLiteralConverter specificLiteralConverter) {
        this.sqlDialect = sqlDialect;
        this.executor = executor;
        this.cartridgeClient = cartridgeClient;
        this.adgHelperTableNamesFactory = adgHelperTableNamesFactory;
        this.specificLiteralConverter = specificLiteralConverter;
    }

    public Future<Void> execute(InsertValuesRequest request) {
        return Future.future(promise -> {
            val source = (SqlCall) request.getQuery().getSource();
            val logicalFields = LlwUtils.getFilteredLogicalFields(request.getEntity(), request.getQuery().getTargetColumnList());
            val newValues = LlwUtils.getExtendRowsOfValues(source, logicalFields, SYSTEM_ROW_VALUES, transformEntry -> specificLiteralConverter.convert(transformEntry.getSqlNode(), transformEntry.getSqlTypeName()));
            val actualColumnList = LlwUtils.getExtendedColumns(logicalFields, SYSTEM_COLUMNS);
            val actualInsert = new SqlInsert(SqlParserPos.ZERO, SqlNodeList.EMPTY, getStagingIdentifier(request), newValues, actualColumnList);
            val sql = actualInsert.toSqlString(sqlDialect).getSql();
            executor.executeUpdate(sql, request.getParameters())
                    .compose(ignored -> executeTransfer(request))
                    .onComplete(promise);
        });
    }

    private Future<Void> executeTransfer(InsertValuesRequest request) {
        val tableNames = adgHelperTableNamesFactory.create(
                request.getEnvName(),
                request.getDatamartMnemonic(),
                request.getEntity().getName());
        val transferDataRequest = new AdgTransferDataEtlRequest(tableNames, request.getSysCn());
        return cartridgeClient.transferDataToScdTable(transferDataRequest);
    }

    private SqlNode getStagingIdentifier(InsertValuesRequest request) {
        return SqlNodeTemplates.identifier(String.format("%s__%s__%s_staging", request.getEnvName(), request.getDatamartMnemonic(), request.getEntity().getName()));
    }

}
