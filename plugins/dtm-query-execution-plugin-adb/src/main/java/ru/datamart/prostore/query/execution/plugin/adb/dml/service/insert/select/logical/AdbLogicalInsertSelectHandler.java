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
package ru.datamart.prostore.query.execution.plugin.adb.dml.service.insert.select.logical;

import io.vertx.core.Future;
import lombok.val;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Util;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.common.dto.QueryParserRequest;
import ru.datamart.prostore.common.dto.QueryParserResponse;
import ru.datamart.prostore.common.model.ddl.EntityFieldUtils;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.calcite.core.service.QueryParserService;
import ru.datamart.prostore.query.calcite.core.service.QueryTemplateExtractor;
import ru.datamart.prostore.query.calcite.core.util.SqlNodeUtil;
import ru.datamart.prostore.query.execution.plugin.adb.dml.service.insert.select.DestinationInsertSelectHandler;
import ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.dto.TransferDataRequest;
import ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.service.executor.AdbMppwDataTransferService;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.dml.LlwUtils;
import ru.datamart.prostore.query.execution.plugin.api.request.InsertSelectRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.dto.EnrichQueryRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.QueryEnrichmentService;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static ru.datamart.prostore.query.calcite.core.util.SqlNodeTemplates.*;

@Service
public class AdbLogicalInsertSelectHandler implements DestinationInsertSelectHandler {

    private static final SqlLiteral NULL_LITERAL = SqlLiteral.createNull(SqlParserPos.ZERO);
    private static final String SYS_OP = "sys_op";
    private static final SqlIdentifier SYS_OP_IDENTIFIER = identifier(SYS_OP);
    private static final List<SqlNode> TARGET_COLUMN_TO_ADD = Collections.singletonList(SYS_OP_IDENTIFIER);
    private static final SqlBasicCall ZERO_AS_SYS_OP = as(longLiteral(0), SYS_OP);
    private static final List<SqlNode> SELECT_COLUMN_TO_ADD = Collections.singletonList(ZERO_AS_SYS_OP);
    private static final List<SqlNode> SELECT_COLUMNS_TO_ADD = Arrays.asList(as(NULL_LITERAL, "sys_from"), as(NULL_LITERAL, "sys_to"), ZERO_AS_SYS_OP);
    private static final String STAGING_SUFFIX = "_staging";

    private final DatabaseExecutor queryExecutor;
    private final QueryParserService parserService;
    private final QueryEnrichmentService enrichmentService;
    private final QueryTemplateExtractor templateExtractor;
    private final SqlDialect sqlDialect;
    private final AdbMppwDataTransferService dataTransferService;

    public AdbLogicalInsertSelectHandler(DatabaseExecutor queryExecutor,
                                         @Qualifier("adbCalciteDMLQueryParserService") QueryParserService parserService,
                                         @Qualifier("adbQueryEnrichmentService") QueryEnrichmentService enrichmentService,
                                         @Qualifier("adbQueryTemplateExtractor") QueryTemplateExtractor templateExtractor,
                                         @Qualifier("adbSqlDialect") SqlDialect sqlDialect,
                                         AdbMppwDataTransferService dataTransferService) {
        this.parserService = parserService;
        this.enrichmentService = enrichmentService;
        this.templateExtractor = templateExtractor;
        this.sqlDialect = sqlDialect;
        this.queryExecutor = queryExecutor;
        this.dataTransferService = dataTransferService;
    }

    @Override
    public Future<Void> handle(InsertSelectRequest request) {
        return Future.future(promise -> {
            val insertSql = request.getQuery();
            val sourceSql = SqlNodeUtil.copy(request.getSourceQuery());
            val targetColumns = LlwUtils.extendTargetColumns(insertSql, request.getEntity(), TARGET_COLUMN_TO_ADD);
            val extendedSelect = extendSelectList(sourceSql, targetColumns);
            val targetTable = request.getEntity().getNameWithSchema() + STAGING_SUFFIX;

            enrichSelect(extendedSelect, request)
                    .compose(enrichedSelect -> executeInsert(targetTable, enrichedSelect, targetColumns, request))
                    .compose(ignore -> executeTransfer(request))
                    .onComplete(promise);
        });
    }

    private Future<SqlNode> enrichSelect(SqlNode sourceSql, InsertSelectRequest request) {
        val schema = request.getDatamarts();
        return parserService.parse(new QueryParserRequest(sourceSql, schema, request.getEnvName()))
                .compose(queryParserResponse -> enrichQuery(request, queryParserResponse));
    }

    private Future<SqlNode> enrichQuery(InsertSelectRequest request, QueryParserResponse queryParserResponse) {
        val enrichRequest = EnrichQueryRequest.builder()
                .deltaInformations(request.getDeltaInformations())
                .envName(request.getEnvName())
                .calciteContext(queryParserResponse.getCalciteContext())
                .relNode(queryParserResponse.getRelNode())
                .build();
        return enrichmentService.getEnrichedSqlNode(enrichRequest)
                .map(enrichedQuery -> templateExtractor.enrichTemplate(enrichedQuery, request.getExtractedParams()));
    }

    private SqlNode extendSelectList(SqlNode sqlSelect, SqlNodeList targetColumns) {
        if (targetColumns == null) {
            return LlwUtils.extendQuerySelectColumns(sqlSelect, SELECT_COLUMNS_TO_ADD);
        }

        return LlwUtils.extendQuerySelectColumns(sqlSelect, SELECT_COLUMN_TO_ADD);
    }

    private Future<Void> executeInsert(String extTableName, SqlNode enrichedSelect, SqlNodeList targetColumns, InsertSelectRequest request) {
        val sqlInsert = new SqlInsert(SqlParserPos.ZERO, SqlNodeList.EMPTY, identifier(extTableName), enrichedSelect, targetColumns);
        val insertQuery = sqlNodeToString(sqlInsert);
        return queryExecutor.executeWithParams(insertQuery, request.getParameters(), Collections.emptyList())
                .mapEmpty();
    }

    private String sqlNodeToString(SqlNode sqlNode) {
        return Util.toLinux(sqlNode.toSqlString(sqlDialect).getSql()).replaceAll("\r\n|\r|\n", " ");
    }

    private Future<Void> executeTransfer(InsertSelectRequest request) {
        val transferDataRequest = TransferDataRequest.builder()
                .datamart(request.getDatamartMnemonic())
                .hotDelta(request.getSysCn())
                .tableName(request.getEntity().getName())
                .columnList(EntityFieldUtils.getFieldNames(request.getEntity()))
                .keyColumnList(EntityFieldUtils.getPkFieldNames(request.getEntity()))
                .build();
        return dataTransferService.execute(transferDataRequest);
    }

    @Override
    public SourceType getDestination() {
        return SourceType.ADB;
    }

    @Override
    public boolean isLogical() {
        return true;
    }
}
