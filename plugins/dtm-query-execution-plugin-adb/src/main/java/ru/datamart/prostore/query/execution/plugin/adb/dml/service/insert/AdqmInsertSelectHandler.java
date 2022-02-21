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
package ru.datamart.prostore.query.execution.plugin.adb.dml.service.insert;

import io.vertx.core.Future;
import lombok.val;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Util;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.common.dto.QueryParserRequest;
import ru.datamart.prostore.common.dto.QueryParserResponse;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.calcite.core.service.QueryParserService;
import ru.datamart.prostore.query.calcite.core.service.QueryTemplateExtractor;
import ru.datamart.prostore.query.calcite.core.util.SqlNodeUtil;
import ru.datamart.prostore.query.execution.plugin.adb.base.factory.adqm.AdqmConnectorSqlFactory;
import ru.datamart.prostore.query.execution.plugin.adb.base.service.castservice.ColumnsCastService;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.dml.LlwUtils;
import ru.datamart.prostore.query.execution.plugin.api.request.InsertSelectRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.dto.EnrichQueryRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.QueryEnrichmentService;
import ru.datamart.prostore.query.execution.plugin.api.service.shared.adqm.AdqmSharedService;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static ru.datamart.prostore.query.calcite.core.util.SqlNodeTemplates.*;

@Service
public class AdqmInsertSelectHandler implements DestinationInsertSelectHandler {

    private static final String SYS_OP = "sys_op";
    private static final String SYS_FROM = "sys_from";
    private static final String SYS_TO = "sys_to";
    private static final String SYS_CLOSE_DATE = "sys_close_date";
    private static final String SIGN = "sign";
    private static final SqlIdentifier SYS_OP_IDENTIFIER = identifier(SYS_OP);
    private static final SqlIdentifier SYS_FROM_IDENTIFIER = identifier(SYS_FROM);
    private static final SqlIdentifier SYS_TO_IDENTIFIER = identifier(SYS_TO);
    private static final SqlIdentifier SYS_CLOSE_DATE_IDENTIFIER = identifier(SYS_CLOSE_DATE);
    private static final SqlIdentifier SIGN_IDENTIFIER = identifier(SIGN);
    private static final List<SqlNode> TARGET_COLUMNS_TO_ADD = Arrays.asList(SYS_FROM_IDENTIFIER, SYS_TO_IDENTIFIER, SYS_OP_IDENTIFIER, SYS_CLOSE_DATE_IDENTIFIER, SIGN_IDENTIFIER);
    private static final SqlBasicCall ZERO_AS_SYS_OP = as(longLiteral(0), SYS_OP);
    private static final SqlBasicCall MAX_LONG_AS_SYS_TO = as(longLiteral(Long.MAX_VALUE), SYS_TO);
    private static final SqlBasicCall MAX_LONG_AS_SYS_CLOSE_DATE = as(longLiteral(Long.MAX_VALUE), SYS_CLOSE_DATE);
    private static final SqlBasicCall ONE_AS_SIGN = as(longLiteral(1), SIGN);


    private final AdqmConnectorSqlFactory connectorSqlFactory;
    private final DatabaseExecutor queryExecutor;
    private final AdqmSharedService adqmSharedService;
    private final QueryParserService parserService;
    private final ColumnsCastService columnsCastService;
    private final QueryEnrichmentService enrichmentService;
    private final QueryTemplateExtractor templateExtractor;
    private final SqlDialect sqlDialect;

    public AdqmInsertSelectHandler(AdqmConnectorSqlFactory connectorSqlFactory,
                                   DatabaseExecutor queryExecutor,
                                   AdqmSharedService adqmSharedService,
                                   @Qualifier("adbCalciteDMLQueryParserService") QueryParserService parserService,
                                   @Qualifier("adqmColumnsCastService") ColumnsCastService columnsCastService,
                                   @Qualifier("adbQueryEnrichmentService") QueryEnrichmentService enrichmentService,
                                   @Qualifier("adbQueryTemplateExtractor") QueryTemplateExtractor templateExtractor,
                                   @Qualifier("adbSqlDialect") SqlDialect sqlDialect) {
        this.connectorSqlFactory = connectorSqlFactory;
        this.queryExecutor = queryExecutor;
        this.adqmSharedService = adqmSharedService;
        this.parserService = parserService;
        this.columnsCastService = columnsCastService;
        this.enrichmentService = enrichmentService;
        this.templateExtractor = templateExtractor;
        this.sqlDialect = sqlDialect;
    }

    @Override
    public Future<Void> handle(InsertSelectRequest request) {
        return Future.future(promise -> {
            val sourceSql = SqlNodeUtil.copy(request.getSourceQuery());
            val targetColumns = LlwUtils.extendTargetColumns(request.getQuery(), request.getEntity(), TARGET_COLUMNS_TO_ADD);
            val targetColumnsTypes = LlwUtils.getColumnTypesWithAnyForSystem(targetColumns, request.getEntity());

            createWritableExtTable(request.getEntity(), request.getEnvName())
                    .compose(extTableName -> enrichSelect(sourceSql, request, targetColumnsTypes)
                            .compose(enrichedSelect -> executeInsert(extTableName, enrichedSelect, targetColumns, request))
                            .compose(ignore -> queryExecutor.executeUpdate(connectorSqlFactory.dropExternalTable(extTableName))))
                    .compose(ignore -> adqmSharedService.flushActualTable(request.getEnvName(), request.getDatamartMnemonic(), request.getEntity()))
                    .compose(ignore -> adqmSharedService.closeVersionSqlByTableActual(request.getEnvName(), request.getDatamartMnemonic(), request.getEntity(), request.getSysCn()))
                    .compose(ignore -> adqmSharedService.flushActualTable(request.getEnvName(), request.getDatamartMnemonic(), request.getEntity()))
                    .onComplete(promise);
        });
    }

    private Future<String> createWritableExtTable(Entity destination, String env) {
        val extTableName = connectorSqlFactory.extTableName(destination);
        val dropExtTable = connectorSqlFactory.dropExternalTable(extTableName);
        val createExtTable = connectorSqlFactory.createExternalTable(env, destination.getSchema(), destination);
        return queryExecutor.executeUpdate(dropExtTable)
                .compose(ignore -> queryExecutor.executeUpdate(createExtTable))
                .map(ignore -> extTableName);
    }

    private Future<SqlNode> enrichSelect(SqlNode sourceSql, InsertSelectRequest request, List<ColumnType> targetColumnsTypes) {
        val schema = request.getDatamarts();
        val sysFrom = as(longLiteral(request.getSysCn()), SYS_FROM);
        List<SqlNode> selectColumnsToAdd = Arrays.asList(sysFrom, MAX_LONG_AS_SYS_TO, ZERO_AS_SYS_OP, MAX_LONG_AS_SYS_CLOSE_DATE, ONE_AS_SIGN);
        val extendedSelect = LlwUtils.extendQuerySelectColumns(sourceSql, selectColumnsToAdd);
        return parserService.parse(new QueryParserRequest(extendedSelect, schema))
                .compose(queryParserResponse -> enrichQuery(request, queryParserResponse)
                        .compose(enrichedSqlNode -> columnsCastService.apply(enrichedSqlNode, queryParserResponse.getRelNode().rel, targetColumnsTypes)));
    }

    private Future<SqlNode> enrichQuery(InsertSelectRequest request, QueryParserResponse queryParserResponse) {
        val schema = request.getDatamarts();
        val enrichRequest = EnrichQueryRequest.builder()
                .query(queryParserResponse.getSqlNode())
                .schema(schema)
                .envName(request.getEnvName())
                .deltaInformations(request.getDeltaInformations())
                .build();
        return enrichmentService.getEnrichedSqlNode(enrichRequest, queryParserResponse)
                .map(enrichedQuery -> templateExtractor.enrichTemplate(enrichedQuery, request.getExtractedParams()));
    }

    private Future<Void> executeInsert(String extTableName, SqlNode enrichedSelect, SqlNodeList targetColumns, InsertSelectRequest request) {
        val sqlInsert = new SqlInsert(SqlParserPos.ZERO, SqlNodeList.EMPTY, identifier(extTableName), enrichedSelect, targetColumns);
        val insertQuery = sqlNodeToString(sqlInsert);
        return queryExecutor.executeWithParams(insertQuery, request.getParameters(), Collections.emptyList())
                .mapEmpty();
    }

    private String sqlNodeToString(SqlNode sqlNode) {
        return Util.toLinux(sqlNode.toSqlString(sqlDialect).getSql())
                .replaceAll("\r\n|\r|\n", " ");
    }

    @Override
    public SourceType getDestination() {
        return SourceType.ADQM;
    }
}
