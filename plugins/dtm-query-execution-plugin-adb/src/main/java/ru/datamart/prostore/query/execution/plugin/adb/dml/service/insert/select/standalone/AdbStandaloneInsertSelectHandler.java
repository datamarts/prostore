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
package ru.datamart.prostore.query.execution.plugin.adb.dml.service.insert.select.standalone;

import io.vertx.core.Future;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Util;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.common.dto.QueryParserRequest;
import ru.datamart.prostore.common.dto.QueryParserResponse;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.calcite.core.service.QueryParserService;
import ru.datamart.prostore.query.calcite.core.service.QueryTemplateExtractor;
import ru.datamart.prostore.query.calcite.core.util.SqlNodeUtil;
import ru.datamart.prostore.query.execution.plugin.adb.dml.service.insert.select.DestinationInsertSelectHandler;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.dml.LlwUtils;
import ru.datamart.prostore.query.execution.plugin.api.request.InsertSelectRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.dto.EnrichQueryRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.QueryEnrichmentService;

import java.util.Collections;

import static ru.datamart.prostore.query.calcite.core.util.SqlNodeTemplates.identifier;

@Service
public class AdbStandaloneInsertSelectHandler implements DestinationInsertSelectHandler {

    private final DatabaseExecutor queryExecutor;
    private final QueryParserService parserService;
    private final QueryEnrichmentService enrichmentService;
    private final QueryTemplateExtractor templateExtractor;
    private final SqlDialect sqlDialect;

    public AdbStandaloneInsertSelectHandler(DatabaseExecutor queryExecutor,
                                            @Qualifier("adbCalciteDMLQueryParserService") QueryParserService parserService,
                                            @Qualifier("adbQueryEnrichmentService") QueryEnrichmentService enrichmentService,
                                            @Qualifier("adbQueryTemplateExtractor") QueryTemplateExtractor templateExtractor,
                                            @Qualifier("adbSqlDialect") SqlDialect sqlDialect) {
        this.parserService = parserService;
        this.enrichmentService = enrichmentService;
        this.templateExtractor = templateExtractor;
        this.sqlDialect = sqlDialect;
        this.queryExecutor = queryExecutor;
    }

    @Override
    public Future<Void> handle(InsertSelectRequest request) {
        return Future.future(promise -> {
            val insertSql = request.getQuery();
            val sourceSql = SqlNodeUtil.copy(request.getSourceQuery());
            val targetColumns = LlwUtils.extendTargetColumns(insertSql, request.getEntity(), Collections.emptyList());
            val targetTable = request.getEntity().getExternalTableLocationPath();

            enrichSelect(sourceSql, request)
                    .compose(enrichedSelect -> executeInsert(targetTable, enrichedSelect, targetColumns, request))
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

    private Future<Void> executeInsert(String extTableName, SqlNode enrichedSelect, SqlNodeList targetColumns, InsertSelectRequest request) {
        val sqlInsert = new SqlInsert(SqlParserPos.ZERO, SqlNodeList.EMPTY, identifier(extTableName), enrichedSelect, targetColumns);
        val insertQuery = sqlNodeToString(sqlInsert);
        return queryExecutor.executeWithParams(insertQuery, request.getParameters(), Collections.emptyList())
                .mapEmpty();
    }

    private String sqlNodeToString(SqlNode sqlNode) {
        return Util.toLinux(sqlNode.toSqlString(sqlDialect).getSql()).replaceAll("\r\n|\r|\n", " ");
    }

    @Override
    public SourceType getDestination() {
        return SourceType.ADB;
    }

    @Override
    public boolean isLogical() {
        return false;
    }
}
