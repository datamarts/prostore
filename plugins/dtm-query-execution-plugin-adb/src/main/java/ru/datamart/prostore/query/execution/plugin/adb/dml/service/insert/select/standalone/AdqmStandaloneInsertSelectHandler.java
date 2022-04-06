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
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.calcite.core.service.QueryParserService;
import ru.datamart.prostore.query.calcite.core.service.QueryTemplateExtractor;
import ru.datamart.prostore.query.calcite.core.util.SqlNodeUtil;
import ru.datamart.prostore.query.execution.plugin.adb.base.factory.adqm.AdqmConnectorSqlFactory;
import ru.datamart.prostore.query.execution.plugin.adb.base.service.castservice.ColumnsCastService;
import ru.datamart.prostore.query.execution.plugin.adb.dml.service.insert.select.DestinationInsertSelectHandler;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.dml.LlwUtils;
import ru.datamart.prostore.query.execution.plugin.api.request.InsertSelectRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.dto.EnrichQueryRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.QueryEnrichmentService;
import ru.datamart.prostore.query.execution.plugin.api.service.shared.adqm.AdqmSharedService;

import java.util.Collections;

import static ru.datamart.prostore.query.calcite.core.util.SqlNodeTemplates.identifier;

@Service
public class AdqmStandaloneInsertSelectHandler implements DestinationInsertSelectHandler {

    private final AdqmConnectorSqlFactory connectorSqlFactory;
    private final DatabaseExecutor queryExecutor;
    private final AdqmSharedService adqmSharedService;
    private final QueryParserService parserService;
    private final ColumnsCastService columnsCastService;
    private final QueryEnrichmentService enrichmentService;
    private final QueryTemplateExtractor templateExtractor;
    private final SqlDialect sqlDialect;

    public AdqmStandaloneInsertSelectHandler(AdqmConnectorSqlFactory connectorSqlFactory,
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
            val schema = request.getDatamarts();
            val entity = request.getEntity();
            val targetColumns = LlwUtils.extendTargetColumns(request.getQuery(), entity, Collections.emptyList());
            val targetColumnsTypes = LlwUtils.getColumnTypesWithAnyForSystem(targetColumns, entity);

            createStandaloneWritableExtTable(entity)
                    .compose(extTableName -> parserService.parse(new QueryParserRequest(sourceSql, schema, request.getEnvName()))
                            .compose(queryParserResponse -> enrichQuery(request, queryParserResponse)
                                    .compose(enrichedSqlNode -> columnsCastService.apply(enrichedSqlNode, queryParserResponse.getRelNode().rel, targetColumnsTypes)))
                            .compose(enrichedSelect -> executeInsert(extTableName, enrichedSelect, targetColumns, request))
                            .compose(ignore -> queryExecutor.executeUpdate(connectorSqlFactory.dropExternalTable(extTableName))))
                    .compose(ignore -> adqmSharedService.flushTable(entity.getExternalTableLocationPath()))
                    .onComplete(promise);
        });
    }

    private Future<String> createStandaloneWritableExtTable(Entity destination) {
        val extTableName = connectorSqlFactory.extTableName(destination);
        val dropExtTable = connectorSqlFactory.dropExternalTable(extTableName);
        val createExtTable = connectorSqlFactory.createStandaloneExternalTable(destination);
        return queryExecutor.executeUpdate(dropExtTable)
                .compose(ignore -> queryExecutor.executeUpdate(createExtTable))
                .map(ignore -> extTableName);
    }

    private Future<SqlNode> enrichQuery(InsertSelectRequest request, QueryParserResponse queryParserResponse) {
        val enrichRequest = EnrichQueryRequest.builder()
                .envName(request.getEnvName())
                .deltaInformations(request.getDeltaInformations())
                .calciteContext(queryParserResponse.getCalciteContext())
                .relNode(queryParserResponse.getRelNode())
                .allowStar(false)
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
        return Util.toLinux(sqlNode.toSqlString(sqlDialect).getSql())
                .replaceAll("\r\n|\r|\n", " ");
    }

    @Override
    public SourceType getDestination() {
        return SourceType.ADQM;
    }

    @Override
    public boolean isLogical() {
        return false;
    }
}
