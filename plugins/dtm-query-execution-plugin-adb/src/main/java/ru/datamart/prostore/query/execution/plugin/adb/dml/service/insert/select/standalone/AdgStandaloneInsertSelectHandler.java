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
import ru.datamart.prostore.query.execution.plugin.adb.base.factory.adg.AdgConnectorSqlFactory;
import ru.datamart.prostore.query.execution.plugin.adb.base.service.castservice.ColumnsCastService;
import ru.datamart.prostore.query.execution.plugin.adb.dml.service.insert.select.DestinationInsertSelectHandler;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.dml.LlwUtils;
import ru.datamart.prostore.query.execution.plugin.api.request.InsertSelectRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.dto.EnrichQueryRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.QueryEnrichmentService;

import java.util.Collections;
import java.util.List;

import static ru.datamart.prostore.query.calcite.core.util.SqlNodeTemplates.identifier;

@Service
public class AdgStandaloneInsertSelectHandler implements DestinationInsertSelectHandler {
    private static final String BUCKET_ID = "bucket_id";

    private final AdgConnectorSqlFactory connectorSqlFactory;
    private final DatabaseExecutor queryExecutor;
    private final QueryParserService parserService;
    private final ColumnsCastService columnsCastService;
    private final QueryEnrichmentService enrichmentService;
    private final QueryTemplateExtractor templateExtractor;
    private final SqlDialect sqlDialect;

    public AdgStandaloneInsertSelectHandler(AdgConnectorSqlFactory connectorSqlFactory,
                                            DatabaseExecutor queryExecutor,
                                            @Qualifier("adbCalciteDMLQueryParserService") QueryParserService parserService,
                                            @Qualifier("adgColumnsCastService") ColumnsCastService columnsCastService,
                                            @Qualifier("adbQueryEnrichmentService") QueryEnrichmentService enrichmentService,
                                            @Qualifier("adbQueryTemplateExtractor") QueryTemplateExtractor templateExtractor,
                                            @Qualifier("adbSqlDialect") SqlDialect sqlDialect) {
        this.parserService = parserService;
        this.columnsCastService = columnsCastService;
        this.enrichmentService = enrichmentService;
        this.templateExtractor = templateExtractor;
        this.sqlDialect = sqlDialect;
        this.queryExecutor = queryExecutor;
        this.connectorSqlFactory = connectorSqlFactory;
    }

    @Override
    public Future<Void> handle(InsertSelectRequest request) {
        return Future.future(promise -> {
            val sourceSql = SqlNodeUtil.copy(request.getSourceQuery());
            val targetColumns = LlwUtils.extendTargetColumns(request.getQuery(), request.getEntity(), Collections.emptyList());
            val targetColumnsTypes = LlwUtils.getColumnTypesWithAnyForSystem(targetColumns, request.getEntity());
            val bucketIdColumnIdx = findBucketId(targetColumns);

            createStandaloneWritebleExtTable(request.getEntity())
                    .compose(extTableName -> enrichSelect(sourceSql, request, targetColumnsTypes, bucketIdColumnIdx)
                            .compose(enrichedSelect -> executeInsert(extTableName, enrichedSelect, targetColumns, request))
                            .compose(ignore -> queryExecutor.executeUpdate(connectorSqlFactory.dropExternalTable(extTableName))))
                    .onComplete(promise);
        });
    }

    private int findBucketId(SqlNodeList nodeList) {
        //nodes must contains bucket_id, due constraint on standalone table in ADG
        val nodes = nodeList.getList();
        for (int i = 0; i < nodes.size(); i++) {
            val node = nodes.get(i);
            if (node instanceof SqlIdentifier && ((SqlIdentifier) node).getSimple().equals(BUCKET_ID)) {
                return i;
            }
        }

        return 0;
    }

    private Future<String> createStandaloneWritebleExtTable(Entity destination) {
        val extTableName = connectorSqlFactory.extTableName(destination);
        val dropExtTable = connectorSqlFactory.dropExternalTable(extTableName);
        val createExtTable = connectorSqlFactory.createStandaloneExternalTable(destination);
        return queryExecutor.executeUpdate(dropExtTable)
                .compose(ignore -> queryExecutor.executeUpdate(createExtTable))
                .map(ignore -> extTableName);
    }

    private Future<SqlNode> enrichSelect(SqlNode sourceSql, InsertSelectRequest request, List<ColumnType> targetColumnsTypes, int bucketIdIdx) {
        val schema = request.getDatamarts();
        return parserService.parse(new QueryParserRequest(sourceSql, schema, request.getEnvName()))
                .compose(queryParserResponse -> enrichQuery(request, queryParserResponse)
                        .compose(enrichedSqlNode -> columnsCastService.apply(enrichedSqlNode, queryParserResponse.getRelNode().rel, targetColumnsTypes, bucketIdIdx)));
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
        return SourceType.ADG;
    }

    @Override
    public boolean isLogical() {
        return false;
    }
}
