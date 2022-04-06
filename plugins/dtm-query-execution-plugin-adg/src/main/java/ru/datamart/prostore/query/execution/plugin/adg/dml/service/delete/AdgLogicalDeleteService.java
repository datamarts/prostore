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
package ru.datamart.prostore.query.execution.plugin.adg.dml.service.delete;

import io.vertx.core.Future;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Util;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.common.delta.DeltaInformation;
import ru.datamart.prostore.common.delta.DeltaType;
import ru.datamart.prostore.common.dto.QueryParserRequest;
import ru.datamart.prostore.common.dto.QueryParserResponse;
import ru.datamart.prostore.common.model.ddl.EntityFieldUtils;
import ru.datamart.prostore.query.calcite.core.extension.dml.SqlSelectExt;
import ru.datamart.prostore.query.calcite.core.service.QueryParserService;
import ru.datamart.prostore.query.calcite.core.service.QueryTemplateExtractor;
import ru.datamart.prostore.query.calcite.core.util.SqlNodeTemplates;
import ru.datamart.prostore.query.execution.plugin.adg.base.factory.AdgHelperTableNamesFactory;
import ru.datamart.prostore.query.execution.plugin.adg.base.model.cartridge.request.AdgTransferDataEtlRequest;
import ru.datamart.prostore.query.execution.plugin.adg.base.service.client.AdgCartridgeClient;
import ru.datamart.prostore.query.execution.plugin.adg.base.service.converter.AdgPluginSpecificLiteralConverter;
import ru.datamart.prostore.query.execution.plugin.adg.dml.factory.AdgDmlSqlFactory;
import ru.datamart.prostore.query.execution.plugin.adg.query.service.AdgQueryExecutorService;
import ru.datamart.prostore.query.execution.plugin.api.dml.LlwUtils;
import ru.datamart.prostore.query.execution.plugin.api.request.DeleteRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.dto.EnrichQueryRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.QueryEnrichmentService;

import java.util.Collections;
import java.util.List;

import static java.util.Collections.singletonList;

@Service
public class AdgLogicalDeleteService {
    private static final SqlLiteral ONE_SYS_OP = SqlNodeTemplates.longLiteral(1);
    private static final List<SqlLiteral> SYSTEM_LITERALS = singletonList(ONE_SYS_OP);
    private final AdgQueryExecutorService executor;
    private final AdgCartridgeClient cartridgeClient;
    private final AdgHelperTableNamesFactory adgHelperTableNamesFactory;
    private final QueryEnrichmentService queryEnrichmentService;
    private final QueryParserService queryParserService;
    private final AdgPluginSpecificLiteralConverter pluginSpecificLiteralConverter;
    private final QueryTemplateExtractor queryTemplateExtractor;
    private final SqlDialect sqlDialect;

    public AdgLogicalDeleteService(AdgQueryExecutorService executor,
                                   AdgCartridgeClient cartridgeClient,
                                   AdgHelperTableNamesFactory adgHelperTableNamesFactory,
                                   @Qualifier("adgQueryEnrichmentService") QueryEnrichmentService queryEnrichmentService,
                                   @Qualifier("adgCalciteDMLQueryParserService") QueryParserService queryParserService,
                                   @Qualifier("adgPluginSpecificLiteralConverter") AdgPluginSpecificLiteralConverter pluginSpecificLiteralConverter,
                                   @Qualifier("adgQueryTemplateExtractor") QueryTemplateExtractor queryTemplateExtractor,
                                   @Qualifier("adgSqlDialect") SqlDialect sqlDialect) {
        this.executor = executor;
        this.cartridgeClient = cartridgeClient;
        this.adgHelperTableNamesFactory = adgHelperTableNamesFactory;
        this.queryEnrichmentService = queryEnrichmentService;
        this.queryParserService = queryParserService;
        this.pluginSpecificLiteralConverter = pluginSpecificLiteralConverter;
        this.queryTemplateExtractor = queryTemplateExtractor;
        this.sqlDialect = sqlDialect;
    }

    public Future<Void> execute(DeleteRequest request) {
        return Future.future(promise -> {
            val condition = request.getQuery().getCondition();
            val tableIdentifier = LlwUtils.getTableIdentifier(request.getDatamartMnemonic(), request.getEntity().getName(), request.getQuery().getAlias());
            val notNullableFields = EntityFieldUtils.getNotNullableFields(request.getEntity());
            val columns = LlwUtils.getExtendedSelectList(notNullableFields, SYSTEM_LITERALS);
            val sqlSelectExt = new SqlSelectExt(SqlParserPos.ZERO, SqlNodeList.EMPTY, columns, tableIdentifier, condition, null, null, SqlNodeList.EMPTY, null, null, null, SqlNodeList.EMPTY, null, false);
            val schema = request.getDatamarts();
            queryParserService.parse(new QueryParserRequest(sqlSelectExt, schema, request.getEnvName()))
                    .compose(queryParserResponse -> enrichQuery(request, queryParserResponse))
                    .map(enrichedQuery -> convertParams(request, enrichedQuery, condition))
                    .map(this::sqlNodeToString)
                    .compose(enrichedQuery -> executeInsert(request, enrichedQuery))
                    .compose(v -> executeTransfer(request))
                    .onComplete(promise);
        });
    }

    private String sqlNodeToString(SqlNode sqlNode) {
        return Util.toLinux(sqlNode.toSqlString(sqlDialect).getSql())
                .replaceAll("\r\n|\r|\n", " ");
    }

    private SqlNode convertParams(DeleteRequest request, SqlNode enrichedQuery, SqlNode origDeleteCondition) {
        if (origDeleteCondition == null) {
            return enrichedQuery;
        }

        val convertedParams = pluginSpecificLiteralConverter.convertDeleteParams(request.getExtractedParams(), request.getParametersTypes());
        return queryTemplateExtractor.enrichTemplate(enrichedQuery, convertedParams);
    }

    private Future<SqlNode> enrichQuery(DeleteRequest request, QueryParserResponse queryParserResponse) {
        val enrichRequest = EnrichQueryRequest.builder()
                .envName(request.getEnvName())
                .deltaInformations(Collections.singletonList(
                        DeltaInformation.builder()
                                .schemaName(request.getDatamartMnemonic())
                                .type(DeltaType.WITHOUT_SNAPSHOT)
                                .selectOnNum(request.getDeltaOkSysCn())
                                .build()
                ))
                .calciteContext(queryParserResponse.getCalciteContext())
                .relNode(queryParserResponse.getRelNode())
                .build();
        return queryEnrichmentService.getEnrichedSqlNode(enrichRequest);
    }

    private Future<Void> executeInsert(DeleteRequest request, String enrichedQuery) {
        val queryToExecute = AdgDmlSqlFactory.createDeleteSql(request.getDatamartMnemonic(), request.getEnvName(), request.getEntity(), enrichedQuery);
        return executor.executeUpdate(queryToExecute, request.getParameters());
    }

    private Future<Void> executeTransfer(DeleteRequest request) {
        val tableNames = adgHelperTableNamesFactory.create(
                request.getEnvName(),
                request.getDatamartMnemonic(),
                request.getEntity().getName());
        val transferDataRequest = new AdgTransferDataEtlRequest(tableNames, request.getSysCn());
        return cartridgeClient.transferDataToScdTable(transferDataRequest);
    }
}
