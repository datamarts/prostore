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
package ru.datamart.prostore.query.execution.plugin.adg.dml.service.insert;

import io.vertx.core.Future;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.datamart.prostore.common.dto.QueryParserRequest;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.calcite.core.service.QueryParserService;
import ru.datamart.prostore.query.calcite.core.service.QueryTemplateExtractor;
import ru.datamart.prostore.query.calcite.core.util.SqlNodeTemplates;
import ru.datamart.prostore.query.execution.plugin.adg.base.factory.AdgHelperTableNamesFactory;
import ru.datamart.prostore.query.execution.plugin.adg.base.model.cartridge.request.AdgTransferDataEtlRequest;
import ru.datamart.prostore.query.execution.plugin.adg.base.service.client.AdgCartridgeClient;
import ru.datamart.prostore.query.execution.plugin.adg.dml.factory.AdgDmlSqlFactory;
import ru.datamart.prostore.query.execution.plugin.adg.query.service.AdgQueryExecutorService;
import ru.datamart.prostore.query.execution.plugin.api.dml.LlwUtils;
import ru.datamart.prostore.query.execution.plugin.api.request.InsertSelectRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.PluginSpecificLiteralConverter;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.dto.EnrichQueryRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.QueryEnrichmentService;

import java.util.List;

import static java.util.Collections.singletonList;
import static ru.datamart.prostore.query.execution.plugin.api.dml.LlwUtils.extendQuerySelectColumns;

@Component
public class InsertSelectToAdgHandler implements DestinationInsertSelectHandler {
    private static final SqlLiteral ZERO_SYS_OP = SqlNodeTemplates.longLiteral(0);
    private static final List<SqlLiteral> COLUMNS_TO_ADD = singletonList(ZERO_SYS_OP);

    private final QueryParserService queryParserService;
    private final QueryEnrichmentService queryEnrichmentService;
    private final PluginSpecificLiteralConverter pluginSpecificLiteralConverter;
    private final QueryTemplateExtractor queryTemplateExtractor;
    private final AdgQueryExecutorService queryExecutorService;
    private final AdgCartridgeClient cartridgeClient;
    private final AdgHelperTableNamesFactory adgHelperTableNamesFactory;
    private final SqlDialect sqlDialect;

    public InsertSelectToAdgHandler(@Qualifier("adgCalciteDMLQueryParserService") QueryParserService queryParserService,
                                    @Qualifier("adgQueryEnrichmentService") QueryEnrichmentService queryEnrichmentService,
                                    @Qualifier("adgTemplateParameterConverter") PluginSpecificLiteralConverter pluginSpecificLiteralConverter,
                                    @Qualifier("adgQueryTemplateExtractor") QueryTemplateExtractor queryTemplateExtractor,
                                    AdgQueryExecutorService queryExecutorService,
                                    AdgCartridgeClient cartridgeClient,
                                    AdgHelperTableNamesFactory adgHelperTableNamesFactory,
                                    @Qualifier("adgSqlDialect") SqlDialect sqlDialect) {
        this.queryParserService = queryParserService;
        this.queryEnrichmentService = queryEnrichmentService;
        this.pluginSpecificLiteralConverter = pluginSpecificLiteralConverter;
        this.queryTemplateExtractor = queryTemplateExtractor;
        this.queryExecutorService = queryExecutorService;
        this.cartridgeClient = cartridgeClient;
        this.adgHelperTableNamesFactory = adgHelperTableNamesFactory;
        this.sqlDialect = sqlDialect;
    }

    @Override
    public Future<Void> handle(InsertSelectRequest request) {
        return Future.future(promise -> {
            val logicalFields = LlwUtils.getFilteredLogicalFields(request.getEntity(), request.getQuery().getTargetColumnList());
            val source = extendQuerySelectColumns(request.getSourceQuery(), COLUMNS_TO_ADD);

            queryParserService.parse(new QueryParserRequest(source, request.getDatamarts()))
                    .compose(queryParserResponse -> queryEnrichmentService.getEnrichedSqlNode(new EnrichQueryRequest(request.getDeltaInformations(), request.getDatamarts(), request.getEnvName(), source, false), queryParserResponse))
                    .compose(enrichedQuery -> {
                        val convertedParams = pluginSpecificLiteralConverter.convert(request.getExtractedParams(), request.getParametersTypes());
                        val sourceWithParams = queryTemplateExtractor.enrichTemplate(enrichedQuery, convertedParams);
                        val queryString = getQueryString(sourceWithParams);
                        val insertSelectSql = AdgDmlSqlFactory.createInsertSelectSql(request.getDatamartMnemonic(), request.getEnvName(),
                                request.getEntity().getName(), logicalFields, queryString);
                        return queryExecutorService.executeUpdate(insertSelectSql, request.getParameters());
                    })
                    .compose(v -> executeTransfer(request))
                    .onComplete(promise);
        });
    }

    @Override
    public SourceType getDestinations() {
        return SourceType.ADG;
    }

    private String getQueryString(SqlNode sourceWithParams) {
        return sourceWithParams.toSqlString(sqlDialect).getSql().replaceAll("\r\n|\r|\n", " ");
    }

    private Future<Void> executeTransfer(InsertSelectRequest request) {
        val tableNames = adgHelperTableNamesFactory.create(
                request.getEnvName(),
                request.getDatamartMnemonic(),
                request.getEntity().getName());
        val transferDataRequest = new AdgTransferDataEtlRequest(tableNames, request.getSysCn());
        return cartridgeClient.transferDataToScdTable(transferDataRequest);
    }
}
