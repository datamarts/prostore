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
package ru.datamart.prostore.query.execution.plugin.adg.dml.service.insert.select;

import io.vertx.core.Future;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.datamart.prostore.common.dto.QueryParserRequest;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.calcite.core.service.QueryParserService;
import ru.datamart.prostore.query.calcite.core.service.QueryTemplateExtractor;
import ru.datamart.prostore.query.execution.plugin.adg.dml.factory.AdgDmlSqlFactory;
import ru.datamart.prostore.query.execution.plugin.adg.query.service.AdgQueryExecutorService;
import ru.datamart.prostore.query.execution.plugin.api.dml.AbstractConstantReplacer;
import ru.datamart.prostore.query.execution.plugin.api.dml.LlwUtils;
import ru.datamart.prostore.query.execution.plugin.api.request.InsertSelectRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.PluginSpecificLiteralConverter;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.dto.EnrichQueryRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.QueryEnrichmentService;

@Component
public class InsertSelectToAdgStandaloneHandler implements DestinationInsertSelectHandler {
    private final QueryParserService queryParserService;
    private final QueryEnrichmentService queryEnrichmentService;
    private final PluginSpecificLiteralConverter pluginSpecificLiteralConverter;
    private final QueryTemplateExtractor queryTemplateExtractor;
    private final AdgQueryExecutorService queryExecutorService;
    private final SqlDialect sqlDialect;
    private final AbstractConstantReplacer constantReplacer;

    public InsertSelectToAdgStandaloneHandler(@Qualifier("adgCalciteDMLQueryParserService") QueryParserService queryParserService,
                                              @Qualifier("adgQueryEnrichmentService") QueryEnrichmentService queryEnrichmentService,
                                              @Qualifier("adgPluginSpecificLiteralConverter") PluginSpecificLiteralConverter pluginSpecificLiteralConverter,
                                              @Qualifier("adgQueryTemplateExtractor") QueryTemplateExtractor queryTemplateExtractor,
                                              AdgQueryExecutorService queryExecutorService,
                                              @Qualifier("adgSqlDialect") SqlDialect sqlDialect,
                                              @Qualifier("adgConstantReplacer") AbstractConstantReplacer constantReplacer) {
        this.queryParserService = queryParserService;
        this.queryEnrichmentService = queryEnrichmentService;
        this.pluginSpecificLiteralConverter = pluginSpecificLiteralConverter;
        this.queryTemplateExtractor = queryTemplateExtractor;
        this.queryExecutorService = queryExecutorService;
        this.sqlDialect = sqlDialect;
        this.constantReplacer = constantReplacer;
    }

    @Override
    public Future<Void> handle(InsertSelectRequest request) {
        return Future.future(promise -> {
            val entity = request.getEntity();
            val targetColumns = request.getQuery().getTargetColumnList();
            val logicalFields = LlwUtils.getFilteredLogicalFields(entity, targetColumns);
            val source = constantReplacer.replace(logicalFields, request.getSourceQuery());
            val env = request.getEnvName();
            val datamarts = request.getDatamarts();

            queryParserService.parse(new QueryParserRequest(source, datamarts, env))
                    .compose(queryParserResponse -> {
                        val enrichRequest = EnrichQueryRequest.builder()
                                .envName(env)
                                .deltaInformations(request.getDeltaInformations())
                                .calciteContext(queryParserResponse.getCalciteContext())
                                .relNode(queryParserResponse.getRelNode())
                                .build();
                        return queryEnrichmentService.getEnrichedSqlNode(enrichRequest);
                    })
                    .compose(enrichedQuery -> {
                        val convertedParams = pluginSpecificLiteralConverter.convert(request.getExtractedParams(), request.getParametersTypes());
                        val sourceWithParams = queryTemplateExtractor.enrichTemplate(enrichedQuery, convertedParams);
                        val queryString = getQueryString(sourceWithParams);
                        val insertSelectSql = AdgDmlSqlFactory.createStandaloneInsertSelectSql(entity.getExternalTableLocationPath(), logicalFields, queryString);
                        return queryExecutorService.executeUpdate(insertSelectSql, request.getParameters());
                    })
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

    @Override
    public boolean isLogical() {
        return false;
    }
}
