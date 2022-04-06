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
package ru.datamart.prostore.query.execution.plugin.adqm.dml.service.insert.standalone;

import io.vertx.core.Future;
import lombok.val;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.datamart.prostore.common.dto.QueryParserRequest;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.EntityFieldUtils;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.calcite.core.service.QueryParserService;
import ru.datamart.prostore.query.calcite.core.util.SqlNodeTemplates;
import ru.datamart.prostore.query.execution.plugin.adqm.dml.service.insert.DestinationInsertSelectHandler;
import ru.datamart.prostore.query.execution.plugin.adqm.factory.AdqmProcessingSqlFactory;
import ru.datamart.prostore.query.execution.plugin.adqm.query.service.AdqmQueryTemplateExtractor;
import ru.datamart.prostore.query.execution.plugin.adqm.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.dml.AbstractConstantReplacer;
import ru.datamart.prostore.query.execution.plugin.api.dml.LlwUtils;
import ru.datamart.prostore.query.execution.plugin.api.request.InsertSelectRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.LlrValidationService;
import ru.datamart.prostore.query.execution.plugin.api.service.PluginSpecificLiteralConverter;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.dto.EnrichQueryRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.QueryEnrichmentService;

import java.util.Collections;
import java.util.List;

import static ru.datamart.prostore.query.execution.plugin.adqm.dml.util.AdqmDmlUtils.extendParameters;
import static ru.datamart.prostore.query.execution.plugin.adqm.dml.util.AdqmDmlUtils.validatePrimaryKeys;
import static ru.datamart.prostore.query.execution.plugin.api.dml.LlwUtils.convertEntityFieldsToSqlNodeList;

@Component
public class InsertSelectToAdqmStandaloneHandler implements DestinationInsertSelectHandler {
    private final QueryParserService queryParserService;
    private final QueryEnrichmentService queryEnrichmentService;
    private final AdqmProcessingSqlFactory adqmProcessingSqlFactory;
    private final DatabaseExecutor databaseExecutor;
    private final PluginSpecificLiteralConverter pluginSpecificLiteralConverter;
    private final AdqmQueryTemplateExtractor queryTemplateExtractor;
    private final LlrValidationService adqmValidationService;
    private final AbstractConstantReplacer constantReplacer;

    public InsertSelectToAdqmStandaloneHandler(@Qualifier("adqmCalciteDMLQueryParserService") QueryParserService queryParserService,
                                               @Qualifier("adqmQueryEnrichmentService") QueryEnrichmentService queryEnrichmentService,
                                               AdqmProcessingSqlFactory adqmProcessingSqlFactory,
                                               @Qualifier("adqmQueryExecutor") DatabaseExecutor databaseExecutor,
                                               @Qualifier("adqmPluginSpecificLiteralConverter") PluginSpecificLiteralConverter pluginSpecificLiteralConverter,
                                               AdqmQueryTemplateExtractor queryTemplateExtractor,
                                               @Qualifier("adqmValidationService") LlrValidationService adqmValidationService,
                                               @Qualifier("adqmConstantReplacer") AbstractConstantReplacer constantReplacer) {
        this.queryParserService = queryParserService;
        this.queryEnrichmentService = queryEnrichmentService;
        this.adqmProcessingSqlFactory = adqmProcessingSqlFactory;
        this.databaseExecutor = databaseExecutor;
        this.pluginSpecificLiteralConverter = pluginSpecificLiteralConverter;
        this.queryTemplateExtractor = queryTemplateExtractor;
        this.adqmValidationService = adqmValidationService;
        this.constantReplacer = constantReplacer;
    }

    @Override
    public Future<Void> handle(InsertSelectRequest request) {
        return Future.future(promise -> {
            val logicalFields = LlwUtils.getFilteredLogicalFields(request.getEntity(), request.getQuery().getTargetColumnList());
            val pkFieldNames = EntityFieldUtils.getPkFieldNames(request.getEntity());
            validatePrimaryKeys(logicalFields, pkFieldNames);
            val source = constantReplacer.replace(logicalFields, request.getSourceQuery());

            queryParserService.parse(new QueryParserRequest(source, request.getDatamarts(), request.getEnvName()))
                    .compose(queryParserResponse -> {
                        adqmValidationService.validate(queryParserResponse);
                        val enrichRequest = EnrichQueryRequest.builder()
                                .envName(request.getEnvName())
                                .deltaInformations(request.getDeltaInformations())
                                .calciteContext(queryParserResponse.getCalciteContext())
                                .relNode(queryParserResponse.getRelNode())
                                .isLocal(false)
                                .build();
                        return queryEnrichmentService.getEnrichedSqlNode(enrichRequest);
                    })
                    .compose(enrichedQuery -> {
                        val actualInsertSql = getSqlInsert(request, logicalFields, enrichedQuery);
                        val queryParameters = extendParameters(request.getDatamarts(), request.getParameters());
                        return databaseExecutor.executeWithParams(actualInsertSql, queryParameters, Collections.emptyList());
                    })
                    .<Void>mapEmpty()
                    .onComplete(promise);
        });
    }

    private String getSqlInsert(InsertSelectRequest request, List<EntityField> insertedColumns, SqlNode source) {
        val actualColumnList = convertEntityFieldsToSqlNodeList(insertedColumns);
        val tableName = SqlNodeTemplates.identifier(request.getEntity().getExternalTableLocationPath().split("\\."));
        val result = new SqlInsert(SqlParserPos.ZERO, SqlNodeList.EMPTY, tableName, new SqlNodeList(SqlParserPos.ZERO), actualColumnList);
        val convertedParams = pluginSpecificLiteralConverter.convert(request.getExtractedParams(), request.getParametersTypes());
        val sourceWithParams = queryTemplateExtractor.enrichTemplate(source, convertedParams);
        return adqmProcessingSqlFactory.getSqlFromNodes(result, sourceWithParams);
    }

    @Override
    public SourceType getDestinations() {
        return SourceType.ADQM;
    }

    @Override
    public boolean isLogical() {
        return false;
    }
}
