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
package ru.datamart.prostore.query.execution.plugin.adqm.dml.service.insert;

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
import ru.datamart.prostore.query.execution.plugin.adqm.factory.AdqmProcessingSqlFactory;
import ru.datamart.prostore.query.execution.plugin.adqm.query.service.AdqmQueryTemplateExtractor;
import ru.datamart.prostore.query.execution.plugin.adqm.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.dml.LlwUtils;
import ru.datamart.prostore.query.execution.plugin.api.request.InsertSelectRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.LlrValidationService;
import ru.datamart.prostore.query.execution.plugin.api.service.PluginSpecificLiteralConverter;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.dto.EnrichQueryRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.QueryEnrichmentService;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static ru.datamart.prostore.query.calcite.core.util.SqlNodeTemplates.longLiteral;
import static ru.datamart.prostore.query.execution.plugin.adqm.dml.util.AdqmDmlUtils.*;
import static ru.datamart.prostore.query.execution.plugin.api.dml.LlwUtils.extendQuerySelectColumns;

@Component
public class InsertSelectToAdqmHandler implements DestinationInsertSelectHandler {
    private final QueryParserService queryParserService;
    private final QueryEnrichmentService queryEnrichmentService;
    private final AdqmProcessingSqlFactory adqmProcessingSqlFactory;
    private final DatabaseExecutor databaseExecutor;
    private final PluginSpecificLiteralConverter pluginSpecificLiteralConverter;
    private final AdqmQueryTemplateExtractor queryTemplateExtractor;
    private final LlrValidationService adqmValidationService;

    public InsertSelectToAdqmHandler(@Qualifier("adqmCalciteDMLQueryParserService") QueryParserService queryParserService,
                                     @Qualifier("adqmQueryEnrichmentService") QueryEnrichmentService queryEnrichmentService,
                                     AdqmProcessingSqlFactory adqmProcessingSqlFactory,
                                     @Qualifier("adqmQueryExecutor") DatabaseExecutor databaseExecutor,
                                     @Qualifier("adqmTemplateParameterConverter") PluginSpecificLiteralConverter pluginSpecificLiteralConverter,
                                     AdqmQueryTemplateExtractor queryTemplateExtractor,
                                     @Qualifier("adqmValidationService") LlrValidationService adqmValidationService) {
        this.queryParserService = queryParserService;
        this.queryEnrichmentService = queryEnrichmentService;
        this.adqmProcessingSqlFactory = adqmProcessingSqlFactory;
        this.databaseExecutor = databaseExecutor;
        this.pluginSpecificLiteralConverter = pluginSpecificLiteralConverter;
        this.queryTemplateExtractor = queryTemplateExtractor;
        this.adqmValidationService = adqmValidationService;
    }

    @Override
    public Future<Void> handle(InsertSelectRequest request) {
        return Future.future(promise -> {
            val logicalFields = LlwUtils.getFilteredLogicalFields(request.getEntity(), request.getQuery().getTargetColumnList());
            val pkFieldNames = EntityFieldUtils.getPkFieldNames(request.getEntity());
            validatePrimaryKeys(logicalFields, pkFieldNames);

            val columnsToAdd = Arrays.asList(longLiteral(request.getSysCn()), MAX_CN_LITERAL, ZERO_SYS_OP_LITERAL, MAX_CN_LITERAL, ONE_SIGN_LITERAL);
            val source = extendQuerySelectColumns(request.getSourceQuery(), columnsToAdd);

            queryParserService.parse(new QueryParserRequest(source, request.getDatamarts()))
                    .compose(queryParserResponse -> {
                        adqmValidationService.validate(queryParserResponse);
                        return queryEnrichmentService.getEnrichedSqlNode(new EnrichQueryRequest(request.getDeltaInformations(), request.getDatamarts(), request.getEnvName(), source, false), queryParserResponse);
                    })
                    .compose(enrichedQuery -> {
                        val actualInsertSql = getSqlInsert(request, logicalFields, enrichedQuery);
                        val queryParameters = extendParameters(request.getParameters());
                        return databaseExecutor.executeWithParams(actualInsertSql, queryParameters, Collections.emptyList());
                    })
                    .compose(ignored -> flushAndOptimize(request))
                    .compose(ignored -> {
                        val closedInsertSql = adqmProcessingSqlFactory.getCloseVersionSqlByTableActual(request.getEnvName(), request.getDatamartMnemonic(), request.getEntity(), request.getSysCn());
                        return databaseExecutor.executeUpdate(closedInsertSql);
                    })
                    .compose(ignored -> flushAndOptimize(request))
                    .onComplete(promise);
        });
    }

    private String getSqlInsert(InsertSelectRequest request, List<EntityField> insertedColumns, SqlNode source) {
        val actualColumnList = getInsertedColumnsList(insertedColumns);
        val result = new SqlInsert(SqlParserPos.ZERO, SqlNodeList.EMPTY, getActualTableIdentifier(request.getEnvName(), request.getDatamartMnemonic(), request.getEntity().getName()), new SqlNodeList(SqlParserPos.ZERO), actualColumnList);
        val convertedParams = pluginSpecificLiteralConverter.convert(request.getExtractedParams(), request.getParametersTypes());
        val sourceWithParams = queryTemplateExtractor.enrichTemplate(source, convertedParams);
        return adqmProcessingSqlFactory.getSqlFromNodes(result, sourceWithParams);
    }

    private Future<Void> flushAndOptimize(InsertSelectRequest request) {
        return databaseExecutor.executeUpdate(adqmProcessingSqlFactory.getFlushActualSql(request.getEnvName(), request.getDatamartMnemonic(), request.getEntity().getName()))
                .compose(unused -> databaseExecutor.executeUpdate(adqmProcessingSqlFactory.getOptimizeActualSql(request.getEnvName(), request.getDatamartMnemonic(), request.getEntity().getName())));
    }

    @Override
    public SourceType getDestinations() {
        return SourceType.ADQM;
    }
}
