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
package ru.datamart.prostore.query.execution.plugin.adp.dml;

import ru.datamart.prostore.common.delta.DeltaInformation;
import ru.datamart.prostore.common.delta.DeltaType;
import ru.datamart.prostore.common.dto.QueryParserRequest;
import ru.datamart.prostore.common.dto.QueryParserResponse;
import ru.datamart.prostore.common.model.ddl.EntityFieldUtils;
import ru.datamart.prostore.query.calcite.core.extension.dml.SqlSelectExt;
import ru.datamart.prostore.query.calcite.core.service.QueryParserService;
import ru.datamart.prostore.query.calcite.core.service.QueryTemplateExtractor;
import ru.datamart.prostore.query.calcite.core.util.SqlNodeTemplates;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import ru.datamart.prostore.query.execution.plugin.adp.db.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.adp.dml.factory.AdpLlwSqlFactory;
import ru.datamart.prostore.query.execution.plugin.adp.mppw.dto.AdpTransferDataRequest;
import ru.datamart.prostore.query.execution.plugin.adp.mppw.transfer.AdpTransferDataService;
import ru.datamart.prostore.query.execution.plugin.api.dml.LlwUtils;
import ru.datamart.prostore.query.execution.plugin.api.request.DeleteRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.DeleteService;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.dto.EnrichQueryRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.QueryEnrichmentService;
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

import java.util.Arrays;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

@Service("adpDeleteService")
public class AdpDeleteService implements DeleteService {
    private static final SqlLiteral ONE_SYS_OP = SqlNodeTemplates.longLiteral(1);
    private static final List<SqlLiteral> SYSTEM_LITERALS = singletonList(ONE_SYS_OP);
    private final DatabaseExecutor executor;
    private final AdpTransferDataService dataTransferService;
    private final QueryEnrichmentService queryEnrichmentService;
    private final QueryParserService queryParserService;
    private final QueryTemplateExtractor templateExtractor;
    private final SqlDialect sqlDialect;

    public AdpDeleteService(DatabaseExecutor executor,
                            AdpTransferDataService dataTransferService,
                            @Qualifier("adpQueryEnrichmentService") QueryEnrichmentService queryEnrichmentService,
                            @Qualifier("adpCalciteDMLQueryParserService") QueryParserService queryParserService,
                            @Qualifier("adpQueryTemplateExtractor") QueryTemplateExtractor templateExtractor,
                            @Qualifier("adpSqlDialect") SqlDialect sqlDialect) {
        this.executor = executor;
        this.dataTransferService = dataTransferService;
        this.queryEnrichmentService = queryEnrichmentService;
        this.queryParserService = queryParserService;
        this.templateExtractor = templateExtractor;
        this.sqlDialect = sqlDialect;
    }

    @Override
    public Future<Void> execute(DeleteRequest request) {
        return Future.future(promise -> {
            val condition = request.getQuery().getCondition();
            val tableIdentifier = LlwUtils.getTableIdentifier(request.getDatamartMnemonic(), request.getEntity().getName(), request.getQuery().getAlias());
            val notNullableFields = EntityFieldUtils.getNotNullableFields(request.getEntity());
            val columns = LlwUtils.getExtendedSelectList(notNullableFields, SYSTEM_LITERALS);
            val sqlSelectExt = new SqlSelectExt(SqlParserPos.ZERO, SqlNodeList.EMPTY, columns, tableIdentifier, condition, null, null, SqlNodeList.EMPTY, null, null, null, SqlNodeList.EMPTY, null, false);
            val schema = request.getDatamarts();
            queryParserService.parse(new QueryParserRequest(sqlSelectExt, schema))
                    .compose(queryParserResponse -> enrichQuery(request, sqlSelectExt, schema, queryParserResponse))
                    .map(enrichedSql -> enrichTemplate(enrichedSql, request.getExtractedParams(), condition))
                    .map(LlwUtils::replaceDynamicParams)
                    .map(this::sqlNodeToString)
                    .compose(enrichedQuery -> executeInsert(request, enrichedQuery))
                    .compose(v -> executeTransfer(request))
                    .onSuccess(promise::complete)
                    .onFailure(promise::fail);
        });
    }

    private SqlNode enrichTemplate(SqlNode enrichedSql, List<SqlNode> extractedParams, SqlNode origDeleteCondition) {
        if (origDeleteCondition == null) {
            return enrichedSql;
        }

        return templateExtractor.enrichTemplate(enrichedSql, extractedParams);
    }

    private String sqlNodeToString(SqlNode sqlNode) {
        return Util.toLinux(sqlNode.toSqlString(sqlDialect).getSql()).replaceAll("\r\n|\r|\n", " ");
    }

    private Future<Void> executeTransfer(DeleteRequest request) {
        val transferDataRequest = AdpTransferDataRequest.builder()
                .datamart(request.getDatamartMnemonic())
                .sysCn(request.getSysCn())
                .tableName(request.getEntity().getName())
                .allFields(EntityFieldUtils.getFieldNames(request.getEntity()))
                .primaryKeys(EntityFieldUtils.getPkFieldNames(request.getEntity()))
                .build();
        return dataTransferService.transferData(transferDataRequest);
    }

    private Future<Void> executeInsert(DeleteRequest request, String enrichedQuery) {
        val queryToExecute = AdpLlwSqlFactory.createLlwDeleteSql(enrichedQuery, request.getDatamartMnemonic(), request.getEntity());
        return executor.executeWithParams(queryToExecute, request.getParameters(), emptyList())
                .mapEmpty();
    }

    private Future<SqlNode> enrichQuery(DeleteRequest request, SqlSelectExt sqlSelectExt, List<Datamart> schema, QueryParserResponse queryParserResponse) {
        val enrichRequest = EnrichQueryRequest.builder()
                .query(sqlSelectExt)
                .schema(schema)
                .envName(request.getEnvName())
                .deltaInformations(Arrays.asList(
                        DeltaInformation.builder()
                                .type(DeltaType.WITHOUT_SNAPSHOT)
                                .selectOnNum(request.getDeltaOkSysCn())
                                .build()
                ))
                .build();
        return queryEnrichmentService.getEnrichedSqlNode(enrichRequest, queryParserResponse);
    }


}
