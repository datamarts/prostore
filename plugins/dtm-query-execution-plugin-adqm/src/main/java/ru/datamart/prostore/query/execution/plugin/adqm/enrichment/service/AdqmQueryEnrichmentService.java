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
package ru.datamart.prostore.query.execution.plugin.adqm.enrichment.service;

import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import lombok.var;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Util;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.common.calcite.CalciteContext;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.query.calcite.core.node.SqlKindKey;
import ru.datamart.prostore.query.calcite.core.node.SqlSelectTree;
import ru.datamart.prostore.query.calcite.core.rel2sql.DtmRelToSqlConverter;
import ru.datamart.prostore.query.execution.plugin.adqm.calcite.model.schema.AdqmDtmTable;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.dto.EnrichQueryRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.dto.QueryGeneratorContext;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.QueryEnrichmentService;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.QueryExtendService;

import java.util.Arrays;

@Slf4j
@Service("adqmQueryEnrichmentService")
public class AdqmQueryEnrichmentService implements QueryEnrichmentService {
    private static final SqlKindKey UNION_KEY = new SqlKindKey(SqlKind.UNION, 1);
    private final QueryExtendService queryExtendService;
    private final SqlDialect sqlDialect;
    private final DtmRelToSqlConverter relToSqlConverter;

    @Autowired
    public AdqmQueryEnrichmentService(@Qualifier("adqmDmlQueryExtendService") QueryExtendService queryExtendService,
                                      @Qualifier("adqmSqlDialect") SqlDialect sqlDialect,
                                      @Qualifier("adqmRelToSqlConverter") DtmRelToSqlConverter relToSqlConverter) {
        this.queryExtendService = queryExtendService;
        this.sqlDialect = sqlDialect;
        this.relToSqlConverter = relToSqlConverter;
    }

    @Override
    public Future<String> enrich(EnrichQueryRequest request) {
        return getEnrichedSqlNode(request)
                .map(sqlNodeResult -> {
                    val queryResult = Util.toLinux(sqlNodeResult.toSqlString(sqlDialect).getSql())
                            .replace("\n", " ");
                    log.debug("sql = " + queryResult);
                    return queryResult;
                })
                .onSuccess(enrichedQueryResult -> log.debug("Request generated: {}", enrichedQueryResult));
    }

    @Override
    public Future<SqlNode> getEnrichedSqlNode(EnrichQueryRequest request) {
        return Future.future(promise -> {
            val generatorContext = getContext(request);
            var extendedQuery = queryExtendService.extendQuery(generatorContext);
            val sqlNodeResult = relToSqlConverter.convert(extendedQuery);
            val sqlTree = new SqlSelectTree(sqlNodeResult);
            addFinalOperatorTopUnionTables(sqlTree, request.getCalciteContext());
            promise.complete(sqlNodeResult);
        });
    }

    private void addFinalOperatorTopUnionTables(SqlSelectTree tree, CalciteContext calciteContext) {
        tree.findAllTableAndSnapshots()
                .stream()
                .filter(n -> n.getKindPath().stream()
                        .noneMatch(sqlKindKey -> sqlKindKey.getSqlKind() == SqlKind.SCALAR_QUERY || sqlKindKey.equals(UNION_KEY)))
                .forEach(node -> {
                    val identifier = (SqlIdentifier) node.getNode();
                    val schema = identifier.names.get(0);
                    val tableName = identifier.names.get(1);
                    val adqmTable = (AdqmDtmTable) calciteContext.getSchema().getSubSchema(schema).getTable(tableName);
                    if (adqmTable.getEntity().getEntityType() == EntityType.WRITEABLE_EXTERNAL_TABLE || adqmTable.getEntity().getEntityType() == EntityType.READABLE_EXTERNAL_TABLE) {
                        return;
                    }
                    val names = Arrays.asList(
                            schema,
                            tableName + " FINAL"
                    );
                    node.getSqlNodeSetter().accept(new SqlIdentifier(names, identifier.getParserPosition()));
                });
    }

    private QueryGeneratorContext getContext(EnrichQueryRequest enrichQueryRequest) {
        return new QueryGeneratorContext(
                enrichQueryRequest.getDeltaInformations().iterator(),
                enrichQueryRequest.getCalciteContext().getRelBuilder(),
                enrichQueryRequest.getRelNode(),
                enrichQueryRequest.getEnvName(),
                enrichQueryRequest.isLocal()
        );
    }

}
