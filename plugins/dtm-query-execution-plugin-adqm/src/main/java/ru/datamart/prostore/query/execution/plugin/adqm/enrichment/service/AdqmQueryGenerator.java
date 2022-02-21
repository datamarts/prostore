/*
 * Copyright © 2021 ProStore
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

import ru.datamart.prostore.common.calcite.CalciteContext;
import ru.datamart.prostore.common.delta.DeltaInformation;
import ru.datamart.prostore.query.calcite.core.node.SqlKindKey;
import ru.datamart.prostore.query.calcite.core.node.SqlSelectTree;
import ru.datamart.prostore.query.calcite.core.rel2sql.DtmRelToSqlConverter;
import ru.datamart.prostore.query.execution.plugin.api.exception.DataSourceException;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.dto.EnrichQueryRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.dto.QueryGeneratorContext;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.QueryExtendService;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.QueryGenerator;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import lombok.var;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Util;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;

@Slf4j
@Service("adqmQueryGenerator")
public class AdqmQueryGenerator implements QueryGenerator {
    public static final SqlKindKey UNION_KEY = new SqlKindKey(SqlKind.UNION, 1);
    private final QueryExtendService queryExtendService;
    private final SqlDialect sqlDialect;
    private final DtmRelToSqlConverter relToSqlConverter;

    public AdqmQueryGenerator(@Qualifier("adqmDmlQueryExtendService") QueryExtendService queryExtendService,
                              @Qualifier("adqmSqlDialect") SqlDialect sqlDialect,
                              @Qualifier("adqmRelToSqlConverter") DtmRelToSqlConverter relToSqlConverter) {
        this.queryExtendService = queryExtendService;
        this.sqlDialect = sqlDialect;
        this.relToSqlConverter = relToSqlConverter;
    }

    @Override
    public Future<String> mutateQuery(RelRoot relNode,
                                      List<DeltaInformation> deltaInformations,
                                      CalciteContext calciteContext,
                                      EnrichQueryRequest enrichQueryRequest) {
        return getMutatedSqlNode(relNode, deltaInformations, calciteContext, enrichQueryRequest)
                .map(sqlNodeResult -> {
                    val queryResult = Util.toLinux(sqlNodeResult.toSqlString(sqlDialect).getSql())
                            .replace("\n", " ");
                    log.debug("sql = " + queryResult);
                    return queryResult;
                });
    }

    @Override
    public Future<SqlNode> getMutatedSqlNode(RelRoot relNode,
                                             List<DeltaInformation> deltaInformations,
                                             CalciteContext calciteContext,
                                             EnrichQueryRequest enrichQueryRequest) {

        return Future.future(promise -> {
            val generatorContext = getContext(relNode,
                    deltaInformations,
                    calciteContext,
                    enrichQueryRequest);

            try {
                var extendedQuery = queryExtendService.extendQuery(generatorContext);
                val sqlNodeResult = relToSqlConverter.convert(extendedQuery);
                val sqlTree = new SqlSelectTree(sqlNodeResult);
                addFinalOperatorTopUnionTables(sqlTree);
                promise.complete(sqlNodeResult);
            } catch (Exception exception) {
                promise.fail(new DataSourceException("Error in converting relation node", exception));
            }
        });
    }

    private void addFinalOperatorTopUnionTables(SqlSelectTree tree) {
        tree.findAllTableAndSnapshots()
                .stream()
                .filter(n -> n.getKindPath().stream()
                        .noneMatch(sqlKindKey -> sqlKindKey.getSqlKind() == SqlKind.SCALAR_QUERY || sqlKindKey.equals(UNION_KEY)))
                .forEach(node -> {
                    SqlIdentifier identifier = node.getNode();
                    val names = Arrays.asList(
                            identifier.names.get(0),
                            identifier.names.get(1) + " FINAL"
                    );
                    node.getSqlNodeSetter().accept(new SqlIdentifier(names, identifier.getParserPosition()));
                });
    }

    private QueryGeneratorContext getContext(RelRoot relNode,
                                             List<DeltaInformation> deltaInformations,
                                             CalciteContext calciteContext,
                                             EnrichQueryRequest enrichQueryRequest) {
        return QueryGeneratorContext.builder()
                .deltaIterator(deltaInformations.iterator())
                .relBuilder(calciteContext.getRelBuilder())
                .enrichQueryRequest(enrichQueryRequest)
                .relNode(relNode)
                .build();
    }
}
