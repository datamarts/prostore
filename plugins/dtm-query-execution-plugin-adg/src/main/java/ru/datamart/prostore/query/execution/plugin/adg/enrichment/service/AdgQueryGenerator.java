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
package ru.datamart.prostore.query.execution.plugin.adg.enrichment.service;

import ru.datamart.prostore.common.calcite.CalciteContext;
import ru.datamart.prostore.common.delta.DeltaInformation;
import ru.datamart.prostore.query.calcite.core.rel2sql.DtmRelToSqlConverter;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.dto.EnrichQueryRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.dto.QueryGeneratorContext;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.QueryExtendService;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.QueryGenerator;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.*;
import org.apache.calcite.util.Util;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.List;

@Service("adgQueryGenerator")
@Slf4j
public class AdgQueryGenerator implements QueryGenerator {
    private final QueryExtendService queryExtendService;
    private final SqlDialect sqlDialect;
    private final DtmRelToSqlConverter relToSqlConverter;
    private final AdgCollateValueReplacer collateReplacer;

    @Autowired
    public AdgQueryGenerator(@Qualifier("adgDmlQueryExtendService") QueryExtendService queryExtendService,
                             @Qualifier("adgSqlDialect") SqlDialect sqlDialect,
                             @Qualifier("adgRelToSqlConverter") DtmRelToSqlConverter relToSqlConverter,
                             AdgCollateValueReplacer collateReplacer) {
        this.queryExtendService = queryExtendService;
        this.sqlDialect = sqlDialect;
        this.relToSqlConverter = relToSqlConverter;
        this.collateReplacer = collateReplacer;
    }

    @Override
    public Future<String> mutateQuery(RelRoot relNode,
                                      List<DeltaInformation> deltaInformations,
                                      CalciteContext calciteContext,
                                      EnrichQueryRequest enrichQueryRequest) {
        return getMutatedSqlNode(relNode, deltaInformations, calciteContext, enrichQueryRequest)
                .map(sqlNodeResult -> {
                    val queryResult = Util.toLinux(sqlNodeResult.toSqlString(sqlDialect).getSql())
                            .replaceAll("\r\n|\r|\n", " ");
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
            val generatorContext = getContext(deltaInformations, calciteContext, relNode,
                    enrichQueryRequest);
            val extendedQuery = queryExtendService.extendQuery(generatorContext);
            val sqlNodeResult = relToSqlConverter.convert(extendedQuery);
            val nodeWithReplacedCollate = collateReplacer.replace(sqlNodeResult);
            promise.complete(nodeWithReplacedCollate);
        });
    }

    private QueryGeneratorContext getContext(List<DeltaInformation> deltaInformations,
                                             CalciteContext calciteContext,
                                             RelRoot relNode,
                                             EnrichQueryRequest enrichQueryRequest) {
        return new QueryGeneratorContext(
                deltaInformations.iterator(),
                calciteContext.getRelBuilder(),
                relNode,
                true,
                enrichQueryRequest);
    }
}
