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
package ru.datamart.prostore.query.execution.plugin.adb.enrichment.service;

import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Util;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.query.calcite.core.rel2sql.DtmRelToSqlConverter;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.dto.EnrichQueryRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.dto.QueryGeneratorContext;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.QueryEnrichmentService;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.QueryExtendService;

@Service("adbQueryEnrichmentService")
@Slf4j
public class AdbQueryEnrichmentService implements QueryEnrichmentService {

    private final QueryExtendService queryExtendService;
    private final SqlDialect sqlDialect;
    private final DtmRelToSqlConverter relToSqlConverter;

    @Autowired
    public AdbQueryEnrichmentService(@Qualifier("adbDmlExtendService") QueryExtendService queryExtendService,
                                     @Qualifier("adbSqlDialect") SqlDialect sqlDialect,
                                     @Qualifier("adbRelToSqlConverter") DtmRelToSqlConverter relToSqlConverter) {
        this.queryExtendService = queryExtendService;
        this.sqlDialect = sqlDialect;
        this.relToSqlConverter = relToSqlConverter;
    }

    @Override
    public Future<String> enrich(EnrichQueryRequest request) {
        return getEnrichedSqlNode(request)
                .map(sqlNodeResult -> Util.toLinux(sqlNodeResult.toSqlString(sqlDialect).getSql()).replaceAll("\r\n|\r|\n", " "))
                .onSuccess(result -> log.debug("Request generated: {}", result));
    }

    @Override
    public Future<SqlNode> getEnrichedSqlNode(EnrichQueryRequest request) {
        return Future.future(promise -> {
            val generatorContext = getContext(request);
            val extendedQuery = queryExtendService.extendQuery(generatorContext);
            val sqlNodeResult = relToSqlConverter.convert(extendedQuery, request.isAllowStar());
            promise.complete(sqlNodeResult);
        });
    }

    private QueryGeneratorContext getContext(EnrichQueryRequest enrichQueryRequest) {
        return new QueryGeneratorContext(
                enrichQueryRequest.getDeltaInformations().iterator(),
                enrichQueryRequest.getCalciteContext().getRelBuilder(),
                enrichQueryRequest.getRelNode(),
                enrichQueryRequest.getEnvName()
        );
    }

}
