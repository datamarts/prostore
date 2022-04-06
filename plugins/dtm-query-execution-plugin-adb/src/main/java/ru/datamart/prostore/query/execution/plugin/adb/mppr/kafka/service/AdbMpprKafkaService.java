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
package ru.datamart.prostore.query.execution.plugin.adb.mppr.kafka.service;

import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.common.dto.QueryParserRequest;
import ru.datamart.prostore.common.model.ddl.ExternalTableLocationType;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.query.calcite.core.service.QueryParserService;
import ru.datamart.prostore.query.execution.plugin.adb.mppr.AdbMpprExecutor;
import ru.datamart.prostore.query.execution.plugin.adb.mppr.kafka.factory.KafkaMpprSqlFactory;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.exception.MpprDatasourceException;
import ru.datamart.prostore.query.execution.plugin.api.mppr.MpprRequest;
import ru.datamart.prostore.query.execution.plugin.api.mppr.kafka.MpprKafkaRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.dto.EnrichQueryRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.QueryEnrichmentService;

@Slf4j
@Service("adbMpprKafkaService")
public class AdbMpprKafkaService implements AdbMpprExecutor {

    private final QueryParserService parserService;
    private final QueryEnrichmentService adbQueryEnrichmentService;
    private final KafkaMpprSqlFactory kafkampprSqlFactory;
    private final DatabaseExecutor adbQueryExecutor;

    @Autowired
    public AdbMpprKafkaService(@Qualifier("adbCalciteDMLQueryParserService") QueryParserService parserService,
                               @Qualifier("adbQueryEnrichmentService") QueryEnrichmentService adbQueryEnrichmentService,
                               KafkaMpprSqlFactory kafkampprSqlFactory,
                               @Qualifier("adbQueryExecutor") DatabaseExecutor adbQueryExecutor) {
        this.parserService = parserService;
        this.adbQueryEnrichmentService = adbQueryEnrichmentService;
        this.kafkampprSqlFactory = kafkampprSqlFactory;
        this.adbQueryExecutor = adbQueryExecutor;
    }

    @Override
    public Future<QueryResult> execute(MpprRequest request) {
        return Future.future(promise -> {
            val mpprKafkaRequest = (MpprKafkaRequest) request;
            val schema = request.getDatamartMnemonic();
            val table = kafkampprSqlFactory.getTableName(request.getRequestId().toString());
            adbQueryExecutor.executeUpdate(kafkampprSqlFactory.createWritableExtTableSqlQuery(mpprKafkaRequest))
                    .compose(v -> enrichQuery(mpprKafkaRequest))
                    .compose(enrichedQuery -> insertIntoWritableExtTableSqlQuery(schema, table, enrichedQuery))
                    .compose(v -> dropWritableExtTableSqlQuery(schema, table))
                    .onSuccess(success -> promise.complete(QueryResult.emptyResult()))
                    .onFailure(err -> dropWritableExtTableSqlQuery(schema, table)
                            .onComplete(dropResult -> {
                                if (dropResult.failed()) {
                                    log.error("Failed to drop writable external table {}.{}", schema, table);
                                }
                                promise.fail(new MpprDatasourceException(
                                        String.format("Failed to unload data from datasource by request %s",
                                                request),
                                        err));
                            }));
        });
    }

    private Future<Void> dropWritableExtTableSqlQuery(String schema, String table) {
        return adbQueryExecutor.executeUpdate(
                kafkampprSqlFactory.dropWritableExtTableSqlQuery(schema,
                        table));
    }

    private Future<Void> insertIntoWritableExtTableSqlQuery(String schema, String table, String sql) {
        return adbQueryExecutor.executeUpdate(
                kafkampprSqlFactory.insertIntoWritableExtTableSqlQuery(schema,
                        table,
                        sql));
    }

    private Future<String> enrichQuery(MpprKafkaRequest request) {
        return parserService.parse(new QueryParserRequest(request.getDmlSubQuery(), request.getLogicalSchema(), request.getEnvName()))
                .compose(parserResponse -> {
                    val enrichRequest = EnrichQueryRequest.builder()
                            .deltaInformations(request.getDeltaInformations())
                            .relNode(parserResponse.getRelNode())
                            .calciteContext(parserResponse.getCalciteContext())
                            .envName(request.getEnvName())
                            .build();
                    return adbQueryEnrichmentService.enrich(enrichRequest);
                });
    }

    @Override
    public ExternalTableLocationType getType() {
        return ExternalTableLocationType.KAFKA;
    }
}
