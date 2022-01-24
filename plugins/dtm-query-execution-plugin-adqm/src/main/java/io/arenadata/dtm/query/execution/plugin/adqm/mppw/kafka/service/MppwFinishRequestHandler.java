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
package io.arenadata.dtm.query.execution.plugin.adqm.mppw.kafka.service;

import io.arenadata.dtm.common.model.ddl.EntityFieldUtils;
import io.arenadata.dtm.query.execution.plugin.adqm.base.utils.AdqmDdlUtil;
import io.arenadata.dtm.query.execution.plugin.adqm.ddl.configuration.properties.DdlProperties;
import io.arenadata.dtm.query.execution.plugin.adqm.factory.AdqmProcessingSqlFactory;
import io.arenadata.dtm.query.execution.plugin.adqm.mppw.kafka.dto.RestMppwKafkaStopRequest;
import io.arenadata.dtm.query.execution.plugin.adqm.mppw.kafka.service.load.RestLoadClient;
import io.arenadata.dtm.query.execution.plugin.adqm.query.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adqm.status.dto.StatusReportDto;
import io.arenadata.dtm.query.execution.plugin.adqm.status.service.StatusReporter;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;

import static io.arenadata.dtm.query.execution.plugin.adqm.base.utils.AdqmDdlUtil.sequenceAll;
import static io.arenadata.dtm.query.execution.plugin.adqm.base.utils.Constants.*;

@Component("adqmMppwFinishRequestHandler")
@Slf4j
public class MppwFinishRequestHandler extends AbstractMppwRequestHandler {

    private final RestLoadClient restLoadClient;
    private final StatusReporter statusReporter;
    private final AdqmProcessingSqlFactory adqmProcessingSqlFactory;

    @Autowired
    public MppwFinishRequestHandler(RestLoadClient restLoadClient,
                                    final DatabaseExecutor databaseExecutor,
                                    final DdlProperties ddlProperties,
                                    StatusReporter statusReporter,
                                    AdqmProcessingSqlFactory adqmProcessingSqlFactory) {
        super(databaseExecutor, ddlProperties);
        this.restLoadClient = restLoadClient;
        this.statusReporter = statusReporter;
        this.adqmProcessingSqlFactory = adqmProcessingSqlFactory;
    }

    @Override
    public Future<String> execute(final MppwKafkaRequest request) {
        val err = AdqmDdlUtil.validateRequest(request);
        if (err.isPresent()) {
            return Future.failedFuture(err.get());
        }

        val fullName = AdqmDdlUtil.getQualifiedTableName(request);
        val sysCn = request.getSysCn();
        val columnNames = String.join(", ", EntityFieldUtils.getFieldNames(request.getDestinationEntity()));
        val primaryKeys = String.join(", ", EntityFieldUtils.getPkFieldNames(request.getDestinationEntity()));

        return sequenceAll(Arrays.asList(  // 1. drop shard tables
                fullName + EXT_SHARD_POSTFIX,
                fullName + ACTUAL_LOADER_SHARD_POSTFIX,
                fullName + BUFFER_LOADER_SHARD_POSTFIX
        ), this::dropTable)
                .compose(v -> sequenceAll(Arrays.asList( // 2. flush distributed tables
                        fullName + BUFFER_POSTFIX,
                        fullName + ACTUAL_POSTFIX), this::flushTable))
                .compose(v -> closeDeletedVersions(fullName, columnNames, primaryKeys, sysCn))  // 3. close deleted versions
                .compose(v -> closeByTableActual(fullName, columnNames, primaryKeys, sysCn))  // 4. close version by table actual
                .compose(v -> flushTable(fullName + ACTUAL_POSTFIX))  // 5. flush actual table
                .compose(v -> sequenceAll(Arrays.asList(  // 6. drop buffer tables
                        fullName + BUFFER_POSTFIX,
                        fullName + BUFFER_SHARD_POSTFIX), this::dropTable))
                .compose(v -> optimizeTable(fullName + ACTUAL_SHARD_POSTFIX))// 7. merge shards
                .compose(v -> {
                    final RestMppwKafkaStopRequest mppwKafkaStopRequest = new RestMppwKafkaStopRequest(
                            request.getRequestId().toString(),
                            request.getTopic());
                    log.debug("ADQM: Send mppw kafka stopping rest request {}", mppwKafkaStopRequest);
                    return restLoadClient.stopLoading(mppwKafkaStopRequest);
                })
                .compose(v -> {
                    reportFinish(request.getTopic());
                    return Future.succeededFuture();
                }, f -> {
                    reportError(request.getTopic());
                    return Future.failedFuture(f);
                });
    }

    private Future<Void> flushTable(String table) {
        return databaseExecutor.executeUpdate(adqmProcessingSqlFactory.getFlushSql(table));
    }

    private Future<Void> closeDeletedVersions(String table, String columnNames, String primaryKeys, long sysCn) {
        return databaseExecutor.executeUpdate(
                adqmProcessingSqlFactory.getCloseVersionSqlByTableBuffer(table, columnNames, primaryKeys, sysCn));
    }

    private Future<Void> closeByTableActual(String table, String columnNames, String primaryKeys, long sysCn) {
        return databaseExecutor.executeUpdate(
                adqmProcessingSqlFactory.getCloseVersionSqlByTableActual(table, columnNames, primaryKeys, sysCn));
    }

    private Future<Void> optimizeTable(String table) {
        return databaseExecutor.executeUpdate(adqmProcessingSqlFactory.getOptimizeSql(table));
    }

    private void reportFinish(String topic) {
        StatusReportDto start = new StatusReportDto(topic);
        statusReporter.onFinish(start);
    }

    private void reportError(String topic) {
        StatusReportDto start = new StatusReportDto(topic);
        statusReporter.onError(start);
    }
}
