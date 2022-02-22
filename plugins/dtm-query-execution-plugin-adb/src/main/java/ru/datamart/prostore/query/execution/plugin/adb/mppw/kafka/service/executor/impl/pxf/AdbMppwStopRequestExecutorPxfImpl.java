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
package ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.service.executor.impl.pxf;

import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityFieldUtils;
import ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.factory.KafkaMppwSqlFactory;
import ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.factory.MppwTransferRequestFactory;
import ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.service.executor.AdbMppwDataTransferService;
import ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.service.executor.AdbMppwRequestExecutor;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.mppw.MppwRequest;
import ru.datamart.prostore.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@ConditionalOnProperty(
        value = "adb.mppw.usePxfConnector",
        havingValue = "true")
@Component("adbMppwStopRequestExecutor")
@Slf4j
public class AdbMppwStopRequestExecutorPxfImpl implements AdbMppwRequestExecutor {

    private final DatabaseExecutor adbQueryExecutor;
    private final KafkaMppwSqlFactory kafkaMppwSqlFactory;
    private final MppwTransferRequestFactory mppwTransferRequestFactory;
    private final AdbMppwDataTransferService mppwDataTransferService;

    @Autowired
    public AdbMppwStopRequestExecutorPxfImpl(@Qualifier("adbQueryExecutor") DatabaseExecutor adbQueryExecutor,
                                             KafkaMppwSqlFactory kafkaMppwSqlFactory,
                                             MppwTransferRequestFactory mppwTransferRequestFactory,
                                             AdbMppwDataTransferService mppwDataTransferService) {
        this.mppwTransferRequestFactory = mppwTransferRequestFactory;
        this.mppwDataTransferService = mppwDataTransferService;
        this.adbQueryExecutor = adbQueryExecutor;
        this.kafkaMppwSqlFactory = kafkaMppwSqlFactory;
    }

    @Override
    public Future<String> execute(MppwKafkaRequest request) {
        return Future.future(promise ->
                dropExtTable(request)
                        .compose(ignore -> mppwDataTransferService.execute(mppwTransferRequestFactory.create(request, EntityFieldUtils.getPkFieldNames(request.getDestinationEntity()))))
                        .compose(ignore -> checkStagingTable(request.getDestinationEntity()))
                        .onComplete(ar ->
                            adbQueryExecutor.executeUpdate(String.format("TRUNCATE %s_staging", request.getDestinationEntity().getNameWithSchema()))
                                    .onComplete(truncateResult -> {
                                        if (ar.failed()) {
                                            log.error("Error during stop ADB-MPPW", ar.cause());
                                            promise.fail(ar.cause());
                                        } else {
                                            if (truncateResult.failed()) {
                                                log.error("Error during TRUNCATE staging table on stop ADB-MPPW", truncateResult.cause());
                                                promise.fail(truncateResult.cause());
                                            } else {
                                                log.debug("Mppw kafka stopped successfully");
                                                promise.complete();
                                            }
                                        }
                                    })
                        )
        );
    }

    private Future<Void> dropExtTable(MppwRequest request) {
        return adbQueryExecutor.executeUpdate(kafkaMppwSqlFactory.dropExtTableSqlQuery(request.getSourceEntity().getSchema(),
                request.getSourceEntity().getName(),
                request.getRequestId().toString().replace("-", "_")));
    }

    private Future<List<Map<String, Object>>> checkStagingTable(Entity destinationEntity) {
        return adbQueryExecutor.execute(kafkaMppwSqlFactory.checkStagingTableSqlQuery(destinationEntity.getNameWithSchema()))
                .map(list -> {
                    if (!list.isEmpty()) {
                        throw new DtmException(String.format("The %s_staging is not empty after data transfer", destinationEntity.getNameWithSchema()));
                    }
                    return list;
                });
    }
}
