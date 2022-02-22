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
package ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.service.executor.impl.fdw;

import ru.datamart.prostore.query.execution.plugin.adb.mppw.configuration.properties.AdbMppwProperties;
import ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.dto.MppwTopic;
import ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.factory.KafkaMppwSqlFactory;
import ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.service.executor.AdbMppwRequestExecutor;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.exception.MppwDatasourceException;
import ru.datamart.prostore.query.execution.plugin.api.mppw.MppwRequest;
import ru.datamart.prostore.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@ConditionalOnProperty(
        value = "adb.mppw.usePxfConnector",
        havingValue = "false")
@Component("adbMppwStopRequestExecutor")
@Slf4j
public class AdbMppwStopRequestExecutorFdwImpl implements AdbMppwRequestExecutor {

    private final Vertx vertx;
    private final DatabaseExecutor adbQueryExecutor;
    private final KafkaMppwSqlFactory kafkaMppwSqlFactory;
    private final AdbMppwProperties mppwProperties;

    @Autowired
    public AdbMppwStopRequestExecutorFdwImpl(@Qualifier("coreVertx") Vertx vertx,
                                             @Qualifier("adbQueryExecutor") DatabaseExecutor adbQueryExecutor,
                                             KafkaMppwSqlFactory kafkaMppwSqlFactory,
                                             AdbMppwProperties adbMppwProperties) {
        this.vertx = vertx;
        this.adbQueryExecutor = adbQueryExecutor;
        this.kafkaMppwSqlFactory = kafkaMppwSqlFactory;
        this.mppwProperties = adbMppwProperties;
    }

    @Override
    public Future<String> execute(MppwKafkaRequest request) {
        return Future.future(promise -> vertx.eventBus().request(
                MppwTopic.KAFKA_STOP.getValue(),
                request.getRequestId().toString(),
                new DeliveryOptions().setSendTimeout(mppwProperties.getStopTimeoutMs()),
                ar -> dropExtTable(request)
                        .onSuccess(v -> {
                            if (ar.succeeded()) {
                                log.debug("Mppw kafka stopped successfully");
                                promise.complete();
                            } else {
                                promise.fail(new MppwDatasourceException("Error stopping mppw kafka", ar.cause()));
                            }
                        })
                        .onFailure(error -> promise.fail(new MppwDatasourceException("Error stopping mppw kafka", error)))));
    }

    private Future<Void> dropExtTable(MppwRequest request) {
        return Future.future(promise ->
                adbQueryExecutor.executeUpdate(kafkaMppwSqlFactory.dropForeignTableSqlQuery(request.getDatamartMnemonic(),
                        kafkaMppwSqlFactory.getTableName(request.getRequestId().toString())))
                        .onComplete(promise));
    }
}
