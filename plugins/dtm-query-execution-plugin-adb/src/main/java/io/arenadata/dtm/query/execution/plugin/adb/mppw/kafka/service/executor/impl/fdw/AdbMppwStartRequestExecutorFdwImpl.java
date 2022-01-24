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
package io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.service.executor.impl.fdw;

import io.arenadata.dtm.common.dto.KafkaBrokerInfo;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.EntityFieldUtils;
import io.arenadata.dtm.common.model.ddl.ExternalTableFormat;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.configuration.properties.AdbMppwProperties;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.dto.MppwKafkaLoadRequest;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.dto.MppwKafkaRequestContext;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.dto.MppwTopic;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.dto.TransferDataRequest;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.factory.KafkaMppwSqlFactory;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.factory.MppwKafkaLoadRequestFactory;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.factory.MppwTransferRequestFactory;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.service.executor.AdbMppwRequestExecutor;
import io.arenadata.dtm.query.execution.plugin.adb.query.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.api.exception.MppwDatasourceException;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@ConditionalOnProperty(
        value = "adb.mppw.usePxfConnector",
        havingValue = "false")
@Component("adbMppwStartRequestExecutor")
@Slf4j
public class AdbMppwStartRequestExecutorFdwImpl implements AdbMppwRequestExecutor {

    private final DatabaseExecutor adbQueryExecutor;
    private final KafkaMppwSqlFactory kafkaMppwSqlFactory;
    private final MppwTransferRequestFactory mppwTransferRequestFactory;
    private final MppwKafkaLoadRequestFactory mppwKafkaLoadRequestFactory;
    private final Vertx vertx;
    private final AdbMppwProperties mppwProperties;
    private final String dbName;

    @Autowired
    public AdbMppwStartRequestExecutorFdwImpl(@Qualifier("adbQueryExecutor") DatabaseExecutor adbQueryExecutor,
                                              KafkaMppwSqlFactory kafkaMppwSqlFactory,
                                              MppwTransferRequestFactory mppwTransferRequestFactory,
                                              MppwKafkaLoadRequestFactory mppwKafkaLoadRequestFactory,
                                              @Qualifier("coreVertx") Vertx vertx,
                                              AdbMppwProperties adbMppwProperties,
                                              @Value("${core.env.name}") String dbName) {
        this.adbQueryExecutor = adbQueryExecutor;
        this.kafkaMppwSqlFactory = kafkaMppwSqlFactory;
        this.mppwTransferRequestFactory = mppwTransferRequestFactory;
        this.mppwKafkaLoadRequestFactory = mppwKafkaLoadRequestFactory;
        this.vertx = vertx;
        this.mppwProperties = adbMppwProperties;
        this.dbName = dbName;
    }

    @Override
    public Future<String> execute(MppwKafkaRequest request) {
        val format = request.getUploadMetadata().getFormat();
        if (!ExternalTableFormat.AVRO.equals(format)) {
            return Future.failedFuture(new MppwDatasourceException(String.format("Format %s not implemented", format)));
        }
        return Future.future(promise -> {
            List<KafkaBrokerInfo> brokers = request.getBrokers();
            getOrCreateServer(brokers, dbName, request.getRequestId())
                    .compose(server -> createWritableExternalTable(request, server))
                    .map(server -> createMppwKafkaRequestContext(request, server))
                    .compose(kafkaContext -> moveOffsetsExtTable(request).map(v -> kafkaContext))
                    .onSuccess(kafkaContext -> {
                        vertx.eventBus().request(MppwTopic.KAFKA_START.getValue(), Json.encode(kafkaContext));
                        log.debug("Mppw started successfully");
                        promise.complete(mppwProperties.getConsumerGroup());
                    })
                    .onFailure(promise::fail);
        });
    }

    private Future<String> getOrCreateServer(List<KafkaBrokerInfo> brokers, String currentDatabase, UUID requestId) {
        val columnMetadata = Collections.singletonList(
                new ColumnMetadata("foreign_server_name", ColumnType.VARCHAR));
        val brokersList = brokers.stream()
                .map(KafkaBrokerInfo::getAddress)
                .collect(Collectors.joining(","));
        final String serverSqlQuery = kafkaMppwSqlFactory.checkServerSqlQuery(currentDatabase, brokersList);
        log.debug("Created check server for mppw query {}", serverSqlQuery);
        return adbQueryExecutor.execute(serverSqlQuery, columnMetadata)
                .compose(result -> {
                    if (result.isEmpty()) {
                        return createServer(brokersList, currentDatabase, requestId);
                    } else {
                        return Future.succeededFuture(result.get(0).get("foreign_server_name").toString());
                    }
                });
    }

    private Future<String> createServer(String brokersList, String currentDatabase, UUID requestId) {
        return adbQueryExecutor.executeUpdate(kafkaMppwSqlFactory.createServerSqlQuery(currentDatabase, requestId, brokersList))
                .map(v -> kafkaMppwSqlFactory.getServerName(currentDatabase, requestId));
    }

    private Future<String> createWritableExternalTable(MppwKafkaRequest request, String server) {
        val sourceEntity = request.getSourceEntity();
        val columns = kafkaMppwSqlFactory.getColumnsFromEntity(sourceEntity);
        columns.add("sys_op int");
        return adbQueryExecutor.executeUpdate(kafkaMppwSqlFactory.createWritableExtTableSqlQuery(server,
                columns,
                request,
                mppwProperties))
                .map(v -> server);
    }

    private Future<Void> moveOffsetsExtTable(MppwKafkaRequest request) {
        val schema = request.getDatamartMnemonic();
        val table = kafkaMppwSqlFactory.getTableName(request.getRequestId().toString());
        return adbQueryExecutor.executeUpdate(kafkaMppwSqlFactory.insertIntoKadbOffsetsSqlQuery(schema, table))
                .compose(v -> adbQueryExecutor.executeUpdate(kafkaMppwSqlFactory.moveOffsetsExtTableSqlQuery(schema, table)));
    }

    private MppwKafkaRequestContext createMppwKafkaRequestContext(MppwKafkaRequest request, String server) {
        final MppwKafkaLoadRequest mppwKafkaLoadRequest =
                mppwKafkaLoadRequestFactory.create(request, server, mppwProperties);
        final TransferDataRequest transferDataRequest =
                mppwTransferRequestFactory.create(request, EntityFieldUtils.getPkFieldNames(request.getDestinationEntity()));
        return new MppwKafkaRequestContext(mppwKafkaLoadRequest, transferDataRequest);
    }
}
