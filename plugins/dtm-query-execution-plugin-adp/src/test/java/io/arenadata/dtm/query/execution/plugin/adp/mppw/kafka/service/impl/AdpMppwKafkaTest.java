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
package io.arenadata.dtm.query.execution.plugin.adp.mppw.kafka.service.impl;

import io.arenadata.dtm.common.dto.KafkaBrokerInfo;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.ExternalTableFormat;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.adp.base.properties.AdpMppwProperties;
import io.arenadata.dtm.query.execution.plugin.adp.connector.service.AdpConnectorClient;
import io.arenadata.dtm.query.execution.plugin.adp.db.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adp.connector.dto.AdpConnectorMppwStartRequest;
import io.arenadata.dtm.query.execution.plugin.adp.connector.dto.AdpConnectorMppwStopRequest;
import io.arenadata.dtm.query.execution.plugin.adp.mppw.factory.AdpTransferDataSqlFactory;
import io.arenadata.dtm.query.execution.plugin.adp.mppw.transfer.AdpTransferDataService;
import io.arenadata.dtm.query.execution.plugin.api.exception.MppwDatasourceException;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.UploadExternalEntityMetadata;
import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.apache.avro.SchemaParseException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class AdpMppwKafkaTest {
    private static final String SCHEMA = "{\"type\":\"record\",\"name\":\"table\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}";
    private static final String ENV = "ENV";
    private static final String DATAMART = "DATAMART";
    private static final long SYS_CN = 10;
    private static final String TABLE_NAME = "table";
    private static final String TOPIC = "topic";
    private static final String BROKER_HOST = "localhost";
    private static final int BROKER_PORT = 2181;
    private static final String CONSUMER_GROUP = "CONSUMER_GROUP";
    private static final EntityField ID_PK_FIELD = EntityField.builder()
            .name("id")
            .type(ColumnType.BIGINT)
            .primaryOrder(1)
            .build();

    @Mock
    private DatabaseExecutor databaseExecutor;

    @Mock
    private AdpConnectorClient adpConnectorClient;

    @Mock
    private AdpMppwProperties adpMppwProperties;

    @Captor
    private ArgumentCaptor<AdpConnectorMppwStartRequest> startRequestCaptor;

    @Captor
    private ArgumentCaptor<AdpConnectorMppwStopRequest> stopRequestCaptor;

    @Captor
    private ArgumentCaptor<String> updateRequestCaptor;

    private AdpMppwKafkaExecutor adpMppwKafkaExecutor;

    @BeforeEach
    void setUp() {
        AdpTransferDataService adpTransferDataService = new AdpTransferDataService(new AdpTransferDataSqlFactory(), databaseExecutor);
        AdpStartMppwRequestExecutor adpStartMppwRequestExecutor = new AdpStartMppwRequestExecutor(adpConnectorClient, adpMppwProperties);
        AdpStopMppwRequestExecutor adpStopMppwRequestExecutor = new AdpStopMppwRequestExecutor(adpConnectorClient, adpTransferDataService);
        adpMppwKafkaExecutor = new AdpMppwKafkaExecutor(adpStartMppwRequestExecutor, adpStopMppwRequestExecutor);

        lenient().when(adpMppwProperties.getKafkaConsumerGroup()).thenReturn(CONSUMER_GROUP);
        lenient().when(adpConnectorClient.startMppw(Mockito.any())).thenReturn(Future.succeededFuture());
        lenient().when(adpConnectorClient.stopMppw(Mockito.any())).thenReturn(Future.succeededFuture());
        lenient().when(databaseExecutor.executeUpdate(Mockito.any())).thenReturn(Future.succeededFuture());
    }

    @Test
    void shouldSuccessfullyStartLoad(VertxTestContext vertxTestContext) {
        // arrange
        val requestId = UUID.randomUUID();
        val request = getRequest(requestId,true, createEntity(), ExternalTableFormat.AVRO, SCHEMA, Collections.singletonList(ID_PK_FIELD));

        // act
        Future<String> result = adpMppwKafkaExecutor.execute(request);

        // assert
        result.onComplete(vertxTestContext.succeeding(ar ->
                vertxTestContext.verify(() -> {
                    assertEquals(CONSUMER_GROUP, ar);
                    verify(adpConnectorClient).startMppw(startRequestCaptor.capture());
                    val capture = startRequestCaptor.getValue();
                    assertEquals(requestId.toString(), capture.getRequestId());
                    assertEquals(DATAMART, capture.getDatamart());
                    assertEquals(TABLE_NAME + "_staging", capture.getTableName());
                    assertEquals(1, capture.getKafkaBrokers().size());
                    assertEquals(BROKER_HOST + ":" + BROKER_PORT, capture.getKafkaBrokers().get(0).getAddress());
                    assertEquals(TOPIC, capture.getKafkaTopic());
                    assertEquals(CONSUMER_GROUP, capture.getConsumerGroup());
                    assertEquals(ExternalTableFormat.AVRO.getName(), capture.getFormat());
                    assertEquals(SCHEMA, capture.getSchema().toString());
                }).completeNow()));
}

    @Test
    void shouldFailStartWhenConnectorFailed(VertxTestContext vertxTestContext) {
        // arrange
        val request = getRequest(UUID.randomUUID(),true, createEntity(), ExternalTableFormat.AVRO, SCHEMA, Collections.singletonList(ID_PK_FIELD));

        reset(adpConnectorClient);
        when(adpConnectorClient.startMppw(Mockito.any())).thenReturn(Future.failedFuture(new RuntimeException("Exception")));

        // act
        Future<String> result = adpMppwKafkaExecutor.execute(request);

        // assert
        result.onComplete(vertxTestContext.failing(error ->
                vertxTestContext.verify(() -> assertSame(RuntimeException.class, error.getClass()))
                        .completeNow()));
    }

    @Test
    void shouldFailStartWhenWrongSchema(VertxTestContext vertxTestContext) {
        // arrange
        val request = getRequest(UUID.randomUUID(), true, createEntity(), ExternalTableFormat.AVRO, "{}", Collections.singletonList(ID_PK_FIELD));

        // act
        Future<String> result = adpMppwKafkaExecutor.execute(request);

        // assert
        result.onComplete(vertxTestContext.failing(error ->
                vertxTestContext.verify(() -> assertSame(SchemaParseException.class, error.getClass()))
                        .completeNow()));
    }

    @Test
    void shouldSuccessfullyStopLoad(VertxTestContext vertxTestContext) {
        // arrange
        UUID requestId = UUID.randomUUID();
        val request = getRequest(requestId, false, createEntity(), ExternalTableFormat.AVRO, SCHEMA, Collections.singletonList(ID_PK_FIELD));

        // act
        Future<String> result = adpMppwKafkaExecutor.execute(request);

        // assert
        result.onComplete(vertxTestContext.succeeding(ar ->
                vertxTestContext.verify(() -> {
                    assertNull(ar);
                    verify(adpConnectorClient).stopMppw(stopRequestCaptor.capture());
                    val capture = stopRequestCaptor.getValue();
                    assertEquals(requestId.toString(), capture.getRequestId());
                    assertEquals(TOPIC, capture.getKafkaTopic());

                    verify(databaseExecutor).executeUpdate(updateRequestCaptor.capture());
                    val value = updateRequestCaptor.getValue();
                    Assertions.assertThat(value).isEqualToNormalizingNewlines("UPDATE DATAMART.table_actual actual\n" +
                            "SET \n" +
                            "  sys_to = 9,\n" +
                            "  sys_op = staging.sys_op\n" +
                            "FROM (\n" +
                            "  SELECT id, MAX(sys_op) as sys_op\n" +
                            "  FROM DATAMART.table_staging\n" +
                            "  GROUP BY id\n" +
                            "    ) staging\n" +
                            "WHERE actual.id = staging.id \n" +
                            "  AND actual.sys_from < 10\n" +
                            "  AND actual.sys_to IS NULL;INSERT INTO DATAMART.table_actual (id, surname, age, sys_from, sys_op)\n" +
                            "  SELECT DISTINCT ON (staging.id) staging.id, staging.surname, staging.age, 10 AS sys_from, 0 AS sys_op \n" +
                            "  FROM DATAMART.table_staging staging\n" +
                            "    LEFT JOIN DATAMART.table_actual actual \n" +
                            "    ON actual.id = staging.id AND actual.sys_from = 10\n" +
                            "  WHERE actual.sys_from IS NULL AND staging.sys_op <> 1;TRUNCATE DATAMART.table_staging;");
                }).completeNow()));
    }

    @Test
    void shouldFailStopWhenConnectorFailed(VertxTestContext vertxTestContext) {
        // arrange
        val request = getRequest(UUID.randomUUID(), false, createEntity(), ExternalTableFormat.AVRO, SCHEMA, Collections.singletonList(ID_PK_FIELD));
        reset(adpConnectorClient);
        when(adpConnectorClient.stopMppw(Mockito.any())).thenReturn(Future.failedFuture(new RuntimeException("Exception")));

        // act
        Future<String> result = adpMppwKafkaExecutor.execute(request);

        // assert
        result.onComplete(vertxTestContext.failing(error ->
                vertxTestContext.verify(() -> assertSame(RuntimeException.class, error.getClass()))
                        .completeNow()));
    }

    @Test
    void shouldFailStopWhenDatabaseFailed(VertxTestContext vertxTestContext) {
        // arrange
        val request = getRequest(UUID.randomUUID(), false, createEntity(), ExternalTableFormat.AVRO, SCHEMA, Collections.singletonList(ID_PK_FIELD));
        reset(databaseExecutor);
        when(databaseExecutor.executeUpdate(Mockito.any())).thenReturn(Future.failedFuture(new RuntimeException("Exception")));

        // act
        Future<String> result = adpMppwKafkaExecutor.execute(request);

        // assert
        result.onComplete(vertxTestContext.failing(error ->
                vertxTestContext.verify(() -> assertSame(RuntimeException.class, error.getClass()))
                        .completeNow()));
    }

    @Test
    void shouldFailStopWhenNoFieldsInEntity(VertxTestContext vertxTestContext) {
        // arrange
        Entity entity = createEntity();
        entity.setFields(Collections.emptyList());
        val request = getRequest(UUID.randomUUID(), false, entity, ExternalTableFormat.AVRO, SCHEMA, Collections.singletonList(ID_PK_FIELD));

        // act
        Future<String> result = adpMppwKafkaExecutor.execute(request);

        // assert
        result.onComplete(vertxTestContext.failing(error ->
                vertxTestContext.verify(() -> assertSame(DtmException.class, error.getClass()))
                        .completeNow()));
    }

    @Test
    void shouldFailStopWhenNoPrimaryKeys(VertxTestContext vertxTestContext) {
        // arrange
        val request = getRequest(UUID.randomUUID(), false, createEntity(), ExternalTableFormat.AVRO, SCHEMA, Collections.emptyList());

        // act
        Future<String> result = adpMppwKafkaExecutor.execute(request);

        // assert
        result.onComplete(vertxTestContext.failing(error ->
                vertxTestContext.verify(() -> assertSame(DtmException.class, error.getClass()))
                        .completeNow()));
    }

    @Test
    void shouldFailUnsupportedFormat(VertxTestContext vertxTestContext) {
        //arrange
        val request = getRequest(UUID.randomUUID(), false, createEntity(), ExternalTableFormat.CSV, SCHEMA, Collections.emptyList());

        // act
        Future<String> result = adpMppwKafkaExecutor.execute(request);

        // assert
        result.onComplete(
                vertxTestContext.failing(error ->
                        vertxTestContext.verify(() -> assertSame(MppwDatasourceException.class, error.getClass())).completeNow())
        );
    }

    private MppwKafkaRequest getRequest(UUID requestId, boolean isLoadStart, Entity entity, ExternalTableFormat format, String schema, List<EntityField> primaryKeys) {
        val destEntity = Entity.builder()
                .name(TABLE_NAME)
                .fields(primaryKeys)
                .build();
        return new MppwKafkaRequest(requestId, ENV, DATAMART, isLoadStart,
                entity, SYS_CN, destEntity, new UploadExternalEntityMetadata("name", "path",
                format, schema, 1000),
                Collections.singletonList(new KafkaBrokerInfo(BROKER_HOST, BROKER_PORT)), TOPIC);
    }

    private Entity createEntity() {
        return Entity.builder()
                .name("table_ext")
                .fields(Arrays.asList(
                        EntityField.builder()
                                .name("id")
                                .type(ColumnType.BIGINT)
                                .build(),
                        EntityField.builder()
                                .name("surname")
                                .type(ColumnType.VARCHAR)
                                .build(),
                        EntityField.builder()
                                .name("age")
                                .type(ColumnType.INT32)
                                .build()
                ))
                .build();
    }

}