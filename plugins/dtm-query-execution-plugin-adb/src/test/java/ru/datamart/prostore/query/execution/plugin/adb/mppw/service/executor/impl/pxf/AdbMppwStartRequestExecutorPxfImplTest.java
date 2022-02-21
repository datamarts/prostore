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
package ru.datamart.prostore.query.execution.plugin.adb.mppw.service.executor.impl.pxf;

import ru.datamart.prostore.common.dto.KafkaBrokerInfo;
import ru.datamart.prostore.common.model.ddl.ExternalTableFormat;
import ru.datamart.prostore.query.execution.plugin.adb.mppw.configuration.properties.AdbMppwProperties;
import ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.factory.impl.KafkaMppwSqlFactoryImpl;
import ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.service.executor.impl.pxf.AdbMppwStartRequestExecutorPxfImpl;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.edml.BaseExternalEntityMetadata;
import ru.datamart.prostore.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;
import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.junit.jupiter.MockitoExtension;
import utils.CreateEntityUtils;

import java.util.Collections;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

@ExtendWith({VertxExtension.class, MockitoExtension.class})
class AdbMppwStartRequestExecutorPxfImplTest {

    private static final DatabaseExecutor ADB_QUERY_EXECUTOR = mock(DatabaseExecutor.class);
    private static final AdbMppwProperties MPPW_PROPERTIES = mock(AdbMppwProperties.class);
    private static AdbMppwStartRequestExecutorPxfImpl startRequestExecutor;

    @Captor
    private ArgumentCaptor<String> queryCaptor;

    @BeforeAll
    static void setUp() {
        startRequestExecutor = new AdbMppwStartRequestExecutorPxfImpl(ADB_QUERY_EXECUTOR, new KafkaMppwSqlFactoryImpl(), 2000L);
    }

    @Test
    void executeSuccess(VertxTestContext testContext) {
        reset(ADB_QUERY_EXECUTOR);

        when(ADB_QUERY_EXECUTOR.executeUpdate(queryCaptor.capture()))
                .thenReturn(Future.succeededFuture())
                .thenReturn(Future.succeededFuture());

        startRequestExecutor.execute(getMppwKafkaRequest())
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.succeeded());
                    assertEquals("kgw_test_test_schema.test_table_ext", ar.result());
                    assertThat(queryCaptor.getValue()).containsIgnoringCase("cast");
                    verify(ADB_QUERY_EXECUTOR, times(2)).executeUpdate(anyString());
                }).completeNow());
    }

    @Test
    void executeCreateExtTableError(VertxTestContext testContext) {
        reset(ADB_QUERY_EXECUTOR);

        val error = "create ext table error";
        when(ADB_QUERY_EXECUTOR.executeUpdate(queryCaptor.capture()))
                .thenReturn(Future.failedFuture(error))
                .thenReturn(Future.succeededFuture());

        startRequestExecutor.execute(getMppwKafkaRequest())
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.failed());
                    assertEquals(error, ar.cause().getMessage());
                    verify(ADB_QUERY_EXECUTOR, times(2)).executeUpdate(anyString());
                }).completeNow());
    }

    @Test
    void executeInsertError(VertxTestContext testContext) {
        reset(ADB_QUERY_EXECUTOR);

        val error = "insert error";
        when(ADB_QUERY_EXECUTOR.executeUpdate(queryCaptor.capture()))
                .thenReturn(Future.succeededFuture())
                .thenReturn(Future.failedFuture(error))
                .thenReturn(Future.succeededFuture());

        startRequestExecutor.execute(getMppwKafkaRequest())
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.failed());
                    assertEquals(error, ar.cause().getMessage());
                    verify(ADB_QUERY_EXECUTOR, times(3)).executeUpdate(anyString());
                }).completeNow());
    }

    private MppwKafkaRequest getMppwKafkaRequest() {
        val sourceEntity = CreateEntityUtils.getEntity();
        sourceEntity.setName("test_table_ext");
        return MppwKafkaRequest.builder()
                .uploadMetadata(new BaseExternalEntityMetadata("", "", ExternalTableFormat.AVRO, ""))
                .envName("test")
                .sourceEntity(sourceEntity)
                .destinationEntity(CreateEntityUtils.getEntity())
                .brokers(Collections.singletonList(new KafkaBrokerInfo("localhost", 1234)))
                .topic("topic")
                .requestId(UUID.randomUUID())
                .build();
    }
}
