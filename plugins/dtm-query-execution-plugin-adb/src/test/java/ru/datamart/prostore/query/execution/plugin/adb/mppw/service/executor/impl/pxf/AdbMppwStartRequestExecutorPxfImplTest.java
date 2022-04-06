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
package ru.datamart.prostore.query.execution.plugin.adb.mppw.service.executor.impl.pxf;

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
import ru.datamart.prostore.common.dto.KafkaBrokerInfo;
import ru.datamart.prostore.common.model.ddl.ExternalTableFormat;
import ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.factory.impl.KafkaMppwSqlFactoryImpl;
import ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.service.executor.impl.pxf.AdbMppwStartRequestExecutorPxfImpl;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.edml.BaseExternalEntityMetadata;
import ru.datamart.prostore.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;
import utils.CreateEntityUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

@ExtendWith({VertxExtension.class, MockitoExtension.class})
class AdbMppwStartRequestExecutorPxfImplTest {

    private static final DatabaseExecutor ADB_QUERY_EXECUTOR = mock(DatabaseExecutor.class);
    private static final UUID REQUEST_ID = UUID.fromString("cfb44c0d-8942-4965-ac54-8c78a3041f40");
    private static AdbMppwStartRequestExecutorPxfImpl startRequestExecutor;

    @Captor
    private ArgumentCaptor<String> queryCaptor;

    @BeforeAll
    static void setUp() {
        startRequestExecutor = new AdbMppwStartRequestExecutorPxfImpl(ADB_QUERY_EXECUTOR, new KafkaMppwSqlFactoryImpl(), 2000L);
    }

    @Test
    void executeSuccess(VertxTestContext testContext) {
        // arrange
        reset(ADB_QUERY_EXECUTOR);

        when(ADB_QUERY_EXECUTOR.executeUpdate(queryCaptor.capture()))
                .thenReturn(Future.succeededFuture())
                .thenReturn(Future.succeededFuture());

        // act
        startRequestExecutor.execute(getMppwKafkaRequest())
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    assertTrue(ar.succeeded());
                    assertEquals("kgw_test_test_schema.test_table_ext", ar.result());
                    verify(ADB_QUERY_EXECUTOR, times(2)).executeUpdate(anyString());
                    assertEquals("CREATE READABLE EXTERNAL TABLE test_schema.test_table_ext_cfb44c0d_8942_4965_ac54_8c78a3041f40_ext (VARCHAR_type varchar(20), CHAR_type varchar(20), BIGINT_type int8, INT_type int8, INT32_type int4, DOUBLE_type float8, FLOAT_type float4, DATE_type date, TIME_type timestamp(6), TIMESTAMP_type timestamp(6), BOOLEAN_type bool, UUID_type varchar(36), LINK_type varchar, id int8, sk_key2 int8, pk2 int8, sk_key3 int8, sys_op int) LOCATION ('pxf://topic?PROFILE=kafka-greenplum-writer&KAFKA_BROKERS=localhost:1234&CONSUMER_GROUP_NAME=kgw_test_test_schema.test_table_ext&POLL_TIMEOUT=2000')\n" +
                            "FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import')", queryCaptor.getAllValues().get(0));
                    assertEquals("INSERT INTO test_schema.test_table_staging (id, sk_key2, pk2, sk_key3, VARCHAR_type, CHAR_type, BIGINT_type, INT_type, INT32_type, DOUBLE_type, FLOAT_type, DATE_type, TIME_type, TIMESTAMP_type, BOOLEAN_type, UUID_type, LINK_type, sys_op) SELECT id, sk_key2, pk2, sk_key3, VARCHAR_type, CHAR_type, BIGINT_type, INT_type, INT32_type, DOUBLE_type, FLOAT_type, DATE_type, CAST(TIME_type AS TIME), TIMESTAMP_type, BOOLEAN_type, UUID_type, LINK_type, sys_op FROM test_schema.test_table_ext_cfb44c0d_8942_4965_ac54_8c78a3041f40_ext", queryCaptor.getAllValues().get(1));
                }).completeNow());
    }

    @Test
    void executeSuccessWhenWithoutSysOp(VertxTestContext testContext) {
        // arrange
        reset(ADB_QUERY_EXECUTOR);

        when(ADB_QUERY_EXECUTOR.executeUpdate(queryCaptor.capture()))
                .thenReturn(Future.succeededFuture())
                .thenReturn(Future.succeededFuture());

        val mppwKafkaRequest = getMppwKafkaRequest();
        mppwKafkaRequest.getSourceEntity().getExternalTableOptions().put("auto.create.sys_op.enable", "false");

        // act
        startRequestExecutor.execute(mppwKafkaRequest)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    assertTrue(ar.succeeded());
                    assertEquals("kgw_test_test_schema.test_table_ext", ar.result());
                    verify(ADB_QUERY_EXECUTOR, times(2)).executeUpdate(anyString());
                    assertEquals("CREATE READABLE EXTERNAL TABLE test_schema.test_table_ext_cfb44c0d_8942_4965_ac54_8c78a3041f40_ext (VARCHAR_type varchar(20), CHAR_type varchar(20), BIGINT_type int8, INT_type int8, INT32_type int4, DOUBLE_type float8, FLOAT_type float4, DATE_type date, TIME_type timestamp(6), TIMESTAMP_type timestamp(6), BOOLEAN_type bool, UUID_type varchar(36), LINK_type varchar, id int8, sk_key2 int8, pk2 int8, sk_key3 int8) LOCATION ('pxf://topic?PROFILE=kafka-greenplum-writer&KAFKA_BROKERS=localhost:1234&CONSUMER_GROUP_NAME=kgw_test_test_schema.test_table_ext&POLL_TIMEOUT=2000')\n" +
                            "FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import')", queryCaptor.getAllValues().get(0));
                    assertEquals("INSERT INTO test_schema.test_table_staging (id, sk_key2, pk2, sk_key3, VARCHAR_type, CHAR_type, BIGINT_type, INT_type, INT32_type, DOUBLE_type, FLOAT_type, DATE_type, TIME_type, TIMESTAMP_type, BOOLEAN_type, UUID_type, LINK_type) SELECT id, sk_key2, pk2, sk_key3, VARCHAR_type, CHAR_type, BIGINT_type, INT_type, INT32_type, DOUBLE_type, FLOAT_type, DATE_type, CAST(TIME_type AS TIME), TIMESTAMP_type, BOOLEAN_type, UUID_type, LINK_type FROM test_schema.test_table_ext_cfb44c0d_8942_4965_ac54_8c78a3041f40_ext", queryCaptor.getAllValues().get(1));
                }).completeNow());
    }

    @Test
    void executeCreateExtTableError(VertxTestContext testContext) {
        // arrange
        reset(ADB_QUERY_EXECUTOR);

        val error = "create ext table error";
        when(ADB_QUERY_EXECUTOR.executeUpdate(queryCaptor.capture()))
                .thenReturn(Future.failedFuture(error))
                .thenReturn(Future.succeededFuture());

        // act
        startRequestExecutor.execute(getMppwKafkaRequest())
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    assertTrue(ar.failed());
                    assertEquals(error, ar.cause().getMessage());
                    verify(ADB_QUERY_EXECUTOR, times(2)).executeUpdate(anyString());
                    assertEquals("CREATE READABLE EXTERNAL TABLE test_schema.test_table_ext_cfb44c0d_8942_4965_ac54_8c78a3041f40_ext (VARCHAR_type varchar(20), CHAR_type varchar(20), BIGINT_type int8, INT_type int8, INT32_type int4, DOUBLE_type float8, FLOAT_type float4, DATE_type date, TIME_type timestamp(6), TIMESTAMP_type timestamp(6), BOOLEAN_type bool, UUID_type varchar(36), LINK_type varchar, id int8, sk_key2 int8, pk2 int8, sk_key3 int8, sys_op int) LOCATION ('pxf://topic?PROFILE=kafka-greenplum-writer&KAFKA_BROKERS=localhost:1234&CONSUMER_GROUP_NAME=kgw_test_test_schema.test_table_ext&POLL_TIMEOUT=2000')\n" +
                            "FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import')", queryCaptor.getAllValues().get(0));
                    assertEquals("DROP EXTERNAL TABLE IF EXISTS test_schema.test_table_ext_cfb44c0d_8942_4965_ac54_8c78a3041f40_ext", queryCaptor.getAllValues().get(1));
                }).completeNow());
    }

    @Test
    void executeInsertError(VertxTestContext testContext) {
        // arrange
        reset(ADB_QUERY_EXECUTOR);

        val error = "insert error";
        when(ADB_QUERY_EXECUTOR.executeUpdate(queryCaptor.capture()))
                .thenReturn(Future.succeededFuture())
                .thenReturn(Future.failedFuture(error))
                .thenReturn(Future.succeededFuture());

        // act
        startRequestExecutor.execute(getMppwKafkaRequest())
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    assertTrue(ar.failed());
                    assertEquals(error, ar.cause().getMessage());
                    verify(ADB_QUERY_EXECUTOR, times(3)).executeUpdate(anyString());
                    assertEquals("CREATE READABLE EXTERNAL TABLE test_schema.test_table_ext_cfb44c0d_8942_4965_ac54_8c78a3041f40_ext (VARCHAR_type varchar(20), CHAR_type varchar(20), BIGINT_type int8, INT_type int8, INT32_type int4, DOUBLE_type float8, FLOAT_type float4, DATE_type date, TIME_type timestamp(6), TIMESTAMP_type timestamp(6), BOOLEAN_type bool, UUID_type varchar(36), LINK_type varchar, id int8, sk_key2 int8, pk2 int8, sk_key3 int8, sys_op int) LOCATION ('pxf://topic?PROFILE=kafka-greenplum-writer&KAFKA_BROKERS=localhost:1234&CONSUMER_GROUP_NAME=kgw_test_test_schema.test_table_ext&POLL_TIMEOUT=2000')\n" +
                            "FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import')", queryCaptor.getAllValues().get(0));
                    assertEquals("INSERT INTO test_schema.test_table_staging (id, sk_key2, pk2, sk_key3, VARCHAR_type, CHAR_type, BIGINT_type, INT_type, INT32_type, DOUBLE_type, FLOAT_type, DATE_type, TIME_type, TIMESTAMP_type, BOOLEAN_type, UUID_type, LINK_type, sys_op) SELECT id, sk_key2, pk2, sk_key3, VARCHAR_type, CHAR_type, BIGINT_type, INT_type, INT32_type, DOUBLE_type, FLOAT_type, DATE_type, CAST(TIME_type AS TIME), TIMESTAMP_type, BOOLEAN_type, UUID_type, LINK_type, sys_op FROM test_schema.test_table_ext_cfb44c0d_8942_4965_ac54_8c78a3041f40_ext", queryCaptor.getAllValues().get(1));
                    assertEquals("DROP EXTERNAL TABLE IF EXISTS test_schema.test_table_ext_cfb44c0d_8942_4965_ac54_8c78a3041f40_ext", queryCaptor.getAllValues().get(2));
                }).completeNow());
    }

    private MppwKafkaRequest getMppwKafkaRequest() {
        val sourceEntity = CreateEntityUtils.getEntity();
        sourceEntity.setName("test_table_ext");
        sourceEntity.setExternalTableOptions(new HashMap<>());
        return MppwKafkaRequest.builder()
                .uploadMetadata(new BaseExternalEntityMetadata("", "", ExternalTableFormat.AVRO, ""))
                .envName("test")
                .sourceEntity(sourceEntity)
                .destinationEntity(CreateEntityUtils.getEntity())
                .brokers(Collections.singletonList(new KafkaBrokerInfo("localhost", 1234)))
                .topic("topic")
                .requestId(REQUEST_ID)
                .build();
    }
}
