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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.dto.TransferDataRequest;
import ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.factory.KafkaMppwSqlFactory;
import ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.factory.MppwTransferRequestFactory;
import ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.service.executor.AdbMppwDataTransferService;
import ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.service.executor.impl.pxf.AdbMppwStopRequestExecutorPxfImpl;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;
import utils.CreateEntityUtils;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith({VertxExtension.class, MockitoExtension.class})
class AdbMppwStopRequestExecutorPxfImplTest {
    private static final UUID REQUEST_ID = UUID.fromString("cfb44c0d-8942-4965-ac54-8c78a3041f40");
    public static final String DROP_QUERY = "drop query";
    public static final String CHECK_QUERY = "check query";

    @Mock
    private DatabaseExecutor adbQueryExecutor;

    @Mock
    private KafkaMppwSqlFactory kafkaMppwSqlFactory;

    @Mock
    private MppwTransferRequestFactory mppwTransferRequestFactory;

    @Mock
    private AdbMppwDataTransferService mppwDataTransferService;

    @Captor
    private ArgumentCaptor<String> queryCaptor;

    @InjectMocks
    private AdbMppwStopRequestExecutorPxfImpl stopRequestExecutor;

    @Test
    void executeSuccess(VertxTestContext testContext) {
        when(kafkaMppwSqlFactory.dropExtTableSqlQuery(anyString(), anyString(), anyString())).thenReturn(DROP_QUERY);
        when(mppwTransferRequestFactory.create(any(), anyList())).thenReturn(new TransferDataRequest());
        when(mppwDataTransferService.execute(any())).thenReturn(Future.succeededFuture());
        when(kafkaMppwSqlFactory.checkStagingTableSqlQuery(anyString())).thenReturn(CHECK_QUERY);
        List<Map<String, Object>> resultList = new ArrayList<>();
        when(adbQueryExecutor.execute(queryCaptor.capture())).thenReturn(Future.succeededFuture(resultList));
        when(adbQueryExecutor.executeUpdate(queryCaptor.capture())).thenReturn(Future.succeededFuture());

        //act
        stopRequestExecutor.execute(getMppwKafkaRequest())
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    verify(adbQueryExecutor, times(2)).executeUpdate(anyString());
                    verify(adbQueryExecutor).execute(anyString());
                    verifyNoMoreInteractions(adbQueryExecutor);

                    assertEquals(DROP_QUERY, queryCaptor.getAllValues().get(0));
                    assertEquals(CHECK_QUERY, queryCaptor.getAllValues().get(1));
                    assertEquals("TRUNCATE test_schema.test_table_staging", queryCaptor.getAllValues().get(2));
                }).completeNow());
    }

    @Test
    void executeSuccessWithoutTransfer(VertxTestContext testContext) {
        when(kafkaMppwSqlFactory.dropExtTableSqlQuery(anyString(), anyString(), anyString())).thenReturn(DROP_QUERY);
        when(adbQueryExecutor.executeUpdate(queryCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mppwTransferRequestFactory.create(any(), anyList())).thenReturn(new TransferDataRequest());
        when(mppwDataTransferService.execute(any())).thenReturn(Future.succeededFuture());


        val mppwKafkaRequest = MppwKafkaRequest.builder()
                .sourceEntity(CreateEntityUtils.getEntity())
                .destinationEntity(CreateEntityUtils.getEntity())
                .requestId(REQUEST_ID)
                .sysCn(null)
                .build();

        //act
        stopRequestExecutor.execute(mppwKafkaRequest)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    verify(adbQueryExecutor).executeUpdate(anyString());
                    verifyNoMoreInteractions(adbQueryExecutor);

                    assertEquals(DROP_QUERY, queryCaptor.getAllValues().get(0));
                }).completeNow());
    }

    @Test
    void executeDropExtTableError(VertxTestContext testContext) {
        //drop ext table
        String errorMsg = "drop ext table error";
        when(kafkaMppwSqlFactory.dropExtTableSqlQuery(anyString(), anyString(), anyString())).thenReturn("");
        when(adbQueryExecutor.executeUpdate(anyString())).thenReturn(Future.failedFuture(errorMsg));
        when(adbQueryExecutor.execute(any())).thenReturn(Future.succeededFuture(Collections.emptyList()));

        //act
        stopRequestExecutor.execute(getMppwKafkaRequest())
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }
                    assertEquals(errorMsg, ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void executeCheckStagingNotEmptyResult(VertxTestContext testContext) {
        //drop ext table
        when(kafkaMppwSqlFactory.dropExtTableSqlQuery(anyString(), anyString(), anyString())).thenReturn("");
        when(adbQueryExecutor.executeUpdate(anyString())).thenReturn(Future.succeededFuture());

        //transfer data
        when(mppwTransferRequestFactory.create(any(), anyList())).thenReturn(new TransferDataRequest());
        when(mppwDataTransferService.execute(any())).thenReturn(Future.succeededFuture());

        //check staging
        when(kafkaMppwSqlFactory.checkStagingTableSqlQuery(anyString())).thenReturn("");
        List<Map<String, Object>> resultList = new ArrayList<>();
        resultList.add(new HashMap<>());
        when(adbQueryExecutor.execute(queryCaptor.capture())).thenReturn(Future.succeededFuture(resultList));

        //act
        stopRequestExecutor.execute(getMppwKafkaRequest())
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }
                    assertTrue(ar.cause().getMessage().contains("_staging is not empty after data transfer"));
                }).completeNow());
    }

    @Test
    void executeCheckStagingError(VertxTestContext testContext) {
        //drop ext table
        when(kafkaMppwSqlFactory.dropExtTableSqlQuery(anyString(), anyString(), anyString())).thenReturn("");
        when(adbQueryExecutor.executeUpdate(anyString())).thenReturn(Future.succeededFuture());

        //transfer data
        when(mppwTransferRequestFactory.create(any(), anyList())).thenReturn(new TransferDataRequest());
        when(mppwDataTransferService.execute(any())).thenReturn(Future.succeededFuture());

        //check staging
        String errorMsg = "check staging error";
        when(kafkaMppwSqlFactory.checkStagingTableSqlQuery(anyString())).thenReturn("");
        when(adbQueryExecutor.execute(queryCaptor.capture())).thenReturn(Future.failedFuture(errorMsg));

        //act
        stopRequestExecutor.execute(getMppwKafkaRequest())
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }
                    assertEquals(errorMsg, ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void executeTransferDataError(VertxTestContext testContext) {
        //drop ext table
        when(kafkaMppwSqlFactory.dropExtTableSqlQuery(anyString(), anyString(), anyString())).thenReturn("");
        when(adbQueryExecutor.executeUpdate(anyString())).thenReturn(Future.succeededFuture());
        when(adbQueryExecutor.execute(any())).thenReturn(Future.succeededFuture(Collections.emptyList()));

        //transfer data
        String errorMsg = "transfer data error";
        when(mppwTransferRequestFactory.create(any(), anyList())).thenReturn(new TransferDataRequest());
        when(mppwDataTransferService.execute(any())).thenReturn(Future.failedFuture(errorMsg));

        //act
        stopRequestExecutor.execute(getMppwKafkaRequest())
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }
                    assertEquals(errorMsg, ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void executeTruncateStagingError(VertxTestContext testContext) {
        //drop ext table
        String errorMsg = "truncate staging error";
        when(kafkaMppwSqlFactory.dropExtTableSqlQuery(anyString(), anyString(), anyString())).thenReturn("");
        when(adbQueryExecutor.executeUpdate(anyString()))
                .thenReturn(Future.succeededFuture())
                //truncate staging
                .thenReturn(Future.failedFuture(errorMsg));

        //transfer data
        when(mppwTransferRequestFactory.create(any(), anyList())).thenReturn(new TransferDataRequest());
        when(mppwDataTransferService.execute(any())).thenReturn(Future.succeededFuture());

        //check staging
        when(kafkaMppwSqlFactory.checkStagingTableSqlQuery(anyString())).thenReturn("");
        List<Map<String, Object>> resultList = new ArrayList<>();
        when(adbQueryExecutor.execute(queryCaptor.capture())).thenReturn(Future.succeededFuture(resultList));

        //act
        stopRequestExecutor.execute(getMppwKafkaRequest())
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }
                    assertEquals(errorMsg, ar.cause().getMessage());
                }).completeNow());
    }

    private MppwKafkaRequest getMppwKafkaRequest() {
        return MppwKafkaRequest.builder()
                .sourceEntity(CreateEntityUtils.getEntity())
                .destinationEntity(CreateEntityUtils.getEntity())
                .requestId(REQUEST_ID)
                .sysCn(1L)
                .build();
    }
}
