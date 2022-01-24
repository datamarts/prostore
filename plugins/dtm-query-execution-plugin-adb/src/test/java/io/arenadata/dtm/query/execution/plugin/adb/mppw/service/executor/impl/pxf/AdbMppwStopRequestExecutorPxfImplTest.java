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
package io.arenadata.dtm.query.execution.plugin.adb.mppw.service.executor.impl.pxf;

import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.dto.TransferDataRequest;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.factory.KafkaMppwSqlFactory;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.factory.MppwTransferRequestFactory;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.service.executor.AdbMppwDataTransferService;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.service.executor.impl.pxf.AdbMppwStopRequestExecutorPxfImpl;
import io.arenadata.dtm.query.execution.plugin.adb.query.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;
import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import utils.CreateEntityUtils;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

@ExtendWith({VertxExtension.class, MockitoExtension.class})
class AdbMppwStopRequestExecutorPxfImplTest {

    @Mock
    private DatabaseExecutor adbQueryExecutor;

    @Mock
    private KafkaMppwSqlFactory kafkaMppwSqlFactory;

    @Mock
    private MppwTransferRequestFactory mppwTransferRequestFactory;

    @Mock
    private AdbMppwDataTransferService mppwDataTransferService;

    @InjectMocks
    private AdbMppwStopRequestExecutorPxfImpl stopRequestExecutor;

    @Test
    void executeSuccess(VertxTestContext testContext) {
        //drop ext table
        when(kafkaMppwSqlFactory.dropExtTableSqlQuery(anyString(), anyString(), anyString())).thenReturn("");
        when(adbQueryExecutor.executeUpdate(anyString())).thenReturn(Future.succeededFuture());

        //transfer data
        when(mppwTransferRequestFactory.create(any(), anyList())).thenReturn(new TransferDataRequest());
        when(mppwDataTransferService.execute(any())).thenReturn(Future.succeededFuture());

        //check staging
        when(kafkaMppwSqlFactory.checkStagingTableSqlQuery(anyString())).thenReturn("");
        List<Map<String, Object>> resultList = new ArrayList<>();
        when(adbQueryExecutor.execute(anyString())).thenReturn(Future.succeededFuture(resultList));

        //truncate staging
        when(adbQueryExecutor.executeUpdate(anyString())).thenReturn(Future.succeededFuture());

        //act
        stopRequestExecutor.execute(getMppwKafkaRequest())
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.succeeded());
                }).completeNow());
    }

    @Test
    void executeDropExtTableError(VertxTestContext testContext) {
        //drop ext table
        String errorMsg = "drop ext table error";
        when(kafkaMppwSqlFactory.dropExtTableSqlQuery(anyString(), anyString(), anyString())).thenReturn("");
        when(adbQueryExecutor.executeUpdate(anyString())).thenReturn(Future.failedFuture(errorMsg));

        //act
        stopRequestExecutor.execute(getMppwKafkaRequest())
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
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
        when(adbQueryExecutor.execute(anyString())).thenReturn(Future.succeededFuture(resultList));

        //act
        stopRequestExecutor.execute(getMppwKafkaRequest())
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
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
        when(adbQueryExecutor.execute(anyString())).thenReturn(Future.failedFuture(errorMsg));

        //act
        stopRequestExecutor.execute(getMppwKafkaRequest())
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertEquals(errorMsg, ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void executeTransferDataError(VertxTestContext testContext) {
        //drop ext table
        when(kafkaMppwSqlFactory.dropExtTableSqlQuery(anyString(), anyString(), anyString())).thenReturn("");
        when(adbQueryExecutor.executeUpdate(anyString())).thenReturn(Future.succeededFuture());

        //transfer data
        String errorMsg = "transfer data error";
        when(mppwTransferRequestFactory.create(any(), anyList())).thenReturn(new TransferDataRequest());
        when(mppwDataTransferService.execute(any())).thenReturn(Future.failedFuture(errorMsg));

        //act
        stopRequestExecutor.execute(getMppwKafkaRequest())
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
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
        when(adbQueryExecutor.execute(anyString())).thenReturn(Future.succeededFuture(resultList));

        //act
        stopRequestExecutor.execute(getMppwKafkaRequest())
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertEquals(errorMsg, ar.cause().getMessage());
                }).completeNow());
    }

    private MppwKafkaRequest getMppwKafkaRequest() {
        return MppwKafkaRequest.builder()
                .sourceEntity(CreateEntityUtils.getEntity())
                .destinationEntity(CreateEntityUtils.getEntity())
                .requestId(UUID.randomUUID())
                .build();
    }
}
