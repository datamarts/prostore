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
package ru.datamart.prostore.query.execution.plugin.adb.synchronize.executors.impl;

import ru.datamart.prostore.common.delta.DeltaData;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import ru.datamart.prostore.query.execution.plugin.adb.base.factory.adg.AdgConnectorSqlFactory;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.adb.synchronize.service.PrepareQueriesOfChangesService;
import ru.datamart.prostore.query.execution.plugin.adb.synchronize.service.PrepareRequestOfChangesRequest;
import ru.datamart.prostore.query.execution.plugin.adb.synchronize.service.PrepareRequestOfChangesResult;
import ru.datamart.prostore.query.execution.plugin.api.service.shared.adg.AdgSharedService;
import ru.datamart.prostore.query.execution.plugin.api.shared.adg.AdgSharedPrepareStagingRequest;
import ru.datamart.prostore.query.execution.plugin.api.shared.adg.AdgSharedTransferDataRequest;
import ru.datamart.prostore.query.execution.plugin.api.synchronize.SynchronizeRequest;
import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith({VertxExtension.class, MockitoExtension.class})
class AdgSynchronizeDestinationExecutorTest {
    private static final String NEW_QUERY = "new query";
    private static final String DELETE_QUERY = "delete query";
    private static final String CREATE_EXTERNAL_TABLE_QUERY = "create external table query";
    private static final String DROP_EXTERNAL_TABLE_QUERY = "drop external table query";
    private static final String INSERT_INTO_NEW_QUERY = "insert into " + NEW_QUERY;
    private static final String INSERT_INTO_DELETE_QUERY = "insert into " + DELETE_QUERY;
    private static final SqlNode VIEW_QUERY = new SqlNodeList(SqlParserPos.ZERO);
    private static final String ENV = "env";
    private static final String DATAMART = "datamart1";
    private static final String DATAMART_2 = "datamart2";
    private static final Long DELTA_NUM = 1L;
    private static final Long DELTA_NUM_CN_TO = 2L;
    private static final Long DELTA_NUM_CN_FROM = 0L;
    private static final Long PREVIOUS_DELTA_NUM_CN_TO = -1L;

    @Mock
    private PrepareQueriesOfChangesService prepareQueriesOfChangesService;
    @Mock
    private DatabaseExecutor databaseExecutor;
    @Mock
    private AdgConnectorSqlFactory synchronizeSqlFactory;
    @Mock
    private AdgSharedService adgSharedService;
    @InjectMocks
    private AdgSynchronizeDestinationExecutor adgSynchronizeDestinationExecutor;

    @Captor
    private ArgumentCaptor<PrepareRequestOfChangesRequest> requestOfChangesRequestArgumentCaptor;
    @Captor
    private ArgumentCaptor<AdgSharedPrepareStagingRequest> prepareStagingArgumentCaptor;
    @Captor
    private ArgumentCaptor<AdgSharedTransferDataRequest> transferDataRequestArgumentCaptor;

    @BeforeEach
    void setUp() {
        lenient().when(prepareQueriesOfChangesService.prepare(any())).thenReturn(Future.succeededFuture(new PrepareRequestOfChangesResult(
                NEW_QUERY, DELETE_QUERY
        )));
        lenient().when(databaseExecutor.execute(Mockito.anyString())).thenReturn(Future.succeededFuture(Collections.emptyList()));
        lenient().when(synchronizeSqlFactory.createExternalTable(Mockito.anyString(), Mockito.anyString(), Mockito.any())).thenReturn(CREATE_EXTERNAL_TABLE_QUERY);
        lenient().when(synchronizeSqlFactory.dropExternalTable(Mockito.anyString(), Mockito.any())).thenReturn(DROP_EXTERNAL_TABLE_QUERY);
        lenient().when(synchronizeSqlFactory.insertIntoExternalTable(Mockito.anyString(), Mockito.any(), eq(NEW_QUERY), Mockito.anyBoolean())).thenReturn(INSERT_INTO_NEW_QUERY);
        lenient().when(synchronizeSqlFactory.insertIntoExternalTable(Mockito.anyString(), Mockito.any(), eq(DELETE_QUERY), Mockito.anyBoolean())).thenReturn(INSERT_INTO_DELETE_QUERY);
        lenient().when(adgSharedService.prepareStaging(Mockito.any())).thenReturn(Future.succeededFuture());
        lenient().when(adgSharedService.transferData(Mockito.any())).thenReturn(Future.succeededFuture());
    }

    @Test
    void shouldSuccess(VertxTestContext testContext) {
        // arrange
        UUID uuid = UUID.randomUUID();

        Datamart dmrt1 = Datamart.builder()
                .mnemonic(DATAMART)
                .build();
        List<Datamart> datamarts = Collections.singletonList(dmrt1);
        Entity entity = Entity.builder()
                .build();

        SynchronizeRequest synchronizeRequest = new SynchronizeRequest(uuid, ENV, DATAMART, datamarts, entity, VIEW_QUERY, new DeltaData(DELTA_NUM, DELTA_NUM_CN_FROM, DELTA_NUM_CN_TO), PREVIOUS_DELTA_NUM_CN_TO);

        // act
        Future<Long> result = adgSynchronizeDestinationExecutor.execute(synchronizeRequest);

        // assert
        result.onComplete(ar -> {
            if (ar.failed()) {
                testContext.failNow(ar.cause());
                return;
            }

            testContext.verify(() -> {
                assertEquals(DELTA_NUM, ar.result());

                InOrder inOrder = inOrder(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adgSharedService);

                // 1. assert request of changes
                inOrder.verify(prepareQueriesOfChangesService).prepare(requestOfChangesRequestArgumentCaptor.capture());
                PrepareRequestOfChangesRequest prepareRequestOfChangesRequest = requestOfChangesRequestArgumentCaptor.getValue();
                assertThat(prepareRequestOfChangesRequest.getDatamarts(), Matchers.contains(Matchers.sameInstance(dmrt1)));
                assertEquals(DELTA_NUM, prepareRequestOfChangesRequest.getDeltaToBe().getNum());
                assertEquals(DELTA_NUM_CN_TO, prepareRequestOfChangesRequest.getDeltaToBe().getCnTo());
                assertEquals(DELTA_NUM_CN_FROM, prepareRequestOfChangesRequest.getDeltaToBe().getCnFrom());
                assertEquals(PREVIOUS_DELTA_NUM_CN_TO, prepareRequestOfChangesRequest.getBeforeDeltaCnTo());
                assertEquals(ENV, prepareRequestOfChangesRequest.getEnvName());
                assertSame(VIEW_QUERY, prepareRequestOfChangesRequest.getViewQuery());

                // 2. assert drop external table
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(DATAMART), same(entity));

                inOrder.verify(databaseExecutor).execute(eq(DROP_EXTERNAL_TABLE_QUERY));

                // 3. assert create external table
                inOrder.verify(synchronizeSqlFactory).createExternalTable(eq(ENV), eq(DATAMART), same(entity));
                inOrder.verify(databaseExecutor).execute(eq(CREATE_EXTERNAL_TABLE_QUERY));

                // 4. assert prepare staging
                inOrder.verify(adgSharedService).prepareStaging(prepareStagingArgumentCaptor.capture());
                AdgSharedPrepareStagingRequest prepareStagingRequest = prepareStagingArgumentCaptor.getValue();
                assertSame(entity, prepareStagingRequest.getEntity());
                assertEquals(ENV, prepareStagingRequest.getEnv());
                assertEquals(DATAMART, prepareStagingRequest.getDatamart());

                // 5. assert insert into
                inOrder.verify(synchronizeSqlFactory).insertIntoExternalTable(eq(DATAMART), same(entity), eq(DELETE_QUERY), eq(true));
                inOrder.verify(databaseExecutor).execute(eq(INSERT_INTO_DELETE_QUERY));

                inOrder.verify(synchronizeSqlFactory).insertIntoExternalTable(eq(DATAMART), same(entity), eq(NEW_QUERY), eq(false));
                inOrder.verify(databaseExecutor).execute(eq(INSERT_INTO_NEW_QUERY));

                // 6. assert transfer
                inOrder.verify(adgSharedService).transferData(transferDataRequestArgumentCaptor.capture());
                AdgSharedTransferDataRequest transferDataRequest = transferDataRequestArgumentCaptor.getValue();
                assertSame(entity, transferDataRequest.getEntity());
                assertEquals(ENV, transferDataRequest.getEnv());
                assertEquals(DATAMART, transferDataRequest.getDatamart());
                assertEquals(DELTA_NUM_CN_TO, transferDataRequest.getCnTo());

                // 7. assert drop external table
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(DATAMART), same(entity));
                inOrder.verify(databaseExecutor).execute(eq(DROP_EXTERNAL_TABLE_QUERY));

                inOrder.verifyNoMoreInteractions();
                verifyNoMoreInteractions(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adgSharedService);

            }).completeNow();
        });
    }

    @Test
    void shouldIgnoreOnCompleteDropFailure(VertxTestContext testContext) {
        // arrange
        AtomicInteger callCount = new AtomicInteger();
        when(databaseExecutor.execute(eq(DROP_EXTERNAL_TABLE_QUERY))).thenAnswer(invocationOnMock -> {
            int currentCall = callCount.getAndIncrement();
            if (currentCall == 0) {
                return Future.succeededFuture(Collections.emptyList());
            }
            return Future.failedFuture(new DtmException("Failed: " + currentCall));
        });

        UUID uuid = UUID.randomUUID();

        Datamart dmrt1 = Datamart.builder()
                .mnemonic(DATAMART)
                .build();
        List<Datamart> datamarts = Collections.singletonList(dmrt1);
        Entity entity = Entity.builder()
                .build();

        SynchronizeRequest synchronizeRequest = new SynchronizeRequest(uuid, ENV, DATAMART, datamarts, entity, VIEW_QUERY, new DeltaData(DELTA_NUM, DELTA_NUM_CN_FROM, DELTA_NUM_CN_TO), PREVIOUS_DELTA_NUM_CN_TO);

        // act
        Future<Long> result = adgSynchronizeDestinationExecutor.execute(synchronizeRequest);

        // assert
        result.onComplete(ar -> {
            if (ar.failed()) {
                testContext.failNow(ar.cause());
                return;
            }

            testContext.verify(() -> {
                assertEquals(DELTA_NUM, ar.result());

                InOrder inOrder = inOrder(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adgSharedService);

                // 1. assert request of changes
                inOrder.verify(prepareQueriesOfChangesService).prepare(any());

                // 2. assert drop external table
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(any(), any());
                inOrder.verify(databaseExecutor).execute(any());

                // 3. assert create external table
                inOrder.verify(synchronizeSqlFactory).createExternalTable(any(), any(), any());
                inOrder.verify(databaseExecutor).execute(any());

                // 4. assert prepare staging
                inOrder.verify(adgSharedService).prepareStaging(any());

                // 5. assert insert into
                inOrder.verify(synchronizeSqlFactory).insertIntoExternalTable(any(), any(), any(), eq(true));
                inOrder.verify(databaseExecutor).execute(any());
                inOrder.verify(synchronizeSqlFactory).insertIntoExternalTable(any(), any(), any(), eq(false));
                inOrder.verify(databaseExecutor).execute(any());

                // 6. assert transfer
                inOrder.verify(adgSharedService).transferData(any());

                // 7. assert drop external table
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(any(), any());
                inOrder.verify(databaseExecutor).execute(any());

                inOrder.verifyNoMoreInteractions();
                verifyNoMoreInteractions(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adgSharedService);

            }).completeNow();
        });
    }

    @Test
    void shouldFailWhenMultipleDatamarts(VertxTestContext testContext) {
        // arrange

        UUID uuid = UUID.randomUUID();
        Datamart dmrt1 = Datamart.builder()
                .mnemonic(DATAMART)
                .build();
        Datamart dmrt2 = Datamart.builder()
                .mnemonic(DATAMART_2)
                .build();
        List<Datamart> datamarts = Arrays.asList(dmrt1, dmrt2);
        Entity entity = Entity.builder()
                .build();

        SynchronizeRequest synchronizeRequest = new SynchronizeRequest(uuid, ENV, DATAMART, datamarts, entity, VIEW_QUERY, new DeltaData(DELTA_NUM, DELTA_NUM_CN_FROM, DELTA_NUM_CN_TO), PREVIOUS_DELTA_NUM_CN_TO);

        // act
        Future<Long> result = adgSynchronizeDestinationExecutor.execute(synchronizeRequest);

        // assert
        result.onComplete(ar -> {
            if (ar.succeeded()) {
                testContext.failNow(new AssertionError("Unexpected success"));
                return;
            }

            testContext.verify(() -> {
                assertEquals(DtmException.class, ar.cause().getClass());
                assertTrue(ar.cause().getMessage().contains("multiple datamarts"));

                InOrder inOrder = inOrder(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adgSharedService);
                inOrder.verifyNoMoreInteractions();
                verifyNoMoreInteractions(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adgSharedService);
            }).completeNow();
        });
    }

    @Test
    void shouldFailWhenPrepareRequestOfChangesFailed(VertxTestContext testContext) {
        // arrange
        when(prepareQueriesOfChangesService.prepare(any())).thenReturn(Future.failedFuture(new DtmException("Failed")));

        UUID uuid = UUID.randomUUID();
        Datamart dmrt1 = Datamart.builder()
                .mnemonic(DATAMART)
                .build();
        List<Datamart> datamarts = Collections.singletonList(dmrt1);
        Entity entity = Entity.builder()
                .build();

        SynchronizeRequest synchronizeRequest = new SynchronizeRequest(uuid, ENV, DATAMART, datamarts, entity, VIEW_QUERY, new DeltaData(DELTA_NUM, DELTA_NUM_CN_FROM, DELTA_NUM_CN_TO), PREVIOUS_DELTA_NUM_CN_TO);

        // act
        Future<Long> result = adgSynchronizeDestinationExecutor.execute(synchronizeRequest);

        // assert
        result.onComplete(ar -> {
            if (ar.succeeded()) {
                testContext.failNow(new AssertionError("Unexpected success"));
                return;
            }

            testContext.verify(() -> {
                assertEquals(DtmException.class, ar.cause().getClass());
                assertEquals("Failed", ar.cause().getMessage());

                InOrder inOrder = inOrder(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adgSharedService);

                // 1. assert request of changes
                inOrder.verify(prepareQueriesOfChangesService).prepare(any());

                // 2. assert drop external table
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(any(), any());
                inOrder.verify(databaseExecutor).execute(any());

                inOrder.verifyNoMoreInteractions();
                verifyNoMoreInteractions(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adgSharedService);
            }).completeNow();
        });
    }

    @Test
    void shouldFailWhenDropSqlFactoryThrowsException(VertxTestContext testContext) {
        // arrange
        AtomicInteger callCount = new AtomicInteger();
        when(synchronizeSqlFactory.dropExternalTable(Mockito.anyString(), Mockito.any())).thenAnswer(invocationOnMock -> {
            throw new DtmException("Failed: " + callCount.getAndIncrement());
        });

        UUID uuid = UUID.randomUUID();
        Datamart dmrt1 = Datamart.builder()
                .mnemonic(DATAMART)
                .build();
        List<Datamart> datamarts = Collections.singletonList(dmrt1);
        Entity entity = Entity.builder()
                .build();

        SynchronizeRequest synchronizeRequest = new SynchronizeRequest(uuid, ENV, DATAMART, datamarts, entity, VIEW_QUERY, new DeltaData(DELTA_NUM, DELTA_NUM_CN_FROM, DELTA_NUM_CN_TO), PREVIOUS_DELTA_NUM_CN_TO);

        // act
        Future<Long> result = adgSynchronizeDestinationExecutor.execute(synchronizeRequest);

        // assert
        result.onComplete(ar -> {
            if (ar.succeeded()) {
                testContext.failNow(new AssertionError("Unexpected success"));
                return;
            }

            testContext.verify(() -> {
                assertEquals(DtmException.class, ar.cause().getClass());
                assertEquals("Failed: 0", ar.cause().getMessage()); // ensure we see first failure

                InOrder inOrder = inOrder(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adgSharedService);

                // 1. assert request of changes
                inOrder.verify(prepareQueriesOfChangesService).prepare(any());

                // 2. failed twice (on start and on end)
                inOrder.verify(synchronizeSqlFactory, times(2)).dropExternalTable(any(), any());

                inOrder.verifyNoMoreInteractions();
                verifyNoMoreInteractions(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adgSharedService);
            }).completeNow();
        });
    }

    @Test
    void shouldFailWhenDropExternalTableFailed(VertxTestContext testContext) {
        // arrange
        AtomicInteger callCount = new AtomicInteger();
        when(databaseExecutor.execute(eq(DROP_EXTERNAL_TABLE_QUERY))).thenAnswer(invocationOnMock ->
                Future.failedFuture(new DtmException("Failed: " + callCount.getAndIncrement())));

        UUID uuid = UUID.randomUUID();
        Datamart dmrt1 = Datamart.builder()
                .mnemonic(DATAMART)
                .build();
        List<Datamart> datamarts = Collections.singletonList(dmrt1);
        Entity entity = Entity.builder()
                .build();

        SynchronizeRequest synchronizeRequest = new SynchronizeRequest(uuid, ENV, DATAMART, datamarts, entity, VIEW_QUERY, new DeltaData(DELTA_NUM, DELTA_NUM_CN_FROM, DELTA_NUM_CN_TO), PREVIOUS_DELTA_NUM_CN_TO);

        // act
        Future<Long> result = adgSynchronizeDestinationExecutor.execute(synchronizeRequest);

        // assert
        result.onComplete(ar -> {
            if (ar.succeeded()) {
                testContext.failNow(new AssertionError("Unexpected success"));
                return;
            }

            testContext.verify(() -> {
                assertEquals(DtmException.class, ar.cause().getClass());
                assertEquals("Failed: 0", ar.cause().getMessage()); // ensure we see first failure

                InOrder inOrder = inOrder(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adgSharedService);

                // 1. assert request of changes
                inOrder.verify(prepareQueriesOfChangesService).prepare(any());

                // 2. failed first
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(any(), any());
                inOrder.verify(databaseExecutor).execute(any());

                // 3. failed second
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(any(), any());
                inOrder.verify(databaseExecutor).execute(any());

                inOrder.verifyNoMoreInteractions();
                verifyNoMoreInteractions(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adgSharedService);
            }).completeNow();
        });
    }

    @Test
    void shouldFailWhenCreateExternalTableSqlFactoryThrows(VertxTestContext testContext) {
        // arrange
        when(synchronizeSqlFactory.createExternalTable(anyString(), anyString(), any())).thenThrow(new DtmException("Failed"));

        UUID uuid = UUID.randomUUID();
        Datamart dmrt1 = Datamart.builder()
                .mnemonic(DATAMART)
                .build();
        List<Datamart> datamarts = Collections.singletonList(dmrt1);
        Entity entity = Entity.builder()
                .build();

        SynchronizeRequest synchronizeRequest = new SynchronizeRequest(uuid, ENV, DATAMART, datamarts, entity, VIEW_QUERY, new DeltaData(DELTA_NUM, DELTA_NUM_CN_FROM, DELTA_NUM_CN_TO), PREVIOUS_DELTA_NUM_CN_TO);

        // act
        Future<Long> result = adgSynchronizeDestinationExecutor.execute(synchronizeRequest);

        // assert
        result.onComplete(ar -> {
            if (ar.succeeded()) {
                testContext.failNow(new AssertionError("Unexpected success"));
                return;
            }

            testContext.verify(() -> {
                assertEquals(DtmException.class, ar.cause().getClass());
                assertEquals("Failed", ar.cause().getMessage()); // ensure we see first failure

                InOrder inOrder = inOrder(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adgSharedService);

                // 1. assert request of changes
                inOrder.verify(prepareQueriesOfChangesService).prepare(any());

                // 2. assert drop external table
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(any(), any());
                inOrder.verify(databaseExecutor).execute(any());

                // 3. assert create external table
                inOrder.verify(synchronizeSqlFactory).createExternalTable(any(), any(), any());

                // 4. assert drop external table
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(any(), any());
                inOrder.verify(databaseExecutor).execute(any());

                inOrder.verifyNoMoreInteractions();
                verifyNoMoreInteractions(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adgSharedService);
            }).completeNow();
        });
    }

    @Test
    void shouldFailWhenCreateExternalTableExecuteFails(VertxTestContext testContext) {
        // arrange
        when(databaseExecutor.execute(eq(CREATE_EXTERNAL_TABLE_QUERY))).thenReturn(Future.failedFuture(new DtmException("Failed")));

        UUID uuid = UUID.randomUUID();
        Datamart dmrt1 = Datamart.builder()
                .mnemonic(DATAMART)
                .build();
        List<Datamart> datamarts = Collections.singletonList(dmrt1);
        Entity entity = Entity.builder()
                .build();

        SynchronizeRequest synchronizeRequest = new SynchronizeRequest(uuid, ENV, DATAMART, datamarts, entity, VIEW_QUERY, new DeltaData(DELTA_NUM, DELTA_NUM_CN_FROM, DELTA_NUM_CN_TO), PREVIOUS_DELTA_NUM_CN_TO);

        // act
        Future<Long> result = adgSynchronizeDestinationExecutor.execute(synchronizeRequest);

        // assert
        result.onComplete(ar -> {
            if (ar.succeeded()) {
                testContext.failNow(new AssertionError("Unexpected success"));
                return;
            }

            testContext.verify(() -> {
                assertEquals(DtmException.class, ar.cause().getClass());
                assertEquals("Failed", ar.cause().getMessage()); // ensure we see first failure

                InOrder inOrder = inOrder(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adgSharedService);

                // 1. assert request of changes
                inOrder.verify(prepareQueriesOfChangesService).prepare(any());

                // 2. assert drop external table
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(any(), any());
                inOrder.verify(databaseExecutor).execute(any());

                // 3. assert create external table
                inOrder.verify(synchronizeSqlFactory).createExternalTable(any(), any(), any());
                inOrder.verify(databaseExecutor).execute(any());

                // 4. assert drop external table
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(any(), any());
                inOrder.verify(databaseExecutor).execute(any());

                inOrder.verifyNoMoreInteractions();
                verifyNoMoreInteractions(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adgSharedService);
            }).completeNow();
        });
    }

    @Test
    void shouldFailWhenPrepareStagingFails(VertxTestContext testContext) {
        // arrange
        when(adgSharedService.prepareStaging(any())).thenReturn(Future.failedFuture(new DtmException("Failed")));

        UUID uuid = UUID.randomUUID();
        Datamart dmrt1 = Datamart.builder()
                .mnemonic(DATAMART)
                .build();
        List<Datamart> datamarts = Collections.singletonList(dmrt1);
        Entity entity = Entity.builder()
                .build();

        SynchronizeRequest synchronizeRequest = new SynchronizeRequest(uuid, ENV, DATAMART, datamarts, entity, VIEW_QUERY, new DeltaData(DELTA_NUM, DELTA_NUM_CN_FROM, DELTA_NUM_CN_TO), PREVIOUS_DELTA_NUM_CN_TO);

        // act
        Future<Long> result = adgSynchronizeDestinationExecutor.execute(synchronizeRequest);

        // assert
        result.onComplete(ar -> {
            if (ar.succeeded()) {
                testContext.failNow(new AssertionError("Unexpected success"));
                return;
            }

            testContext.verify(() -> {
                assertEquals(DtmException.class, ar.cause().getClass());
                assertEquals("Failed", ar.cause().getMessage()); // ensure we see first failure

                InOrder inOrder = inOrder(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adgSharedService);

                // 1. assert request of changes
                inOrder.verify(prepareQueriesOfChangesService).prepare(any());

                // 2. assert drop external table
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(any(), any());
                inOrder.verify(databaseExecutor).execute(any());

                // 3. assert create external table
                inOrder.verify(synchronizeSqlFactory).createExternalTable(any(), any(), any());
                inOrder.verify(databaseExecutor).execute(any());

                // 4. assert prepare staging
                inOrder.verify(adgSharedService).prepareStaging(any());

                // 5. assert drop external table
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(any(), any());
                inOrder.verify(databaseExecutor).execute(any());

                inOrder.verifyNoMoreInteractions();
                verifyNoMoreInteractions(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adgSharedService);
            }).completeNow();
        });
    }

    @Test
    void shouldFailWhenDeletedInsertIntoSqlFactoryThrows(VertxTestContext testContext) {
        // arrange
        when(synchronizeSqlFactory.insertIntoExternalTable(any(), any(), eq(DELETE_QUERY), anyBoolean())).thenThrow(new DtmException("Failed"));

        UUID uuid = UUID.randomUUID();
        Datamart dmrt1 = Datamart.builder()
                .mnemonic(DATAMART)
                .build();
        List<Datamart> datamarts = Collections.singletonList(dmrt1);
        Entity entity = Entity.builder()
                .build();

        SynchronizeRequest synchronizeRequest = new SynchronizeRequest(uuid, ENV, DATAMART, datamarts, entity, VIEW_QUERY, new DeltaData(DELTA_NUM, DELTA_NUM_CN_FROM, DELTA_NUM_CN_TO), PREVIOUS_DELTA_NUM_CN_TO);

        // act
        Future<Long> result = adgSynchronizeDestinationExecutor.execute(synchronizeRequest);

        // assert
        result.onComplete(ar -> {
            if (ar.succeeded()) {
                testContext.failNow(new AssertionError("Unexpected success"));
                return;
            }

            testContext.verify(() -> {
                assertEquals(DtmException.class, ar.cause().getClass());
                assertEquals("Failed", ar.cause().getMessage()); // ensure we see first failure

                InOrder inOrder = inOrder(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adgSharedService);

                // 1. assert request of changes
                inOrder.verify(prepareQueriesOfChangesService).prepare(any());

                // 2. assert drop external table
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(any(), any());
                inOrder.verify(databaseExecutor).execute(any());

                // 3. assert create external table
                inOrder.verify(synchronizeSqlFactory).createExternalTable(any(), any(), any());
                inOrder.verify(databaseExecutor).execute(any());

                // 4. assert prepare staging
                inOrder.verify(adgSharedService).prepareStaging(any());

                // 5. assert insert into
                inOrder.verify(synchronizeSqlFactory).insertIntoExternalTable(any(), any(), any(), anyBoolean());

                // 6. assert drop external table
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(any(), any());
                inOrder.verify(databaseExecutor).execute(any());

                inOrder.verifyNoMoreInteractions();
                verifyNoMoreInteractions(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adgSharedService);
            }).completeNow();
        });
    }

    @Test
    void shouldFailWhenDeletedInsertIntoSqlExecuteFails(VertxTestContext testContext) {
        // arrange
        when(databaseExecutor.execute(eq(INSERT_INTO_DELETE_QUERY))).thenReturn(Future.failedFuture(new DtmException("Failed")));

        UUID uuid = UUID.randomUUID();
        Datamart dmrt1 = Datamart.builder()
                .mnemonic(DATAMART)
                .build();
        List<Datamart> datamarts = Collections.singletonList(dmrt1);
        Entity entity = Entity.builder()
                .build();

        SynchronizeRequest synchronizeRequest = new SynchronizeRequest(uuid, ENV, DATAMART, datamarts, entity, VIEW_QUERY, new DeltaData(DELTA_NUM, DELTA_NUM_CN_FROM, DELTA_NUM_CN_TO), PREVIOUS_DELTA_NUM_CN_TO);

        // act
        Future<Long> result = adgSynchronizeDestinationExecutor.execute(synchronizeRequest);

        // assert
        result.onComplete(ar -> {
            if (ar.succeeded()) {
                testContext.failNow(new AssertionError("Unexpected success"));
                return;
            }

            testContext.verify(() -> {
                assertEquals(DtmException.class, ar.cause().getClass());
                assertEquals("Failed", ar.cause().getMessage()); // ensure we see first failure

                InOrder inOrder = inOrder(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adgSharedService);

                // 1. assert request of changes
                inOrder.verify(prepareQueriesOfChangesService).prepare(any());

                // 2. assert drop external table
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(any(), any());
                inOrder.verify(databaseExecutor).execute(any());

                // 3. assert create external table
                inOrder.verify(synchronizeSqlFactory).createExternalTable(any(), any(), any());
                inOrder.verify(databaseExecutor).execute(any());

                // 4. assert prepare staging
                inOrder.verify(adgSharedService).prepareStaging(any());

                // 5. assert insert into
                inOrder.verify(synchronizeSqlFactory).insertIntoExternalTable(any(), any(), any(), anyBoolean());
                inOrder.verify(databaseExecutor).execute(any());

                // 6. assert drop external table
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(any(), any());
                inOrder.verify(databaseExecutor).execute(any());

                inOrder.verifyNoMoreInteractions();
                verifyNoMoreInteractions(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adgSharedService);
            }).completeNow();
        });
    }

    @Test
    void shouldFailWhenNewInsertIntoSqlFactoryThrows(VertxTestContext testContext) {
        // arrange
        when(synchronizeSqlFactory.insertIntoExternalTable(any(), any(), eq(NEW_QUERY), anyBoolean())).thenThrow(new DtmException("Failed"));

        UUID uuid = UUID.randomUUID();

        Datamart dmrt1 = Datamart.builder()
                .mnemonic(DATAMART)
                .build();
        List<Datamart> datamarts = Collections.singletonList(dmrt1);
        Entity entity = Entity.builder()
                .build();

        SynchronizeRequest synchronizeRequest = new SynchronizeRequest(uuid, ENV, DATAMART, datamarts, entity, VIEW_QUERY, new DeltaData(DELTA_NUM, DELTA_NUM_CN_FROM, DELTA_NUM_CN_TO), PREVIOUS_DELTA_NUM_CN_TO);

        // act
        Future<Long> result = adgSynchronizeDestinationExecutor.execute(synchronizeRequest);

        // assert
        result.onComplete(ar -> {
            if (ar.succeeded()) {
                testContext.failNow(new AssertionError("Unexpected success"));
                return;
            }

            testContext.verify(() -> {
                assertEquals(DtmException.class, ar.cause().getClass());
                assertEquals("Failed", ar.cause().getMessage()); // ensure we see first failure

                InOrder inOrder = inOrder(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adgSharedService);

                // 1. assert request of changes
                inOrder.verify(prepareQueriesOfChangesService).prepare(any());

                // 2. assert drop external table
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(any(), any());
                inOrder.verify(databaseExecutor).execute(any());

                // 3. assert create external table
                inOrder.verify(synchronizeSqlFactory).createExternalTable(any(), any(), any());
                inOrder.verify(databaseExecutor).execute(any());

                // 4. assert prepare staging
                inOrder.verify(adgSharedService).prepareStaging(any());

                // 5. assert insert into
                inOrder.verify(synchronizeSqlFactory).insertIntoExternalTable(any(), any(), any(), anyBoolean());
                inOrder.verify(databaseExecutor).execute(any());
                inOrder.verify(synchronizeSqlFactory).insertIntoExternalTable(any(), any(), any(), anyBoolean());

                // 6. assert drop external table
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(any(), any());
                inOrder.verify(databaseExecutor).execute(any());

                inOrder.verifyNoMoreInteractions();
                verifyNoMoreInteractions(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adgSharedService);
            }).completeNow();
        });
    }

    @Test
    void shouldFailWhenNewInsertIntoSqlExecuteFails(VertxTestContext testContext) {
        // arrange
        when(databaseExecutor.execute(eq(INSERT_INTO_NEW_QUERY))).thenReturn(Future.failedFuture(new DtmException("Failed")));

        UUID uuid = UUID.randomUUID();

        Datamart dmrt1 = Datamart.builder()
                .mnemonic(DATAMART)
                .build();
        List<Datamart> datamarts = Collections.singletonList(dmrt1);
        Entity entity = Entity.builder()
                .build();

        SynchronizeRequest synchronizeRequest = new SynchronizeRequest(uuid, ENV, DATAMART, datamarts, entity, VIEW_QUERY, new DeltaData(DELTA_NUM, DELTA_NUM_CN_FROM, DELTA_NUM_CN_TO), PREVIOUS_DELTA_NUM_CN_TO);

        // act
        Future<Long> result = adgSynchronizeDestinationExecutor.execute(synchronizeRequest);

        // assert
        result.onComplete(ar -> {
            if (ar.succeeded()) {
                testContext.failNow(new AssertionError("Unexpected success"));
                return;
            }

            testContext.verify(() -> {
                assertEquals(DtmException.class, ar.cause().getClass());
                assertEquals("Failed", ar.cause().getMessage()); // ensure we see first failure

                InOrder inOrder = inOrder(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adgSharedService);

                // 1. assert request of changes
                inOrder.verify(prepareQueriesOfChangesService).prepare(any());

                // 2. assert drop external table
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(any(), any());
                inOrder.verify(databaseExecutor).execute(any());

                // 3. assert create external table
                inOrder.verify(synchronizeSqlFactory).createExternalTable(any(), any(), any());
                inOrder.verify(databaseExecutor).execute(any());

                // 4. assert prepare staging
                inOrder.verify(adgSharedService).prepareStaging(any());

                // 5. assert insert into
                inOrder.verify(synchronizeSqlFactory).insertIntoExternalTable(any(), any(), any(), anyBoolean());
                inOrder.verify(databaseExecutor).execute(any());
                inOrder.verify(synchronizeSqlFactory).insertIntoExternalTable(any(), any(), any(), anyBoolean());
                inOrder.verify(databaseExecutor).execute(any());

                // 6. assert drop external table
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(any(), any());
                inOrder.verify(databaseExecutor).execute(any());

                inOrder.verifyNoMoreInteractions();
                verifyNoMoreInteractions(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adgSharedService);
            }).completeNow();
        });
    }

    @Test
    void shouldFailWhenTransferDataFails(VertxTestContext testContext) {
        // arrange
        when(adgSharedService.transferData(any())).thenReturn(Future.failedFuture(new DtmException("Failed")));

        UUID uuid = UUID.randomUUID();

        Datamart dmrt1 = Datamart.builder()
                .mnemonic(DATAMART)
                .build();
        List<Datamart> datamarts = Collections.singletonList(dmrt1);
        Entity entity = Entity.builder()
                .build();

        SynchronizeRequest synchronizeRequest = new SynchronizeRequest(uuid, ENV, DATAMART, datamarts, entity, VIEW_QUERY, new DeltaData(DELTA_NUM, DELTA_NUM_CN_FROM, DELTA_NUM_CN_TO), PREVIOUS_DELTA_NUM_CN_TO);

        // act
        Future<Long> result = adgSynchronizeDestinationExecutor.execute(synchronizeRequest);

        // assert
        result.onComplete(ar -> {
            if (ar.succeeded()) {
                testContext.failNow(new AssertionError("Unexpected success"));
                return;
            }

            testContext.verify(() -> {
                assertEquals(DtmException.class, ar.cause().getClass());
                assertEquals("Failed", ar.cause().getMessage()); // ensure we see first failure

                InOrder inOrder = inOrder(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adgSharedService);

                // 1. assert request of changes
                inOrder.verify(prepareQueriesOfChangesService).prepare(any());

                // 2. assert drop external table
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(any(), any());
                inOrder.verify(databaseExecutor).execute(any());

                // 3. assert create external table
                inOrder.verify(synchronizeSqlFactory).createExternalTable(any(), any(), any());
                inOrder.verify(databaseExecutor).execute(any());

                // 4. assert prepare staging
                inOrder.verify(adgSharedService).prepareStaging(any());

                // 5. assert insert into
                inOrder.verify(synchronizeSqlFactory).insertIntoExternalTable(any(), any(), any(), anyBoolean());
                inOrder.verify(databaseExecutor).execute(any());
                inOrder.verify(synchronizeSqlFactory).insertIntoExternalTable(any(), any(), any(), anyBoolean());
                inOrder.verify(databaseExecutor).execute(any());

                // 6. assert transfer data
                inOrder.verify(adgSharedService).transferData(any());

                // 7. assert drop external table
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(any(), any());
                inOrder.verify(databaseExecutor).execute(any());

                inOrder.verifyNoMoreInteractions();
                verifyNoMoreInteractions(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adgSharedService);
            }).completeNow();
        });
    }
}