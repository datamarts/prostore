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
import ru.datamart.prostore.query.execution.plugin.adb.base.factory.adqm.AdqmConnectorSqlFactory;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.adb.synchronize.service.PrepareQueriesOfChangesService;
import ru.datamart.prostore.query.execution.plugin.adb.synchronize.service.PrepareRequestOfChangesRequest;
import ru.datamart.prostore.query.execution.plugin.adb.synchronize.service.PrepareRequestOfChangesResult;
import ru.datamart.prostore.query.execution.plugin.api.service.shared.adqm.AdqmSharedService;
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
class AdqmSynchronizeDestinationExecutorTest {
    private static final String NEW_QUERY = "new query";
    private static final String DELETE_QUERY = "delete query";
    private static final String CREATE_EXTERNAL_TABLE_QUERY = "create external table query";
    private static final String CREATE_EXTERNAL_TABLE_PK_QUERY = "create external table pk query";
    private static final String DROP_EXTERNAL_TABLE_QUERY = "drop external table query";
    private static final String DROP_EXTERNAL_TABLE_PK_QUERY = "drop external table pk query";
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
    private static final String EXT_TABLE_PK_NAME = "extTablePkName";
    private static final String EXT_TABLE_NAME = "extTableName";

    @Mock
    private PrepareQueriesOfChangesService prepareQueriesOfChangesService;
    @Mock
    private DatabaseExecutor databaseExecutor;
    @Mock
    private AdqmConnectorSqlFactory synchronizeSqlFactory;
    @Mock
    private AdqmSharedService adqmSharedService;
    @InjectMocks
    private AdqmSynchronizeDestinationExecutor adqmSynchronizeDestinationExecutor;

    @Captor
    private ArgumentCaptor<PrepareRequestOfChangesRequest> requestOfChangesRequestArgumentCaptor;

    @BeforeEach
    void setUp() {
        lenient().when(prepareQueriesOfChangesService.prepare(any())).thenReturn(Future.succeededFuture(new PrepareRequestOfChangesResult(
                NEW_QUERY, DELETE_QUERY
        )));
        lenient().when(databaseExecutor.executeUpdate(Mockito.anyString())).thenReturn(Future.succeededFuture());
        lenient().when(synchronizeSqlFactory.createExternalTable(Mockito.anyString(), Mockito.anyString(), Mockito.any())).thenReturn(CREATE_EXTERNAL_TABLE_QUERY);
        lenient().when(synchronizeSqlFactory.createExternalPkOnlyTable(Mockito.anyString(), Mockito.anyString(), Mockito.any())).thenReturn(CREATE_EXTERNAL_TABLE_PK_QUERY);
        lenient().when(synchronizeSqlFactory.dropExternalTable(Mockito.eq(EXT_TABLE_NAME))).thenReturn(DROP_EXTERNAL_TABLE_QUERY);
        lenient().when(synchronizeSqlFactory.dropExternalTable(Mockito.eq(EXT_TABLE_PK_NAME))).thenReturn(DROP_EXTERNAL_TABLE_PK_QUERY);
        lenient().when(synchronizeSqlFactory.insertIntoExternalTable(Mockito.anyString(), Mockito.any(), eq(NEW_QUERY), anyLong())).thenReturn(INSERT_INTO_NEW_QUERY);
        lenient().when(synchronizeSqlFactory.insertIntoExternalTable(Mockito.anyString(), Mockito.any(), eq(DELETE_QUERY))).thenReturn(INSERT_INTO_DELETE_QUERY);
        lenient().when(synchronizeSqlFactory.extTableName(Mockito.any())).thenReturn(EXT_TABLE_NAME);
        lenient().when(synchronizeSqlFactory.extTablePkOnlyName(Mockito.any())).thenReturn(EXT_TABLE_PK_NAME);
        lenient().when(adqmSharedService.closeVersionSqlByTableActual(any(), any(), any(), anyLong())).thenReturn(Future.succeededFuture());
        lenient().when(adqmSharedService.closeVersionSqlByTableBuffer(any(), any(), any(), anyLong())).thenReturn(Future.succeededFuture());
        lenient().when(adqmSharedService.flushActualTable(any(), any(), any())).thenReturn(Future.succeededFuture());
        lenient().when(adqmSharedService.recreateBufferTables(any(), any(), any())).thenReturn(Future.succeededFuture());
        lenient().when(adqmSharedService.dropBufferTables(any(), any(), any())).thenReturn(Future.succeededFuture());
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
        Future<Long> result = adqmSynchronizeDestinationExecutor.execute(synchronizeRequest);

        // assert
        result.onComplete(ar -> {
            if (ar.failed()) {
                testContext.failNow(ar.cause());
                return;
            }

            testContext.verify(() -> {
                assertEquals(DELTA_NUM, ar.result());

                InOrder inOrder = inOrder(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adqmSharedService);

                // 1. assert buffer recreate
                inOrder.verify(adqmSharedService).recreateBufferTables(eq(ENV), eq(DATAMART), same(entity));

                // 2. assert request of changes
                inOrder.verify(prepareQueriesOfChangesService).prepare(requestOfChangesRequestArgumentCaptor.capture());
                PrepareRequestOfChangesRequest prepareRequestOfChangesRequest = requestOfChangesRequestArgumentCaptor.getValue();
                assertThat(prepareRequestOfChangesRequest.getDatamarts(), Matchers.contains(Matchers.sameInstance(dmrt1)));
                assertEquals(DELTA_NUM, prepareRequestOfChangesRequest.getDeltaToBe().getNum());
                assertEquals(DELTA_NUM_CN_TO, prepareRequestOfChangesRequest.getDeltaToBe().getCnTo());
                assertEquals(DELTA_NUM_CN_FROM, prepareRequestOfChangesRequest.getDeltaToBe().getCnFrom());
                assertEquals(PREVIOUS_DELTA_NUM_CN_TO, prepareRequestOfChangesRequest.getBeforeDeltaCnTo());
                assertEquals(ENV, prepareRequestOfChangesRequest.getEnvName());
                assertSame(VIEW_QUERY, prepareRequestOfChangesRequest.getViewQuery());

                // 3. assert drop external tables
                inOrder.verify(synchronizeSqlFactory).extTableName(same(entity));
                inOrder.verify(synchronizeSqlFactory).extTablePkOnlyName(same(entity));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_NAME));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_PK_NAME));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_QUERY));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_PK_QUERY));

                // 4. assert create external table
                inOrder.verify(synchronizeSqlFactory).createExternalTable(eq(ENV), eq(DATAMART), same(entity));
                inOrder.verify(synchronizeSqlFactory).createExternalPkOnlyTable(eq(ENV), eq(DATAMART), same(entity));
                inOrder.verify(databaseExecutor).executeUpdate(eq(CREATE_EXTERNAL_TABLE_QUERY));
                inOrder.verify(databaseExecutor).executeUpdate(eq(CREATE_EXTERNAL_TABLE_PK_QUERY));

                // 5. assert insert into
                inOrder.verify(synchronizeSqlFactory).insertIntoExternalTable(eq(DATAMART), same(entity), eq(DELETE_QUERY));
                inOrder.verify(databaseExecutor).executeUpdate(eq(INSERT_INTO_DELETE_QUERY));
                inOrder.verify(synchronizeSqlFactory).insertIntoExternalTable(eq(DATAMART), same(entity), eq(NEW_QUERY), eq(DELTA_NUM_CN_TO));
                inOrder.verify(databaseExecutor).executeUpdate(eq(INSERT_INTO_NEW_QUERY));

                // 6 assert ops of closing versions
                inOrder.verify(adqmSharedService).flushActualTable(eq(ENV), eq(DATAMART), same(entity));
                inOrder.verify(adqmSharedService).closeVersionSqlByTableBuffer(eq(ENV), eq(DATAMART), same(entity), eq(DELTA_NUM_CN_TO));
                inOrder.verify(adqmSharedService).closeVersionSqlByTableActual(eq(ENV), eq(DATAMART), same(entity), eq(DELTA_NUM_CN_TO));
                inOrder.verify(adqmSharedService).flushActualTable(eq(ENV), eq(DATAMART), same(entity));

                // 7 assert post drops
                inOrder.verify(synchronizeSqlFactory).extTableName(same(entity));
                inOrder.verify(synchronizeSqlFactory).extTablePkOnlyName(same(entity));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_NAME));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_PK_NAME));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_QUERY));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_PK_QUERY));
                inOrder.verify(adqmSharedService).dropBufferTables(eq(ENV), eq(DATAMART), same(entity));

                inOrder.verifyNoMoreInteractions();
                verifyNoMoreInteractions(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adqmSharedService);

            }).completeNow();
        });
    }

    @Test
    void shouldIgnoreOnCompleteDropExternalTableFailure(VertxTestContext testContext) {
        // arrange
        AtomicInteger callCount = new AtomicInteger();
        when(databaseExecutor.executeUpdate(eq(DROP_EXTERNAL_TABLE_QUERY))).thenAnswer(invocationOnMock -> {
            int currentCall = callCount.getAndIncrement();
            if (currentCall < 1) {
                return Future.succeededFuture();
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
        Future<Long> result = adqmSynchronizeDestinationExecutor.execute(synchronizeRequest);

        // assert
        result.onComplete(ar -> {
            if (ar.failed()) {
                testContext.failNow(ar.cause());
                return;
            }

            testContext.verify(() -> {
                assertEquals(DELTA_NUM, ar.result());

                InOrder inOrder = inOrder(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adqmSharedService);

                // 1. assert buffer recreate
                inOrder.verify(adqmSharedService).recreateBufferTables(eq(ENV), eq(DATAMART), same(entity));

                // 2. assert request of changes
                inOrder.verify(prepareQueriesOfChangesService).prepare(any());

                // 3. assert drop external tables
                inOrder.verify(synchronizeSqlFactory).extTableName(same(entity));
                inOrder.verify(synchronizeSqlFactory).extTablePkOnlyName(same(entity));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_NAME));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_PK_NAME));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_QUERY));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_PK_QUERY));

                // 4. assert create external table
                inOrder.verify(synchronizeSqlFactory).createExternalTable(eq(ENV), eq(DATAMART), same(entity));
                inOrder.verify(synchronizeSqlFactory).createExternalPkOnlyTable(eq(ENV), eq(DATAMART), same(entity));
                inOrder.verify(databaseExecutor).executeUpdate(eq(CREATE_EXTERNAL_TABLE_QUERY));
                inOrder.verify(databaseExecutor).executeUpdate(eq(CREATE_EXTERNAL_TABLE_PK_QUERY));

                // 5. assert insert into
                inOrder.verify(synchronizeSqlFactory).insertIntoExternalTable(eq(DATAMART), same(entity), eq(DELETE_QUERY));
                inOrder.verify(databaseExecutor).executeUpdate(eq(INSERT_INTO_DELETE_QUERY));
                inOrder.verify(synchronizeSqlFactory).insertIntoExternalTable(eq(DATAMART), same(entity), eq(NEW_QUERY), eq(DELTA_NUM_CN_TO));
                inOrder.verify(databaseExecutor).executeUpdate(eq(INSERT_INTO_NEW_QUERY));

                // 6 assert ops of closing versions
                inOrder.verify(adqmSharedService).flushActualTable(eq(ENV), eq(DATAMART), same(entity));
                inOrder.verify(adqmSharedService).closeVersionSqlByTableBuffer(eq(ENV), eq(DATAMART), same(entity), eq(DELTA_NUM_CN_TO));
                inOrder.verify(adqmSharedService).closeVersionSqlByTableActual(eq(ENV), eq(DATAMART), same(entity), eq(DELTA_NUM_CN_TO));
                inOrder.verify(adqmSharedService).flushActualTable(eq(ENV), eq(DATAMART), same(entity));

                // 7 assert post drops
                inOrder.verify(synchronizeSqlFactory).extTableName(same(entity));
                inOrder.verify(synchronizeSqlFactory).extTablePkOnlyName(same(entity));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_NAME));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_PK_NAME));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_QUERY));
                inOrder.verify(adqmSharedService).dropBufferTables(eq(ENV), eq(DATAMART), same(entity));

                inOrder.verifyNoMoreInteractions();
                verifyNoMoreInteractions(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adqmSharedService);

            }).completeNow();
        });
    }

    @Test
    void shouldIgnoreOnCompleteDropExternalPkTableFailure(VertxTestContext testContext) {
        // arrange
        AtomicInteger callCount = new AtomicInteger();
        when(databaseExecutor.executeUpdate(eq(DROP_EXTERNAL_TABLE_PK_QUERY))).thenAnswer(invocationOnMock -> {
            int currentCall = callCount.getAndIncrement();
            if (currentCall < 1) {
                return Future.succeededFuture();
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
        Future<Long> result = adqmSynchronizeDestinationExecutor.execute(synchronizeRequest);

        // assert
        result.onComplete(ar -> {
            if (ar.failed()) {
                testContext.failNow(ar.cause());
                return;
            }

            testContext.verify(() -> {
                assertEquals(DELTA_NUM, ar.result());

                InOrder inOrder = inOrder(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adqmSharedService);

                // 1. assert buffer recreate
                inOrder.verify(adqmSharedService).recreateBufferTables(eq(ENV), eq(DATAMART), same(entity));

                // 2. assert request of changes
                inOrder.verify(prepareQueriesOfChangesService).prepare(any());

                // 3. assert drop external tables
                inOrder.verify(synchronizeSqlFactory).extTableName(same(entity));
                inOrder.verify(synchronizeSqlFactory).extTablePkOnlyName(same(entity));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_NAME));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_PK_NAME));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_QUERY));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_PK_QUERY));

                // 4. assert create external table
                inOrder.verify(synchronizeSqlFactory).createExternalTable(eq(ENV), eq(DATAMART), same(entity));
                inOrder.verify(synchronizeSqlFactory).createExternalPkOnlyTable(eq(ENV), eq(DATAMART), same(entity));
                inOrder.verify(databaseExecutor).executeUpdate(eq(CREATE_EXTERNAL_TABLE_QUERY));
                inOrder.verify(databaseExecutor).executeUpdate(eq(CREATE_EXTERNAL_TABLE_PK_QUERY));

                // 5. assert insert into
                inOrder.verify(synchronizeSqlFactory).insertIntoExternalTable(eq(DATAMART), same(entity), eq(DELETE_QUERY));
                inOrder.verify(databaseExecutor).executeUpdate(eq(INSERT_INTO_DELETE_QUERY));
                inOrder.verify(synchronizeSqlFactory).insertIntoExternalTable(eq(DATAMART), same(entity), eq(NEW_QUERY), eq(DELTA_NUM_CN_TO));
                inOrder.verify(databaseExecutor).executeUpdate(eq(INSERT_INTO_NEW_QUERY));

                // 6 assert ops of closing versions
                inOrder.verify(adqmSharedService).flushActualTable(eq(ENV), eq(DATAMART), same(entity));
                inOrder.verify(adqmSharedService).closeVersionSqlByTableBuffer(eq(ENV), eq(DATAMART), same(entity), eq(DELTA_NUM_CN_TO));
                inOrder.verify(adqmSharedService).closeVersionSqlByTableActual(eq(ENV), eq(DATAMART), same(entity), eq(DELTA_NUM_CN_TO));
                inOrder.verify(adqmSharedService).flushActualTable(eq(ENV), eq(DATAMART), same(entity));

                // 7 assert post drops
                inOrder.verify(synchronizeSqlFactory).extTableName(same(entity));
                inOrder.verify(synchronizeSqlFactory).extTablePkOnlyName(same(entity));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_NAME));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_PK_NAME));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_QUERY));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_PK_QUERY));
                inOrder.verify(adqmSharedService).dropBufferTables(eq(ENV), eq(DATAMART), same(entity));

                inOrder.verifyNoMoreInteractions();
                verifyNoMoreInteractions(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adqmSharedService);

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
        Future<Long> result = adqmSynchronizeDestinationExecutor.execute(synchronizeRequest);

        // assert
        result.onComplete(ar -> {
            if (ar.succeeded()) {
                testContext.failNow(new AssertionError("Unexpected success"));
                return;
            }

            testContext.verify(() -> {
                assertEquals(DtmException.class, ar.cause().getClass());
                assertTrue(ar.cause().getMessage().contains("multiple datamarts"));

                InOrder inOrder = inOrder(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adqmSharedService);
                inOrder.verifyNoMoreInteractions();
                verifyNoMoreInteractions(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adqmSharedService);
            }).completeNow();
        });
    }

    @Test
    void shouldFailWhenRecreateBuffersFailed(VertxTestContext testContext) {
        // arrange
        when(adqmSharedService.recreateBufferTables(any(), any(), any())).thenReturn(Future.failedFuture(new DtmException("Failed")));

        UUID uuid = UUID.randomUUID();
        Datamart dmrt1 = Datamart.builder()
                .mnemonic(DATAMART)
                .build();
        List<Datamart> datamarts = Collections.singletonList(dmrt1);
        Entity entity = Entity.builder()
                .build();

        SynchronizeRequest synchronizeRequest = new SynchronizeRequest(uuid, ENV, DATAMART, datamarts, entity, VIEW_QUERY, new DeltaData(DELTA_NUM, DELTA_NUM_CN_FROM, DELTA_NUM_CN_TO), PREVIOUS_DELTA_NUM_CN_TO);

        // act
        Future<Long> result = adqmSynchronizeDestinationExecutor.execute(synchronizeRequest);

        // assert
        result.onComplete(ar -> {
            if (ar.succeeded()) {
                testContext.failNow(new AssertionError("Unexpected success"));
                return;
            }

            testContext.verify(() -> {
                assertEquals(DtmException.class, ar.cause().getClass());
                assertEquals("Failed", ar.cause().getMessage());

                InOrder inOrder = inOrder(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adqmSharedService);

                // 1. assert buffer recreate
                inOrder.verify(adqmSharedService).recreateBufferTables(eq(ENV), eq(DATAMART), same(entity));

                // 2 assert post drops
                inOrder.verify(synchronizeSqlFactory).extTableName(same(entity));
                inOrder.verify(synchronizeSqlFactory).extTablePkOnlyName(same(entity));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_NAME));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_PK_NAME));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_QUERY));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_PK_QUERY));
                inOrder.verify(adqmSharedService).dropBufferTables(eq(ENV), eq(DATAMART), same(entity));

                inOrder.verifyNoMoreInteractions();
                verifyNoMoreInteractions(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adqmSharedService);
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
        Future<Long> result = adqmSynchronizeDestinationExecutor.execute(synchronizeRequest);

        // assert
        result.onComplete(ar -> {
            if (ar.succeeded()) {
                testContext.failNow(new AssertionError("Unexpected success"));
                return;
            }

            testContext.verify(() -> {
                assertEquals(DtmException.class, ar.cause().getClass());
                assertEquals("Failed", ar.cause().getMessage());

                InOrder inOrder = inOrder(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adqmSharedService);

                // 1. assert buffer recreate
                inOrder.verify(adqmSharedService).recreateBufferTables(eq(ENV), eq(DATAMART), same(entity));

                // 2. assert request of changes
                inOrder.verify(prepareQueriesOfChangesService).prepare(any());

                // 3 assert post drops
                inOrder.verify(synchronizeSqlFactory).extTableName(same(entity));
                inOrder.verify(synchronizeSqlFactory).extTablePkOnlyName(same(entity));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_NAME));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_PK_NAME));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_QUERY));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_PK_QUERY));
                inOrder.verify(adqmSharedService).dropBufferTables(eq(ENV), eq(DATAMART), same(entity));

                inOrder.verifyNoMoreInteractions();
                verifyNoMoreInteractions(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adqmSharedService);
            }).completeNow();
        });
    }

    @Test
    void shouldFailWhenDropSqlFactoryThrowsException(VertxTestContext testContext) {
        // arrange
        AtomicInteger callCount = new AtomicInteger();
        when(synchronizeSqlFactory.dropExternalTable(Mockito.anyString())).thenAnswer(invocationOnMock -> {
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
        Future<Long> result = adqmSynchronizeDestinationExecutor.execute(synchronizeRequest);

        // assert
        result.onComplete(ar -> {
            if (ar.succeeded()) {
                testContext.failNow(new AssertionError("Unexpected success"));
                return;
            }

            testContext.verify(() -> {
                assertEquals(DtmException.class, ar.cause().getClass());
                assertEquals("Failed: 0", ar.cause().getMessage()); // ensure we see first failure

                InOrder inOrder = inOrder(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adqmSharedService);

                // 1. assert buffer recreate
                inOrder.verify(adqmSharedService).recreateBufferTables(eq(ENV), eq(DATAMART), same(entity));

                // 2. assert request of changes
                inOrder.verify(prepareQueriesOfChangesService).prepare(any());

                // 3. failed twice (on start and on end)
                inOrder.verify(synchronizeSqlFactory).extTableName(same(entity));
                inOrder.verify(synchronizeSqlFactory).extTablePkOnlyName(same(entity));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_NAME));
                inOrder.verify(synchronizeSqlFactory).extTableName(same(entity));
                inOrder.verify(synchronizeSqlFactory).extTablePkOnlyName(same(entity));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_NAME));
                inOrder.verify(adqmSharedService).dropBufferTables(eq(ENV), eq(DATAMART), same(entity));

                inOrder.verifyNoMoreInteractions();
                verifyNoMoreInteractions(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adqmSharedService);
            }).completeNow();
        });
    }

    @Test
    void shouldFailWhenDropExternalTableFailed(VertxTestContext testContext) {
        // arrange
        AtomicInteger callCount = new AtomicInteger();
        when(databaseExecutor.executeUpdate(eq(DROP_EXTERNAL_TABLE_QUERY))).thenAnswer(invocationOnMock ->
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
        Future<Long> result = adqmSynchronizeDestinationExecutor.execute(synchronizeRequest);

        // assert
        result.onComplete(ar -> {
            if (ar.succeeded()) {
                testContext.failNow(new AssertionError("Unexpected success"));
                return;
            }

            testContext.verify(() -> {
                assertEquals(DtmException.class, ar.cause().getClass());
                assertEquals("Failed: 0", ar.cause().getMessage()); // ensure we see first failure

                InOrder inOrder = inOrder(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adqmSharedService);

                // 1. assert buffer recreate
                inOrder.verify(adqmSharedService).recreateBufferTables(eq(ENV), eq(DATAMART), same(entity));

                // 2. assert request of changes
                inOrder.verify(prepareQueriesOfChangesService).prepare(any());

                // 3. failed twice (on start and on end)
                inOrder.verify(synchronizeSqlFactory).extTableName(same(entity));
                inOrder.verify(synchronizeSqlFactory).extTablePkOnlyName(same(entity));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_NAME));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_PK_NAME));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_QUERY));

                inOrder.verify(synchronizeSqlFactory).extTableName(same(entity));
                inOrder.verify(synchronizeSqlFactory).extTablePkOnlyName(same(entity));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_NAME));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_PK_NAME));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_QUERY));
                inOrder.verify(adqmSharedService).dropBufferTables(eq(ENV), eq(DATAMART), same(entity));

                inOrder.verifyNoMoreInteractions();
                verifyNoMoreInteractions(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adqmSharedService);
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
        Future<Long> result = adqmSynchronizeDestinationExecutor.execute(synchronizeRequest);

        // assert
        result.onComplete(ar -> {
            if (ar.succeeded()) {
                testContext.failNow(new AssertionError("Unexpected success"));
                return;
            }

            testContext.verify(() -> {
                assertEquals(DtmException.class, ar.cause().getClass());
                assertEquals("Failed", ar.cause().getMessage()); // ensure we see first failure

                InOrder inOrder = inOrder(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adqmSharedService);

                // 1. assert buffer recreate
                inOrder.verify(adqmSharedService).recreateBufferTables(eq(ENV), eq(DATAMART), same(entity));

                // 2. assert request of changes
                inOrder.verify(prepareQueriesOfChangesService).prepare(any());

                // 3. assert drop external tables
                inOrder.verify(synchronizeSqlFactory).extTableName(same(entity));
                inOrder.verify(synchronizeSqlFactory).extTablePkOnlyName(same(entity));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_NAME));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_PK_NAME));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_QUERY));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_PK_QUERY));

                // 4. assert create external table
                inOrder.verify(synchronizeSqlFactory).createExternalTable(eq(ENV), eq(DATAMART), same(entity));

                // 5 assert post drops
                inOrder.verify(synchronizeSqlFactory).extTableName(same(entity));
                inOrder.verify(synchronizeSqlFactory).extTablePkOnlyName(same(entity));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_NAME));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_PK_NAME));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_QUERY));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_PK_QUERY));
                inOrder.verify(adqmSharedService).dropBufferTables(eq(ENV), eq(DATAMART), same(entity));

                inOrder.verifyNoMoreInteractions();
                verifyNoMoreInteractions(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adqmSharedService);
            }).completeNow();
        });
    }

    @Test
    void shouldFailWhenCreateExternalTableExecuteFails(VertxTestContext testContext) {
        // arrange
        when(databaseExecutor.executeUpdate(eq(CREATE_EXTERNAL_TABLE_QUERY))).thenReturn(Future.failedFuture(new DtmException("Failed")));

        UUID uuid = UUID.randomUUID();
        Datamart dmrt1 = Datamart.builder()
                .mnemonic(DATAMART)
                .build();
        List<Datamart> datamarts = Collections.singletonList(dmrt1);
        Entity entity = Entity.builder()
                .build();

        SynchronizeRequest synchronizeRequest = new SynchronizeRequest(uuid, ENV, DATAMART, datamarts, entity, VIEW_QUERY, new DeltaData(DELTA_NUM, DELTA_NUM_CN_FROM, DELTA_NUM_CN_TO), PREVIOUS_DELTA_NUM_CN_TO);

        // act
        Future<Long> result = adqmSynchronizeDestinationExecutor.execute(synchronizeRequest);

        // assert
        result.onComplete(ar -> {
            if (ar.succeeded()) {
                testContext.failNow(new AssertionError("Unexpected success"));
                return;
            }

            testContext.verify(() -> {
                assertEquals(DtmException.class, ar.cause().getClass());
                assertEquals("Failed", ar.cause().getMessage()); // ensure we see first failure

                InOrder inOrder = inOrder(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adqmSharedService);

                // 1. assert buffer recreate
                inOrder.verify(adqmSharedService).recreateBufferTables(eq(ENV), eq(DATAMART), same(entity));

                // 2. assert request of changes
                inOrder.verify(prepareQueriesOfChangesService).prepare(any());

                // 3. assert drop external tables
                inOrder.verify(synchronizeSqlFactory).extTableName(same(entity));
                inOrder.verify(synchronizeSqlFactory).extTablePkOnlyName(same(entity));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_NAME));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_PK_NAME));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_QUERY));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_PK_QUERY));

                // 4. assert create external table
                inOrder.verify(synchronizeSqlFactory).createExternalTable(eq(ENV), eq(DATAMART), same(entity));
                inOrder.verify(synchronizeSqlFactory).createExternalPkOnlyTable(eq(ENV), eq(DATAMART), same(entity));
                inOrder.verify(databaseExecutor).executeUpdate(eq(CREATE_EXTERNAL_TABLE_QUERY));

                // 5 assert post drops
                inOrder.verify(synchronizeSqlFactory).extTableName(same(entity));
                inOrder.verify(synchronizeSqlFactory).extTablePkOnlyName(same(entity));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_NAME));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_PK_NAME));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_QUERY));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_PK_QUERY));
                inOrder.verify(adqmSharedService).dropBufferTables(eq(ENV), eq(DATAMART), same(entity));

                inOrder.verifyNoMoreInteractions();
                verifyNoMoreInteractions(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adqmSharedService);
            }).completeNow();
        });
    }

    @Test
    void shouldFailWhenDeletedInsertIntoSqlFactoryThrows(VertxTestContext testContext) {
        // arrange
        when(synchronizeSqlFactory.insertIntoExternalTable(any(), any(), eq(DELETE_QUERY))).thenThrow(new DtmException("Failed"));

        UUID uuid = UUID.randomUUID();
        Datamart dmrt1 = Datamart.builder()
                .mnemonic(DATAMART)
                .build();
        List<Datamart> datamarts = Collections.singletonList(dmrt1);
        Entity entity = Entity.builder()
                .build();

        SynchronizeRequest synchronizeRequest = new SynchronizeRequest(uuid, ENV, DATAMART, datamarts, entity, VIEW_QUERY, new DeltaData(DELTA_NUM, DELTA_NUM_CN_FROM, DELTA_NUM_CN_TO), PREVIOUS_DELTA_NUM_CN_TO);

        // act
        Future<Long> result = adqmSynchronizeDestinationExecutor.execute(synchronizeRequest);

        // assert
        result.onComplete(ar -> {
            if (ar.succeeded()) {
                testContext.failNow(new AssertionError("Unexpected success"));
                return;
            }

            testContext.verify(() -> {
                assertEquals(DtmException.class, ar.cause().getClass());
                assertEquals("Failed", ar.cause().getMessage()); // ensure we see first failure

                InOrder inOrder = inOrder(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adqmSharedService);

                // 1. assert buffer recreate
                inOrder.verify(adqmSharedService).recreateBufferTables(eq(ENV), eq(DATAMART), same(entity));

                // 2. assert request of changes
                inOrder.verify(prepareQueriesOfChangesService).prepare(any());

                // 3. assert drop external tables
                inOrder.verify(synchronizeSqlFactory).extTableName(same(entity));
                inOrder.verify(synchronizeSqlFactory).extTablePkOnlyName(same(entity));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_NAME));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_PK_NAME));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_QUERY));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_PK_QUERY));

                // 4. assert create external table
                inOrder.verify(synchronizeSqlFactory).createExternalTable(eq(ENV), eq(DATAMART), same(entity));
                inOrder.verify(synchronizeSqlFactory).createExternalPkOnlyTable(eq(ENV), eq(DATAMART), same(entity));
                inOrder.verify(databaseExecutor).executeUpdate(eq(CREATE_EXTERNAL_TABLE_QUERY));
                inOrder.verify(databaseExecutor).executeUpdate(eq(CREATE_EXTERNAL_TABLE_PK_QUERY));

                // 5. assert insert into
                inOrder.verify(synchronizeSqlFactory).insertIntoExternalTable(eq(DATAMART), same(entity), eq(DELETE_QUERY));

                // 7 assert post drops
                inOrder.verify(synchronizeSqlFactory).extTableName(same(entity));
                inOrder.verify(synchronizeSqlFactory).extTablePkOnlyName(same(entity));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_NAME));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_PK_NAME));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_QUERY));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_PK_QUERY));
                inOrder.verify(adqmSharedService).dropBufferTables(eq(ENV), eq(DATAMART), same(entity));

                inOrder.verifyNoMoreInteractions();
                verifyNoMoreInteractions(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adqmSharedService);
            }).completeNow();
        });
    }

    @Test
    void shouldFailWhenDeletedInsertIntoSqlExecuteFails(VertxTestContext testContext) {
        // arrange
        when(databaseExecutor.executeUpdate(eq(INSERT_INTO_DELETE_QUERY))).thenReturn(Future.failedFuture(new DtmException("Failed")));

        UUID uuid = UUID.randomUUID();
        Datamart dmrt1 = Datamart.builder()
                .mnemonic(DATAMART)
                .build();
        List<Datamart> datamarts = Collections.singletonList(dmrt1);
        Entity entity = Entity.builder()
                .build();

        SynchronizeRequest synchronizeRequest = new SynchronizeRequest(uuid, ENV, DATAMART, datamarts, entity, VIEW_QUERY, new DeltaData(DELTA_NUM, DELTA_NUM_CN_FROM, DELTA_NUM_CN_TO), PREVIOUS_DELTA_NUM_CN_TO);

        // act
        Future<Long> result = adqmSynchronizeDestinationExecutor.execute(synchronizeRequest);

        // assert
        result.onComplete(ar -> {
            if (ar.succeeded()) {
                testContext.failNow(new AssertionError("Unexpected success"));
                return;
            }

            testContext.verify(() -> {
                assertEquals(DtmException.class, ar.cause().getClass());
                assertEquals("Failed", ar.cause().getMessage()); // ensure we see first failure

                InOrder inOrder = inOrder(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adqmSharedService);

                // 1. assert buffer recreate
                inOrder.verify(adqmSharedService).recreateBufferTables(eq(ENV), eq(DATAMART), same(entity));

                // 2. assert request of changes
                inOrder.verify(prepareQueriesOfChangesService).prepare(any());

                // 3. assert drop external tables
                inOrder.verify(synchronizeSqlFactory).extTableName(same(entity));
                inOrder.verify(synchronizeSqlFactory).extTablePkOnlyName(same(entity));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_NAME));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_PK_NAME));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_QUERY));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_PK_QUERY));

                // 4. assert create external table
                inOrder.verify(synchronizeSqlFactory).createExternalTable(eq(ENV), eq(DATAMART), same(entity));
                inOrder.verify(synchronizeSqlFactory).createExternalPkOnlyTable(eq(ENV), eq(DATAMART), same(entity));
                inOrder.verify(databaseExecutor).executeUpdate(eq(CREATE_EXTERNAL_TABLE_QUERY));
                inOrder.verify(databaseExecutor).executeUpdate(eq(CREATE_EXTERNAL_TABLE_PK_QUERY));

                // 5. assert insert into
                inOrder.verify(synchronizeSqlFactory).insertIntoExternalTable(eq(DATAMART), same(entity), eq(DELETE_QUERY));
                inOrder.verify(databaseExecutor).executeUpdate(eq(INSERT_INTO_DELETE_QUERY));

                // 6 assert post drops
                inOrder.verify(synchronizeSqlFactory).extTableName(same(entity));
                inOrder.verify(synchronizeSqlFactory).extTablePkOnlyName(same(entity));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_NAME));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_PK_NAME));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_QUERY));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_PK_QUERY));
                inOrder.verify(adqmSharedService).dropBufferTables(eq(ENV), eq(DATAMART), same(entity));

                inOrder.verifyNoMoreInteractions();
                verifyNoMoreInteractions(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adqmSharedService);
            }).completeNow();
        });
    }

    @Test
    void shouldFailWhenNewInsertIntoSqlFactoryThrows(VertxTestContext testContext) {
        // arrange
        when(synchronizeSqlFactory.insertIntoExternalTable(any(), any(), eq(NEW_QUERY), anyLong())).thenThrow(new DtmException("Failed"));

        UUID uuid = UUID.randomUUID();

        Datamart dmrt1 = Datamart.builder()
                .mnemonic(DATAMART)
                .build();
        List<Datamart> datamarts = Collections.singletonList(dmrt1);
        Entity entity = Entity.builder()
                .build();

        SynchronizeRequest synchronizeRequest = new SynchronizeRequest(uuid, ENV, DATAMART, datamarts, entity, VIEW_QUERY, new DeltaData(DELTA_NUM, DELTA_NUM_CN_FROM, DELTA_NUM_CN_TO), PREVIOUS_DELTA_NUM_CN_TO);

        // act
        Future<Long> result = adqmSynchronizeDestinationExecutor.execute(synchronizeRequest);

        // assert
        result.onComplete(ar -> {
            if (ar.succeeded()) {
                testContext.failNow(new AssertionError("Unexpected success"));
                return;
            }

            testContext.verify(() -> {
                assertEquals(DtmException.class, ar.cause().getClass());
                assertEquals("Failed", ar.cause().getMessage()); // ensure we see first failure

                InOrder inOrder = inOrder(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adqmSharedService);

                // 1. assert buffer recreate
                inOrder.verify(adqmSharedService).recreateBufferTables(eq(ENV), eq(DATAMART), same(entity));

                // 2. assert request of changes
                inOrder.verify(prepareQueriesOfChangesService).prepare(any());

                // 3. assert drop external tables
                inOrder.verify(synchronizeSqlFactory).extTableName(same(entity));
                inOrder.verify(synchronizeSqlFactory).extTablePkOnlyName(same(entity));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_NAME));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_PK_NAME));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_QUERY));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_PK_QUERY));

                // 4. assert create external table
                inOrder.verify(synchronizeSqlFactory).createExternalTable(eq(ENV), eq(DATAMART), same(entity));
                inOrder.verify(synchronizeSqlFactory).createExternalPkOnlyTable(eq(ENV), eq(DATAMART), same(entity));
                inOrder.verify(databaseExecutor).executeUpdate(eq(CREATE_EXTERNAL_TABLE_QUERY));
                inOrder.verify(databaseExecutor).executeUpdate(eq(CREATE_EXTERNAL_TABLE_PK_QUERY));

                // 5. assert insert into
                inOrder.verify(synchronizeSqlFactory).insertIntoExternalTable(eq(DATAMART), same(entity), eq(DELETE_QUERY));
                inOrder.verify(databaseExecutor).executeUpdate(eq(INSERT_INTO_DELETE_QUERY));
                inOrder.verify(synchronizeSqlFactory).insertIntoExternalTable(eq(DATAMART), same(entity), eq(NEW_QUERY), eq(DELTA_NUM_CN_TO));

                // 6 assert post drops
                inOrder.verify(synchronizeSqlFactory).extTableName(same(entity));
                inOrder.verify(synchronizeSqlFactory).extTablePkOnlyName(same(entity));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_NAME));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_PK_NAME));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_QUERY));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_PK_QUERY));
                inOrder.verify(adqmSharedService).dropBufferTables(eq(ENV), eq(DATAMART), same(entity));

                inOrder.verifyNoMoreInteractions();
                verifyNoMoreInteractions(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adqmSharedService);
            }).completeNow();
        });
    }

    @Test
    void shouldFailWhenNewInsertIntoSqlExecuteFails(VertxTestContext testContext) {
        // arrange
        when(databaseExecutor.executeUpdate(eq(INSERT_INTO_NEW_QUERY))).thenReturn(Future.failedFuture(new DtmException("Failed")));

        UUID uuid = UUID.randomUUID();

        Datamart dmrt1 = Datamart.builder()
                .mnemonic(DATAMART)
                .build();
        List<Datamart> datamarts = Collections.singletonList(dmrt1);
        Entity entity = Entity.builder()
                .build();

        SynchronizeRequest synchronizeRequest = new SynchronizeRequest(uuid, ENV, DATAMART, datamarts, entity, VIEW_QUERY, new DeltaData(DELTA_NUM, DELTA_NUM_CN_FROM, DELTA_NUM_CN_TO), PREVIOUS_DELTA_NUM_CN_TO);

        // act
        Future<Long> result = adqmSynchronizeDestinationExecutor.execute(synchronizeRequest);

        // assert
        result.onComplete(ar -> {
            if (ar.succeeded()) {
                testContext.failNow(new AssertionError("Unexpected success"));
                return;
            }

            testContext.verify(() -> {
                assertEquals(DtmException.class, ar.cause().getClass());
                assertEquals("Failed", ar.cause().getMessage()); // ensure we see first failure

                InOrder inOrder = inOrder(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adqmSharedService);

                // 1. assert buffer recreate
                inOrder.verify(adqmSharedService).recreateBufferTables(eq(ENV), eq(DATAMART), same(entity));

                // 2. assert request of changes
                inOrder.verify(prepareQueriesOfChangesService).prepare(any());

                // 3. assert drop external tables
                inOrder.verify(synchronizeSqlFactory).extTableName(same(entity));
                inOrder.verify(synchronizeSqlFactory).extTablePkOnlyName(same(entity));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_NAME));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_PK_NAME));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_QUERY));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_PK_QUERY));

                // 4. assert create external table
                inOrder.verify(synchronizeSqlFactory).createExternalTable(eq(ENV), eq(DATAMART), same(entity));
                inOrder.verify(synchronizeSqlFactory).createExternalPkOnlyTable(eq(ENV), eq(DATAMART), same(entity));
                inOrder.verify(databaseExecutor).executeUpdate(eq(CREATE_EXTERNAL_TABLE_QUERY));
                inOrder.verify(databaseExecutor).executeUpdate(eq(CREATE_EXTERNAL_TABLE_PK_QUERY));

                // 5. assert insert into
                inOrder.verify(synchronizeSqlFactory).insertIntoExternalTable(eq(DATAMART), same(entity), eq(DELETE_QUERY));
                inOrder.verify(databaseExecutor).executeUpdate(eq(INSERT_INTO_DELETE_QUERY));
                inOrder.verify(synchronizeSqlFactory).insertIntoExternalTable(eq(DATAMART), same(entity), eq(NEW_QUERY), eq(DELTA_NUM_CN_TO));
                inOrder.verify(databaseExecutor).executeUpdate(eq(INSERT_INTO_NEW_QUERY));

                // 6 assert post drops
                inOrder.verify(synchronizeSqlFactory).extTableName(same(entity));
                inOrder.verify(synchronizeSqlFactory).extTablePkOnlyName(same(entity));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_NAME));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_PK_NAME));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_QUERY));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_PK_QUERY));
                inOrder.verify(adqmSharedService).dropBufferTables(eq(ENV), eq(DATAMART), same(entity));

                inOrder.verifyNoMoreInteractions();
                verifyNoMoreInteractions(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adqmSharedService);
            }).completeNow();
        });
    }

    @Test
    void shouldFailWhenFirstFlushActualTableFails(VertxTestContext testContext) {
        // arrange
        when(adqmSharedService.flushActualTable(any(), any(), any())).thenReturn(Future.failedFuture(new DtmException("Failed")));

        UUID uuid = UUID.randomUUID();

        Datamart dmrt1 = Datamart.builder()
                .mnemonic(DATAMART)
                .build();
        List<Datamart> datamarts = Collections.singletonList(dmrt1);
        Entity entity = Entity.builder()
                .build();

        SynchronizeRequest synchronizeRequest = new SynchronizeRequest(uuid, ENV, DATAMART, datamarts, entity, VIEW_QUERY, new DeltaData(DELTA_NUM, DELTA_NUM_CN_FROM, DELTA_NUM_CN_TO), PREVIOUS_DELTA_NUM_CN_TO);

        // act
        Future<Long> result = adqmSynchronizeDestinationExecutor.execute(synchronizeRequest);

        // assert
        result.onComplete(ar -> {
            if (ar.succeeded()) {
                testContext.failNow(new AssertionError("Unexpected success"));
                return;
            }

            testContext.verify(() -> {
                assertEquals(DtmException.class, ar.cause().getClass());
                assertEquals("Failed", ar.cause().getMessage()); // ensure we see first failure

                InOrder inOrder = inOrder(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adqmSharedService);

                // 1. assert buffer recreate
                inOrder.verify(adqmSharedService).recreateBufferTables(eq(ENV), eq(DATAMART), same(entity));

                // 2. assert request of changes
                inOrder.verify(prepareQueriesOfChangesService).prepare(any());

                // 3. assert drop external tables
                inOrder.verify(synchronizeSqlFactory).extTableName(same(entity));
                inOrder.verify(synchronizeSqlFactory).extTablePkOnlyName(same(entity));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_NAME));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_PK_NAME));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_QUERY));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_PK_QUERY));

                // 4. assert create external table
                inOrder.verify(synchronizeSqlFactory).createExternalTable(eq(ENV), eq(DATAMART), same(entity));
                inOrder.verify(synchronizeSqlFactory).createExternalPkOnlyTable(eq(ENV), eq(DATAMART), same(entity));
                inOrder.verify(databaseExecutor).executeUpdate(eq(CREATE_EXTERNAL_TABLE_QUERY));
                inOrder.verify(databaseExecutor).executeUpdate(eq(CREATE_EXTERNAL_TABLE_PK_QUERY));

                // 5. assert insert into
                inOrder.verify(synchronizeSqlFactory).insertIntoExternalTable(eq(DATAMART), same(entity), eq(DELETE_QUERY));
                inOrder.verify(databaseExecutor).executeUpdate(eq(INSERT_INTO_DELETE_QUERY));
                inOrder.verify(synchronizeSqlFactory).insertIntoExternalTable(eq(DATAMART), same(entity), eq(NEW_QUERY), eq(DELTA_NUM_CN_TO));
                inOrder.verify(databaseExecutor).executeUpdate(eq(INSERT_INTO_NEW_QUERY));

                // 6 assert ops of closing versions
                inOrder.verify(adqmSharedService).flushActualTable(eq(ENV), eq(DATAMART), same(entity));

                // 7 assert post drops
                inOrder.verify(synchronizeSqlFactory).extTableName(same(entity));
                inOrder.verify(synchronizeSqlFactory).extTablePkOnlyName(same(entity));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_NAME));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_PK_NAME));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_QUERY));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_PK_QUERY));
                inOrder.verify(adqmSharedService).dropBufferTables(eq(ENV), eq(DATAMART), same(entity));

                inOrder.verifyNoMoreInteractions();
                verifyNoMoreInteractions(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adqmSharedService);
            }).completeNow();
        });
    }

    @Test
    void shouldFailWhenCloseByTableBufferFails(VertxTestContext testContext) {
        // arrange
        when(adqmSharedService.closeVersionSqlByTableBuffer(any(), any(), any(), anyLong())).thenReturn(Future.failedFuture(new DtmException("Failed")));

        UUID uuid = UUID.randomUUID();

        Datamart dmrt1 = Datamart.builder()
                .mnemonic(DATAMART)
                .build();
        List<Datamart> datamarts = Collections.singletonList(dmrt1);
        Entity entity = Entity.builder()
                .build();

        SynchronizeRequest synchronizeRequest = new SynchronizeRequest(uuid, ENV, DATAMART, datamarts, entity, VIEW_QUERY, new DeltaData(DELTA_NUM, DELTA_NUM_CN_FROM, DELTA_NUM_CN_TO), PREVIOUS_DELTA_NUM_CN_TO);

        // act
        Future<Long> result = adqmSynchronizeDestinationExecutor.execute(synchronizeRequest);

        // assert
        result.onComplete(ar -> {
            if (ar.succeeded()) {
                testContext.failNow(new AssertionError("Unexpected success"));
                return;
            }

            testContext.verify(() -> {
                assertEquals(DtmException.class, ar.cause().getClass());
                assertEquals("Failed", ar.cause().getMessage()); // ensure we see first failure

                InOrder inOrder = inOrder(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adqmSharedService);

                // 1. assert buffer recreate
                inOrder.verify(adqmSharedService).recreateBufferTables(eq(ENV), eq(DATAMART), same(entity));

                // 2. assert request of changes
                inOrder.verify(prepareQueriesOfChangesService).prepare(any());

                // 3. assert drop external tables
                inOrder.verify(synchronizeSqlFactory).extTableName(same(entity));
                inOrder.verify(synchronizeSqlFactory).extTablePkOnlyName(same(entity));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_NAME));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_PK_NAME));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_QUERY));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_PK_QUERY));

                // 4. assert create external table
                inOrder.verify(synchronizeSqlFactory).createExternalTable(eq(ENV), eq(DATAMART), same(entity));
                inOrder.verify(synchronizeSqlFactory).createExternalPkOnlyTable(eq(ENV), eq(DATAMART), same(entity));
                inOrder.verify(databaseExecutor).executeUpdate(eq(CREATE_EXTERNAL_TABLE_QUERY));
                inOrder.verify(databaseExecutor).executeUpdate(eq(CREATE_EXTERNAL_TABLE_PK_QUERY));

                // 5. assert insert into
                inOrder.verify(synchronizeSqlFactory).insertIntoExternalTable(eq(DATAMART), same(entity), eq(DELETE_QUERY));
                inOrder.verify(databaseExecutor).executeUpdate(eq(INSERT_INTO_DELETE_QUERY));
                inOrder.verify(synchronizeSqlFactory).insertIntoExternalTable(eq(DATAMART), same(entity), eq(NEW_QUERY), eq(DELTA_NUM_CN_TO));
                inOrder.verify(databaseExecutor).executeUpdate(eq(INSERT_INTO_NEW_QUERY));

                // 6 assert ops of closing versions
                inOrder.verify(adqmSharedService).flushActualTable(eq(ENV), eq(DATAMART), same(entity));
                inOrder.verify(adqmSharedService).closeVersionSqlByTableBuffer(eq(ENV), eq(DATAMART), same(entity), eq(DELTA_NUM_CN_TO));

                // 7 assert post drops
                inOrder.verify(synchronizeSqlFactory).extTableName(same(entity));
                inOrder.verify(synchronizeSqlFactory).extTablePkOnlyName(same(entity));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_NAME));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_PK_NAME));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_QUERY));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_PK_QUERY));
                inOrder.verify(adqmSharedService).dropBufferTables(eq(ENV), eq(DATAMART), same(entity));

                inOrder.verifyNoMoreInteractions();
                verifyNoMoreInteractions(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adqmSharedService);
            }).completeNow();
        });
    }

    @Test
    void shouldFailWhenCloseByTableActualFails(VertxTestContext testContext) {
        // arrange
        when(adqmSharedService.closeVersionSqlByTableActual(any(), any(), any(), anyLong())).thenReturn(Future.failedFuture(new DtmException("Failed")));

        UUID uuid = UUID.randomUUID();

        Datamart dmrt1 = Datamart.builder()
                .mnemonic(DATAMART)
                .build();
        List<Datamart> datamarts = Collections.singletonList(dmrt1);
        Entity entity = Entity.builder()
                .build();

        SynchronizeRequest synchronizeRequest = new SynchronizeRequest(uuid, ENV, DATAMART, datamarts, entity, VIEW_QUERY, new DeltaData(DELTA_NUM, DELTA_NUM_CN_FROM, DELTA_NUM_CN_TO), PREVIOUS_DELTA_NUM_CN_TO);

        // act
        Future<Long> result = adqmSynchronizeDestinationExecutor.execute(synchronizeRequest);

        // assert
        result.onComplete(ar -> {
            if (ar.succeeded()) {
                testContext.failNow(new AssertionError("Unexpected success"));
                return;
            }

            testContext.verify(() -> {
                assertEquals(DtmException.class, ar.cause().getClass());
                assertEquals("Failed", ar.cause().getMessage()); // ensure we see first failure

                InOrder inOrder = inOrder(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adqmSharedService);

                // 1. assert buffer recreate
                inOrder.verify(adqmSharedService).recreateBufferTables(eq(ENV), eq(DATAMART), same(entity));

                // 2. assert request of changes
                inOrder.verify(prepareQueriesOfChangesService).prepare(any());

                // 3. assert drop external tables
                inOrder.verify(synchronizeSqlFactory).extTableName(same(entity));
                inOrder.verify(synchronizeSqlFactory).extTablePkOnlyName(same(entity));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_NAME));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_PK_NAME));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_QUERY));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_PK_QUERY));

                // 4. assert create external table
                inOrder.verify(synchronizeSqlFactory).createExternalTable(eq(ENV), eq(DATAMART), same(entity));
                inOrder.verify(synchronizeSqlFactory).createExternalPkOnlyTable(eq(ENV), eq(DATAMART), same(entity));
                inOrder.verify(databaseExecutor).executeUpdate(eq(CREATE_EXTERNAL_TABLE_QUERY));
                inOrder.verify(databaseExecutor).executeUpdate(eq(CREATE_EXTERNAL_TABLE_PK_QUERY));

                // 5. assert insert into
                inOrder.verify(synchronizeSqlFactory).insertIntoExternalTable(eq(DATAMART), same(entity), eq(DELETE_QUERY));
                inOrder.verify(databaseExecutor).executeUpdate(eq(INSERT_INTO_DELETE_QUERY));
                inOrder.verify(synchronizeSqlFactory).insertIntoExternalTable(eq(DATAMART), same(entity), eq(NEW_QUERY), eq(DELTA_NUM_CN_TO));
                inOrder.verify(databaseExecutor).executeUpdate(eq(INSERT_INTO_NEW_QUERY));

                // 6 assert ops of closing versions
                inOrder.verify(adqmSharedService).flushActualTable(eq(ENV), eq(DATAMART), same(entity));
                inOrder.verify(adqmSharedService).closeVersionSqlByTableBuffer(eq(ENV), eq(DATAMART), same(entity), eq(DELTA_NUM_CN_TO));
                inOrder.verify(adqmSharedService).closeVersionSqlByTableActual(eq(ENV), eq(DATAMART), same(entity), eq(DELTA_NUM_CN_TO));

                // 7 assert post drops
                inOrder.verify(synchronizeSqlFactory).extTableName(same(entity));
                inOrder.verify(synchronizeSqlFactory).extTablePkOnlyName(same(entity));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_NAME));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_PK_NAME));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_QUERY));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_PK_QUERY));
                inOrder.verify(adqmSharedService).dropBufferTables(eq(ENV), eq(DATAMART), same(entity));

                inOrder.verifyNoMoreInteractions();
                verifyNoMoreInteractions(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adqmSharedService);
            }).completeNow();
        });
    }

    @Test
    void shouldFailWhenSecondFlushActualTableFails(VertxTestContext testContext) {
        // arrange
        when(adqmSharedService.flushActualTable(any(), any(), any())).thenReturn(Future.succeededFuture()).thenReturn(Future.failedFuture(new DtmException("Failed")));

        UUID uuid = UUID.randomUUID();

        Datamart dmrt1 = Datamart.builder()
                .mnemonic(DATAMART)
                .build();
        List<Datamart> datamarts = Collections.singletonList(dmrt1);
        Entity entity = Entity.builder()
                .build();

        SynchronizeRequest synchronizeRequest = new SynchronizeRequest(uuid, ENV, DATAMART, datamarts, entity, VIEW_QUERY, new DeltaData(DELTA_NUM, DELTA_NUM_CN_FROM, DELTA_NUM_CN_TO), PREVIOUS_DELTA_NUM_CN_TO);

        // act
        Future<Long> result = adqmSynchronizeDestinationExecutor.execute(synchronizeRequest);

        // assert
        result.onComplete(ar -> {
            if (ar.succeeded()) {
                testContext.failNow(new AssertionError("Unexpected success"));
                return;
            }

            testContext.verify(() -> {
                assertEquals(DtmException.class, ar.cause().getClass());
                assertEquals("Failed", ar.cause().getMessage()); // ensure we see first failure

                InOrder inOrder = inOrder(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adqmSharedService);

                // 1. assert buffer recreate
                inOrder.verify(adqmSharedService).recreateBufferTables(eq(ENV), eq(DATAMART), same(entity));

                // 2. assert request of changes
                inOrder.verify(prepareQueriesOfChangesService).prepare(any());

                // 3. assert drop external tables
                inOrder.verify(synchronizeSqlFactory).extTableName(same(entity));
                inOrder.verify(synchronizeSqlFactory).extTablePkOnlyName(same(entity));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_NAME));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_PK_NAME));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_QUERY));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_PK_QUERY));

                // 4. assert create external table
                inOrder.verify(synchronizeSqlFactory).createExternalTable(eq(ENV), eq(DATAMART), same(entity));
                inOrder.verify(synchronizeSqlFactory).createExternalPkOnlyTable(eq(ENV), eq(DATAMART), same(entity));
                inOrder.verify(databaseExecutor).executeUpdate(eq(CREATE_EXTERNAL_TABLE_QUERY));
                inOrder.verify(databaseExecutor).executeUpdate(eq(CREATE_EXTERNAL_TABLE_PK_QUERY));

                // 5. assert insert into
                inOrder.verify(synchronizeSqlFactory).insertIntoExternalTable(eq(DATAMART), same(entity), eq(DELETE_QUERY));
                inOrder.verify(databaseExecutor).executeUpdate(eq(INSERT_INTO_DELETE_QUERY));
                inOrder.verify(synchronizeSqlFactory).insertIntoExternalTable(eq(DATAMART), same(entity), eq(NEW_QUERY), eq(DELTA_NUM_CN_TO));
                inOrder.verify(databaseExecutor).executeUpdate(eq(INSERT_INTO_NEW_QUERY));

                // 6 assert ops of closing versions
                inOrder.verify(adqmSharedService).flushActualTable(eq(ENV), eq(DATAMART), same(entity));
                inOrder.verify(adqmSharedService).closeVersionSqlByTableBuffer(eq(ENV), eq(DATAMART), same(entity), eq(DELTA_NUM_CN_TO));
                inOrder.verify(adqmSharedService).closeVersionSqlByTableActual(eq(ENV), eq(DATAMART), same(entity), eq(DELTA_NUM_CN_TO));
                inOrder.verify(adqmSharedService).flushActualTable(eq(ENV), eq(DATAMART), same(entity));

                // 7 assert post drops
                inOrder.verify(synchronizeSqlFactory).extTableName(same(entity));
                inOrder.verify(synchronizeSqlFactory).extTablePkOnlyName(same(entity));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_NAME));
                inOrder.verify(synchronizeSqlFactory).dropExternalTable(eq(EXT_TABLE_PK_NAME));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_QUERY));
                inOrder.verify(databaseExecutor).executeUpdate(eq(DROP_EXTERNAL_TABLE_PK_QUERY));
                inOrder.verify(adqmSharedService).dropBufferTables(eq(ENV), eq(DATAMART), same(entity));

                inOrder.verifyNoMoreInteractions();
                verifyNoMoreInteractions(prepareQueriesOfChangesService, synchronizeSqlFactory, databaseExecutor, adqmSharedService);
            }).completeNow();
        });
    }
}