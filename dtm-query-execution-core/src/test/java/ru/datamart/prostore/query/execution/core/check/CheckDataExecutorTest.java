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
package ru.datamart.prostore.query.execution.core.check;

import ru.datamart.prostore.common.metrics.RequestMetrics;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.common.reader.QueryRequest;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.common.request.DatamartRequest;
import ru.datamart.prostore.query.calcite.core.extension.check.CheckType;
import ru.datamart.prostore.query.calcite.core.extension.check.SqlCheckData;
import ru.datamart.prostore.query.execution.core.base.exception.table.ColumnsNotExistsException;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.DatamartDao;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.EntityDao;
import ru.datamart.prostore.query.execution.core.check.dto.CheckContext;
import ru.datamart.prostore.query.execution.core.check.factory.CheckQueryResultFactory;
import ru.datamart.prostore.query.execution.core.check.service.impl.CheckDataExecutor;
import ru.datamart.prostore.query.execution.core.delta.dto.OkDelta;
import ru.datamart.prostore.query.execution.core.delta.exception.DeltaNotExistException;
import ru.datamart.prostore.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import ru.datamart.prostore.query.execution.core.plugin.service.DataSourcePluginService;
import ru.datamart.prostore.query.execution.core.plugin.service.impl.DataSourcePluginServiceImpl;
import io.vertx.core.Future;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.*;

class CheckDataExecutorTest {
    private final static String DATAMART_MNEMONIC = "schema";
    private final static String ENTITY_NAME = "entity";
    private final static String FIELD = "field";
    private final static String CHECK_RESULT_COLUMN = "check_result";
    private final static String CHECK_RESULT_VALUE = "OK";
    private final static Set<SourceType> SOURCE_TYPES = Stream.of(SourceType.ADB, SourceType.ADG, SourceType.ADQM)
            .collect(Collectors.toSet());
    private final DataSourcePluginService dataSourcePluginService = mock(DataSourcePluginServiceImpl.class);
    private final DeltaServiceDao deltaServiceDao = mock(DeltaServiceDao.class);
    private final EntityDao entityDao = mock(EntityDao.class);
    private final DatamartDao datamartDao = mock(DatamartDao.class);
    private final CheckQueryResultFactory queryResultFactory = mock(CheckQueryResultFactory.class);
    private final OkDelta okDelta = mock(OkDelta.class);
    private final CheckDataExecutor checkDataExecutor = new CheckDataExecutor(dataSourcePluginService, deltaServiceDao, entityDao, queryResultFactory);
    private Entity entity;

    @BeforeEach
    void setUp() {
        when(dataSourcePluginService.getSourceTypes()).thenReturn(SOURCE_TYPES);
        when(dataSourcePluginService.checkDataByHashInt32(any(), any(), any())).thenReturn(Future.succeededFuture(1L));
        when(dataSourcePluginService.checkDataByCount(any(), any(), any())).thenReturn(Future.succeededFuture(1L));
        when(datamartDao.getDatamart(DATAMART_MNEMONIC)).thenReturn(Future.succeededFuture(new byte[10]));

        when(okDelta.getCnFrom()).thenReturn(0L);
        when(okDelta.getCnTo()).thenReturn(1L);
        when(deltaServiceDao.getDeltaByNum(eq(DATAMART_MNEMONIC), anyLong())).thenReturn(Future.succeededFuture(okDelta));

        this.entity = Entity.builder()
                .schema(DATAMART_MNEMONIC)
                .entityType(EntityType.TABLE)
                .destination(SOURCE_TYPES)
                .name(ENTITY_NAME)
                .fields(Collections.singletonList(EntityField.builder()
                        .name(FIELD)
                        .build()))
                .build();
        when(entityDao.getEntity(DATAMART_MNEMONIC, this.entity.getName()))
                .thenReturn(Future.succeededFuture(this.entity));

        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put(CHECK_RESULT_COLUMN, CHECK_RESULT_VALUE);
        QueryResult queryResult = QueryResult.builder()
                .result(Collections.singletonList(resultMap))
                .build();
        when(queryResultFactory.create(anyString())).thenReturn(queryResult);
    }

    @Test
    void testCheckByHashSuccess() {
        when(deltaServiceDao.getDeltaOk(DATAMART_MNEMONIC)).thenReturn(Future.succeededFuture(okDelta));
        when(deltaServiceDao.getDeltaByNum(eq(DATAMART_MNEMONIC), anyLong())).thenReturn(Future.succeededFuture(okDelta));
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setDatamartMnemonic(DATAMART_MNEMONIC);
        SqlCheckData sqlCheckData = mock(SqlCheckData.class);
        when(sqlCheckData.getColumns()).thenReturn(Stream.of("field").collect(Collectors.toSet()));
        when(sqlCheckData.getDeltaNum()).thenReturn(0L);
        when(sqlCheckData.getTable()).thenReturn(entity.getName());
        CheckContext checkContext = new CheckContext(new RequestMetrics(), "env",
                new DatamartRequest(queryRequest), CheckType.DATA, sqlCheckData);
        checkDataExecutor.execute(checkContext)
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    assertEquals(CHECK_RESULT_VALUE, ar.result().getResult().get(0).get(CHECK_RESULT_COLUMN));
                    SOURCE_TYPES.forEach(sourceType ->
                            verify(dataSourcePluginService)
                                    .checkDataByHashInt32(eq(sourceType), any(),
                                            argThat(request -> request.getEntity().getName().equals(ENTITY_NAME))));
                });

    }

    @Test
    void testCheckDataNullDeltaOkFail() {
        when(deltaServiceDao.getDeltaOk(DATAMART_MNEMONIC)).thenReturn(Future.succeededFuture(null));
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setDatamartMnemonic(DATAMART_MNEMONIC);
        SqlCheckData sqlCheckData = mock(SqlCheckData.class);
        when(sqlCheckData.getDeltaNum()).thenReturn(1L);
        when(sqlCheckData.getTable()).thenReturn(entity.getName());
        CheckContext checkContext = new CheckContext(new RequestMetrics(), "env",
                new DatamartRequest(queryRequest), CheckType.DATA, sqlCheckData);
        checkDataExecutor.execute(checkContext)
                .onComplete(ar -> {
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DeltaNotExistException);
                });

    }

    @Test
    void testCheckDataInvalidDeltaNumFail() {
        when(deltaServiceDao.getDeltaOk(DATAMART_MNEMONIC)).thenReturn(Future.succeededFuture(okDelta));
        when(okDelta.getDeltaNum()).thenReturn(0L);
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setDatamartMnemonic(DATAMART_MNEMONIC);
        SqlCheckData sqlCheckData = mock(SqlCheckData.class);
        when(sqlCheckData.getDeltaNum()).thenReturn(1L);
        when(sqlCheckData.getTable()).thenReturn(entity.getName());
        CheckContext checkContext = new CheckContext(new RequestMetrics(), "env",
                new DatamartRequest(queryRequest), CheckType.DATA, sqlCheckData);
        checkDataExecutor.execute(checkContext)
                .onComplete(ar -> {
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DeltaNotExistException);
                });

    }

    @Test
    void testCheckByCountSuccess() {
        when(deltaServiceDao.getDeltaOk(DATAMART_MNEMONIC)).thenReturn(Future.succeededFuture(okDelta));
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setDatamartMnemonic(DATAMART_MNEMONIC);
        SqlCheckData sqlCheckData = mock(SqlCheckData.class);
        when(sqlCheckData.getDeltaNum()).thenReturn(0L);
        when(sqlCheckData.getTable()).thenReturn(entity.getName());
        CheckContext checkContext = new CheckContext(new RequestMetrics(), "env",
                new DatamartRequest(queryRequest), CheckType.DATA, sqlCheckData);
        checkDataExecutor.execute(checkContext)
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    SOURCE_TYPES.forEach(sourceType ->
                            verify(dataSourcePluginService)
                                    .checkDataByCount(eq(sourceType), any(),
                                            argThat(request -> request.getEntity().getName().equals(ENTITY_NAME))));
                });

    }

    @Test
    void testCheckInvalidColumnFail() {
        when(deltaServiceDao.getDeltaByNum(eq(DATAMART_MNEMONIC), anyLong())).thenReturn(Future.succeededFuture(okDelta));
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setDatamartMnemonic(DATAMART_MNEMONIC);
        SqlCheckData sqlCheckData = mock(SqlCheckData.class);
        when(sqlCheckData.getColumns()).thenReturn(Stream.of("invalid_column").collect(Collectors.toSet()));
        when(sqlCheckData.getTable()).thenReturn(entity.getName());
        CheckContext checkContext = new CheckContext(new RequestMetrics(), "env",
                new DatamartRequest(queryRequest), CheckType.DATA, sqlCheckData);
        checkDataExecutor.execute(checkContext)
                .onComplete(ar -> {
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof ColumnsNotExistsException);
                });

    }
}
