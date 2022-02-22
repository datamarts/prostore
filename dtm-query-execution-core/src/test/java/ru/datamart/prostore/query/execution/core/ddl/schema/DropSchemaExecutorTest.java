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
package ru.datamart.prostore.query.execution.core.ddl.schema;

import ru.datamart.prostore.cache.service.CacheService;
import ru.datamart.prostore.cache.service.CaffeineCacheService;
import ru.datamart.prostore.cache.service.EvictQueryTemplateCacheService;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.reader.QueryRequest;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.common.request.DatamartRequest;
import ru.datamart.prostore.query.calcite.core.configuration.CalciteCoreConfiguration;
import ru.datamart.prostore.query.calcite.core.framework.DtmCalciteFramework;
import ru.datamart.prostore.query.execution.core.base.dto.cache.EntityKey;
import ru.datamart.prostore.query.execution.core.base.dto.cache.MaterializedViewCacheValue;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacade;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacadeImpl;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.DatamartDao;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.ServiceDbDao;
import ru.datamart.prostore.query.execution.core.base.service.hsql.HSQLClient;
import ru.datamart.prostore.query.execution.core.base.service.metadata.MetadataExecutor;
import ru.datamart.prostore.query.execution.core.calcite.configuration.CalciteConfiguration;
import ru.datamart.prostore.query.execution.core.ddl.dto.DdlRequestContext;
import ru.datamart.prostore.query.execution.core.ddl.service.impl.schema.DropSchemaExecutor;
import ru.datamart.prostore.query.execution.core.delta.dto.HotDelta;
import ru.datamart.prostore.query.execution.core.delta.dto.OkDelta;
import ru.datamart.prostore.query.execution.core.utils.TestUtils;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.ResultSet;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class DropSchemaExecutorTest {

    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final SqlParser.Config parserConfig = calciteConfiguration.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory());
    private final MetadataExecutor metadataExecutor = mock(MetadataExecutor.class);
    private final CacheService<String, HotDelta> hotDeltaCacheService = mock(CaffeineCacheService.class);
    private final CacheService<String, OkDelta> okDeltaCacheService = mock(CaffeineCacheService.class);
    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final ServiceDbDao serviceDbDao = mock(ServiceDbDao.class);
    private final DatamartDao datamartDao = mock(DatamartDao.class);
    private final CacheService<EntityKey, Entity> entityCacheService = mock(CaffeineCacheService.class);
    private final EvictQueryTemplateCacheService evictQueryTemplateCacheService =
            mock(EvictQueryTemplateCacheService.class);
    private final CacheService<EntityKey, MaterializedViewCacheValue> materializedViewCacheService = mock(CaffeineCacheService.class);
    private final HSQLClient hsqlClient = mock(HSQLClient.class);
    private DropSchemaExecutor dropSchemaExecutor;
    private DdlRequestContext context;
    private String schema;

    @BeforeEach
    void setUp() throws SqlParseException {
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        when(serviceDbDao.getDatamartDao()).thenReturn(datamartDao);
        dropSchemaExecutor = new DropSchemaExecutor(metadataExecutor,
                serviceDbFacade,
                TestUtils.SQL_DIALECT,
                hotDeltaCacheService,
                okDeltaCacheService,
                entityCacheService,
                materializedViewCacheService,
                evictQueryTemplateCacheService,
                hsqlClient);
        doNothing().when(evictQueryTemplateCacheService).evictByDatamartName(anyString());
        prepareContext(false);
    }

    private void prepareContext(boolean isLogicalOnly) throws SqlParseException {
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);

        schema = "shares";
        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(schema);

        String dropSchemaSql = "drop database shares";
        if (isLogicalOnly) {
            dropSchemaSql += " logical_only";
        }
        queryRequest.setSql(dropSchemaSql);

        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);
        context.setDatamartName(schema);
    }

    @Test
    void executeSuccess() {
        Promise<QueryResult> promise = Promise.promise();
        ResultSet resultSet = new ResultSet();
        resultSet.setResults(new ArrayList<>());
        when(hsqlClient.getQueryResult(anyString())).thenReturn(Future.succeededFuture(resultSet));
        when(datamartDao.existsDatamart(schema))
                .thenReturn(Future.succeededFuture(true));

        when(metadataExecutor.execute(any()))
                .thenReturn(Future.succeededFuture());

        when(datamartDao.deleteDatamart(schema))
                .thenReturn(Future.succeededFuture());

        dropSchemaExecutor.execute(context, null)
                .onComplete(promise);
        assertTrue(promise.future().succeeded());
        verify(evictQueryTemplateCacheService).evictByDatamartName(schema);
        verify(materializedViewCacheService).forEach(any());
        verify(entityCacheService).removeIf(any());
        verify(hotDeltaCacheService).remove(anyString());
        verify(okDeltaCacheService).remove(anyString());
        verify(metadataExecutor).execute(context);
    }

    @Test
    void executeSuccessLogicalOnly() throws SqlParseException {
        prepareContext(true);

        Promise<QueryResult> promise = Promise.promise();
        ResultSet resultSet = new ResultSet();
        resultSet.setResults(new ArrayList<>());
        when(hsqlClient.getQueryResult(anyString())).thenReturn(Future.succeededFuture(resultSet));
        when(datamartDao.existsDatamart(schema))
                .thenReturn(Future.succeededFuture(true));

        when(datamartDao.deleteDatamart(schema))
                .thenReturn(Future.succeededFuture());

        dropSchemaExecutor.execute(context, null)
                .onComplete(promise);

        assertTrue(promise.future().succeeded());
        verify(evictQueryTemplateCacheService).evictByDatamartName(schema);
        verify(materializedViewCacheService).forEach(any());
        verify(entityCacheService).removeIf(any());
        verify(hotDeltaCacheService).remove(anyString());
        verify(okDeltaCacheService).remove(anyString());
        verify(metadataExecutor, never()).execute(context);
    }

    @Test
    void executeWithDropSchemaError() {
        Promise<QueryResult> promise = Promise.promise();
        ResultSet resultSet = new ResultSet();
        resultSet.setResults(new ArrayList<>());
        when(hsqlClient.getQueryResult(anyString())).thenReturn(Future.succeededFuture(resultSet));
        when(datamartDao.existsDatamart(schema))
                .thenReturn(Future.succeededFuture(true));

        when(metadataExecutor.execute(any()))
                .thenReturn(Future.failedFuture(new DtmException("")));

        dropSchemaExecutor.execute(context, null)
                .onComplete(promise);
        assertTrue(promise.future().failed());
        verify(evictQueryTemplateCacheService).evictByDatamartName(any());
        verify(materializedViewCacheService).forEach(any());
        verify(entityCacheService).removeIf(any());
        verify(hotDeltaCacheService).remove(anyString());
        verify(okDeltaCacheService).remove(anyString());
    }

    @Test
    void executeWithDropDatamartError() {
        Promise<QueryResult> promise = Promise.promise();
        ResultSet resultSet = new ResultSet();
        resultSet.setResults(new ArrayList<>());
        when(hsqlClient.getQueryResult(anyString())).thenReturn(Future.succeededFuture(resultSet));
        when(datamartDao.existsDatamart(schema))
                .thenReturn(Future.succeededFuture(true));

        when(metadataExecutor.execute(any()))
                .thenReturn(Future.succeededFuture());

        when(datamartDao.deleteDatamart(schema))
                .thenReturn(Future.failedFuture("delete datamart error"));

        dropSchemaExecutor.execute(context, null)
                .onComplete(promise);
        assertTrue(promise.future().failed());
        verify(evictQueryTemplateCacheService).evictByDatamartName(any());
        verify(materializedViewCacheService).forEach(any());
        verify(entityCacheService).removeIf(any());
        verify(hotDeltaCacheService).remove(anyString());
        verify(okDeltaCacheService).remove(anyString());
    }

    @Test
    void executeRelatedViewExistsError() {
        Promise<QueryResult> promise = Promise.promise();
        ResultSet resultSet = new ResultSet();
        List<JsonArray> result = new ArrayList<>();
        JsonArray jsonArray = new JsonArray();
        jsonArray.add("schema_name");
        jsonArray.add("view_name");
        result.add(jsonArray);
        resultSet.setResults(result);
        when(hsqlClient.getQueryResult(anyString())).thenReturn(Future.succeededFuture(resultSet));

        dropSchemaExecutor.execute(context, null)
                .onComplete(promise);
        assertTrue(promise.future().failed());
        assertEquals("Views [schema_name.view_name] using the 'SHARES' must be dropped first", promise.future().cause().getMessage());
        verify(materializedViewCacheService).forEach(any());
        verify(entityCacheService).removeIf(any());
        verify(hotDeltaCacheService).remove(anyString());
        verify(okDeltaCacheService).remove(anyString());
    }
}
