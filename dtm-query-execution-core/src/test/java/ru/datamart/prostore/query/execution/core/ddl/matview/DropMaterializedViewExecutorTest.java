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
package ru.datamart.prostore.query.execution.core.ddl.matview;

import ru.datamart.prostore.cache.service.CacheService;
import ru.datamart.prostore.cache.service.EvictQueryTemplateCacheService;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.common.reader.QueryRequest;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.common.request.DatamartRequest;
import ru.datamart.prostore.query.calcite.core.configuration.CalciteCoreConfiguration;
import ru.datamart.prostore.query.calcite.core.framework.DtmCalciteFramework;
import ru.datamart.prostore.query.execution.core.base.dto.cache.EntityKey;
import ru.datamart.prostore.query.execution.core.base.dto.cache.MaterializedViewCacheValue;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacade;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.*;
import ru.datamart.prostore.query.execution.core.base.service.hsql.HSQLClient;
import ru.datamart.prostore.query.execution.core.base.service.metadata.MetadataExecutor;
import ru.datamart.prostore.query.execution.core.calcite.configuration.CalciteConfiguration;
import ru.datamart.prostore.query.execution.core.ddl.dto.DdlRequestContext;
import ru.datamart.prostore.query.execution.core.ddl.service.QueryResultDdlExecutor;
import ru.datamart.prostore.query.execution.core.ddl.service.impl.matview.DropMaterializedViewExecutor;
import ru.datamart.prostore.query.execution.core.delta.dto.OkDelta;
import ru.datamart.prostore.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import ru.datamart.prostore.query.execution.core.plugin.service.DataSourcePluginService;
import ru.datamart.prostore.query.execution.core.utils.TestUtils;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.ext.sql.ResultSet;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DropMaterializedViewExecutorTest {
    private static final String SCHEMA = "shares";

    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
    private final SqlParser.Config parserConfig = calciteConfiguration.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory());
    private final FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
    private final OkDelta deltaOk = OkDelta.builder()
            .deltaNum(1)
            .build();
    @Mock
    private MetadataExecutor metadataExecutor;
    @Mock
    private DataSourcePluginService pluginService;
    @Mock
    private CacheService<EntityKey, Entity> cacheService;
    @Mock
    private CacheService<EntityKey, MaterializedViewCacheValue> materializedViewCacheService;
    @Mock
    private ServiceDbFacade serviceDbFacade;
    @Mock
    private ServiceDbDao serviceDbDao;
    @Mock
    private ChangelogDao changelogDao;
    @Mock
    private DatamartDao datamartDao;
    @Mock
    private EntityDao entityDao;
    @Mock
    private DeltaServiceDao deltaServiceDao;
    @Mock
    private EvictQueryTemplateCacheService evictQueryTemplateCacheService;
    @Mock
    private HSQLClient hsqlClient;

    @Captor
    private ArgumentCaptor<DdlRequestContext> contextArgumentCaptor;

    private QueryResultDdlExecutor dropMaterializedViewExecutor;
    private DdlRequestContext context;

    @BeforeEach
    void setUp() {
        lenient().when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        lenient().when(serviceDbDao.getDatamartDao()).thenReturn(datamartDao);
        lenient().when(serviceDbDao.getEntityDao()).thenReturn(entityDao);
        lenient().when(serviceDbFacade.getDeltaServiceDao()).thenReturn(deltaServiceDao);
        lenient().when(serviceDbDao.getChangelogDao()).thenReturn(changelogDao);
        dropMaterializedViewExecutor = new DropMaterializedViewExecutor(metadataExecutor,
                serviceDbFacade,
                TestUtils.SQL_DIALECT,
                cacheService,
                pluginService,
                materializedViewCacheService,
                hsqlClient,
                evictQueryTemplateCacheService);
        lenient().doNothing().when(evictQueryTemplateCacheService).evictByEntityName(anyString(), anyString());
    }

    @Test
    void executeSuccess() throws SqlParseException {
        // arrange
        prepareContext("drop materialized view accounts");

        Entity entity = context.getEntity();

        Promise<QueryResult> promise = Promise.promise();

        when(pluginService.getSourceTypes()).thenReturn(Collections.singleton(SourceType.ADB));
        when(entityDao.getEntity(SCHEMA, entity.getName()))
                .thenReturn(Future.succeededFuture(entity));
        when(hsqlClient.getQueryResult(any()))
                .thenReturn(Future.succeededFuture(new ResultSet().setResults(Collections.EMPTY_LIST)));
        when(deltaServiceDao.getDeltaOk(SCHEMA)).thenReturn(Future.succeededFuture(deltaOk));
        when(changelogDao.writeNewRecord(anyString(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(metadataExecutor.execute(any()))
                .thenReturn(Future.succeededFuture());
        when(entityDao.setEntityState(any(), any(), anyString(), eq(SetEntityState.DELETE)))
                .thenReturn(Future.succeededFuture());
        when(materializedViewCacheService.get(any())).thenReturn(new MaterializedViewCacheValue(entity));

        // act
        dropMaterializedViewExecutor.execute(context, context.getEntity().getName())
                .onComplete(promise);

        // assert
        assertTrue(promise.future().succeeded());
        verify(evictQueryTemplateCacheService, times(1))
                .evictByEntityName(entity.getSchema(), entity.getName());
        verify(metadataExecutor).execute(contextArgumentCaptor.capture());
        verify(materializedViewCacheService).get(any());
        DdlRequestContext value = contextArgumentCaptor.getValue();
        assertNull(value.getSourceType());
    }

    @Test
    void executeSuccessLogicalOnly() throws SqlParseException {
        // arrange
        prepareContext("drop materialized view accounts logical_only");

        Entity entity = context.getEntity();

        Promise<QueryResult> promise = Promise.promise();

        when(pluginService.getSourceTypes()).thenReturn(Collections.singleton(SourceType.ADB));
        when(entityDao.getEntity(SCHEMA, entity.getName()))
                .thenReturn(Future.succeededFuture(entity));
        when(hsqlClient.getQueryResult(any()))
                .thenReturn(Future.succeededFuture(new ResultSet().setResults(Collections.EMPTY_LIST)));
        when(deltaServiceDao.getDeltaOk(SCHEMA)).thenReturn(Future.succeededFuture(deltaOk));
        when(changelogDao.writeNewRecord(anyString(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(entityDao.setEntityState(any(), any(), anyString(), eq(SetEntityState.DELETE)))
                .thenReturn(Future.succeededFuture());
        when(materializedViewCacheService.get(any())).thenReturn(new MaterializedViewCacheValue(entity));

        // act
        dropMaterializedViewExecutor.execute(context, context.getEntity().getName())
                .onComplete(promise);

        // assert
        assertTrue(promise.future().succeeded());
        verify(evictQueryTemplateCacheService, times(1))
                .evictByEntityName(entity.getSchema(), entity.getName());
        verify(metadataExecutor, never()).execute(any());
        verify(materializedViewCacheService).get(any());
    }

    @Test
    void executeCorrectlyExtractSourceType() throws SqlParseException {
        // arrange
        prepareContext("drop materialized view accounts datasource_type = 'ADB'");
        Entity entity = context.getEntity();
        Promise<QueryResult> promise = Promise.promise();

        when(pluginService.getSourceTypes()).thenReturn(Collections.singleton(SourceType.ADB));
        when(entityDao.getEntity(SCHEMA, entity.getName()))
                .thenReturn(Future.succeededFuture(entity));
        when(hsqlClient.getQueryResult(any()))
                .thenReturn(Future.succeededFuture(new ResultSet().setResults(Collections.EMPTY_LIST)));
        when(deltaServiceDao.getDeltaOk(SCHEMA)).thenReturn(Future.succeededFuture(deltaOk));
        when(changelogDao.writeNewRecord(anyString(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(metadataExecutor.execute(any()))
                .thenReturn(Future.succeededFuture());
        when(entityDao.setEntityState(any(), any(), anyString(), eq(SetEntityState.DELETE)))
                .thenReturn(Future.succeededFuture());
        when(materializedViewCacheService.get(any())).thenReturn(new MaterializedViewCacheValue(entity));

        // act
        dropMaterializedViewExecutor.execute(context, context.getEntity().getName())
                .onComplete(promise);

        // assert
        assertTrue(promise.future().succeeded());
        verify(metadataExecutor).execute(contextArgumentCaptor.capture());
        verify(materializedViewCacheService).get(any());
        DdlRequestContext value = contextArgumentCaptor.getValue();
        assertSame(SourceType.ADB, value.getSourceType());
    }

    private void prepareContext(String s) throws SqlParseException {
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(SCHEMA);
        queryRequest.setSql(s);
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);
        EntityField f1 = new EntityField(0, "id", ColumnType.INT, false);
        f1.setPrimaryOrder(1);
        f1.setShardingOrder(1);
        EntityField f2 = new EntityField(1, "name", ColumnType.VARCHAR, true);
        f2.setSize(100);
        String sqlNodeName = "accounts";
        Entity ctxEntity = new Entity(sqlNodeName, SCHEMA, Arrays.asList(f1, f2));
        ctxEntity.setEntityType(EntityType.MATERIALIZED_VIEW);
        ctxEntity.setDestination(Collections.singleton(SourceType.ADB));
        context.setEntity(ctxEntity);
        context.setDatamartName(SCHEMA);
    }

}
