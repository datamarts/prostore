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
package ru.datamart.prostore.query.execution.core.ddl.table;

import ru.datamart.prostore.cache.service.CacheService;
import ru.datamart.prostore.cache.service.EvictQueryTemplateCacheService;
import ru.datamart.prostore.common.exception.DtmException;
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
import ru.datamart.prostore.query.execution.core.base.exception.entity.EntityNotExistsException;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacade;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.*;
import ru.datamart.prostore.query.execution.core.base.service.hsql.HSQLClient;
import ru.datamart.prostore.query.execution.core.base.service.metadata.MetadataExecutor;
import ru.datamart.prostore.query.execution.core.calcite.configuration.CalciteConfiguration;
import ru.datamart.prostore.query.execution.core.ddl.dto.DdlRequestContext;
import ru.datamart.prostore.query.execution.core.ddl.service.QueryResultDdlExecutor;
import ru.datamart.prostore.query.execution.core.ddl.service.impl.table.DropTableExecutor;
import ru.datamart.prostore.query.execution.core.delta.dto.OkDelta;
import ru.datamart.prostore.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import ru.datamart.prostore.query.execution.core.plugin.service.DataSourcePluginService;
import ru.datamart.prostore.query.execution.core.utils.TestUtils;
import ru.datamart.prostore.query.execution.plugin.api.exception.DataSourceException;
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
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DropTableExecutorTest {
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
    @Captor
    private ArgumentCaptor<String> changeQueryCaptor;

    private QueryResultDdlExecutor dropTableDdlExecutor;
    private DdlRequestContext context;

    @BeforeEach
    void setUp() {
        lenient().when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        lenient().when(serviceDbDao.getDatamartDao()).thenReturn(datamartDao);
        lenient().when(serviceDbDao.getEntityDao()).thenReturn(entityDao);
        lenient().when(serviceDbFacade.getDeltaServiceDao()).thenReturn(deltaServiceDao);
        lenient().when(serviceDbDao.getChangelogDao()).thenReturn(changelogDao);
        dropTableDdlExecutor = new DropTableExecutor(metadataExecutor,
                serviceDbFacade,
                TestUtils.SQL_DIALECT,
                cacheService,
                pluginService,
                hsqlClient,
                evictQueryTemplateCacheService);
        lenient().doNothing().when(evictQueryTemplateCacheService).evictByEntityName(anyString(), anyString());
    }

    @Test
    void executeSuccess() throws SqlParseException {
        // arrange
        prepareContext("drop table shares.accounts");
        Promise<QueryResult> promise = Promise.promise();
        Entity entity = context.getEntity();
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

        // act
        dropTableDdlExecutor.execute(context, context.getEntity().getName())
                .onComplete(promise);

        // assert
        assertTrue(promise.future().succeeded());
        verify(evictQueryTemplateCacheService)
                .evictByEntityName(entity.getSchema(), entity.getName());
        verify(metadataExecutor).execute(contextArgumentCaptor.capture());
        DdlRequestContext value = contextArgumentCaptor.getValue();
        assertNull(value.getSourceType());
    }

    @Test
    void executeSuccessLogicalOnly() throws SqlParseException {
        // arrange
        prepareContext("drop table accounts logical_only");
        Promise<QueryResult> promise = Promise.promise();
        Entity entity = context.getEntity();
        when(pluginService.getSourceTypes()).thenReturn(Collections.singleton(SourceType.ADB));

        when(entityDao.getEntity(SCHEMA, entity.getName()))
                .thenReturn(Future.succeededFuture(entity));
        when(hsqlClient.getQueryResult(any()))
                .thenReturn(Future.succeededFuture(new ResultSet().setResults(Collections.EMPTY_LIST)));
        when(deltaServiceDao.getDeltaOk(SCHEMA)).thenReturn(Future.succeededFuture(deltaOk));
        when(changelogDao.writeNewRecord(anyString(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(entityDao.setEntityState(any(), any(), anyString(), eq(SetEntityState.DELETE)))
                .thenReturn(Future.succeededFuture());

        // act
        dropTableDdlExecutor.execute(context, context.getEntity().getName())
                .onComplete(promise);

        // assert
        assertTrue(promise.future().succeeded());
        verify(evictQueryTemplateCacheService)
                .evictByEntityName(entity.getSchema(), entity.getName());
        verify(metadataExecutor, never()).execute(any());
    }

    @Test
    void executeWithIfExistsStmtSuccess() throws SqlParseException {
        // arrange
        prepareContext("drop table shares.accounts");
        Promise<QueryResult> promise = Promise.promise();
        Entity entity = context.getEntity();
        context.getRequest().getQueryRequest().setSql("DROP TABLE IF EXISTS shares.accounts");
        String entityName = entity.getName();
        when(entityDao.getEntity(SCHEMA, entityName))
                .thenReturn(Future.failedFuture(new EntityNotExistsException(entityName)));

        // act
        dropTableDdlExecutor.execute(context, entityName)
                .onComplete(promise);

        // assert
        assertNotNull(promise.future().result());
        verify(evictQueryTemplateCacheService, never())
                .evictByEntityName(entity.getSchema(), entity.getName());
        verifyNoInteractions(pluginService, hsqlClient);
    }

    @Test
    void executeWithMetadataExecuteError() throws SqlParseException {
        // arrange
        prepareContext("drop table shares.accounts");
        Promise<QueryResult> promise = Promise.promise();
        Entity entity = context.getEntity();
        when(pluginService.getSourceTypes()).thenReturn(Collections.singleton(SourceType.ADB));
        when(entityDao.getEntity(SCHEMA, entity.getName()))
                .thenReturn(Future.succeededFuture(entity));

        when(hsqlClient.getQueryResult(any()))
                .thenReturn(Future.succeededFuture(new ResultSet().setResults(Collections.EMPTY_LIST)));

        when(deltaServiceDao.getDeltaOk(SCHEMA))
                .thenReturn(Future.succeededFuture(deltaOk));

        when(changelogDao.writeNewRecord(anyString(), anyString(), changeQueryCaptor.capture(), any()))
                .thenReturn(Future.succeededFuture());

        when(metadataExecutor.execute(any()))
                .thenReturn(Future.failedFuture(new DataSourceException("Error drop table in plugin")));

        // act
        dropTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);

        // assert
        assertNotNull(promise.future().cause());
        assertEquals("DROP TABLE shares.accounts", changeQueryCaptor.getValue());
        verify(evictQueryTemplateCacheService, times(1))
                .evictByEntityName(entity.getSchema(), entity.getName());
    }

    @Test
    void executeWithGetDeltaOkError() throws SqlParseException {
        // arrange
        prepareContext("drop table accounts");
        Promise<QueryResult> promise = Promise.promise();
        Entity entity = context.getEntity();
        when(entityDao.getEntity(SCHEMA, entity.getName()))
                .thenReturn(Future.succeededFuture(context.getEntity()));

        when(hsqlClient.getQueryResult(any()))
                .thenReturn(Future.succeededFuture(new ResultSet().setResults(Collections.EMPTY_LIST)));

        when(deltaServiceDao.getDeltaOk(SCHEMA))
                .thenReturn(Future.failedFuture("get delta ok error"));

        // act
        dropTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);

        // assert
        assertNotNull(promise.future().cause());
        assertEquals("get delta ok error", promise.future().cause().getMessage());
    }

    @Test
    void executeWithWriteChangelogError() throws SqlParseException {
        // arrange
        prepareContext("drop table shares.accounts");
        Promise<QueryResult> promise = Promise.promise();
        Entity entity = context.getEntity();
        when(entityDao.getEntity(SCHEMA, entity.getName()))
                .thenReturn(Future.succeededFuture(context.getEntity()));

        when(hsqlClient.getQueryResult(any()))
                .thenReturn(Future.succeededFuture(new ResultSet().setResults(Collections.EMPTY_LIST)));

        when(deltaServiceDao.getDeltaOk(SCHEMA))
                .thenReturn(Future.succeededFuture(deltaOk));

        when(changelogDao.writeNewRecord(anyString(), anyString(), changeQueryCaptor.capture(), any()))
                .thenReturn(Future.failedFuture("changelog write new record error"));

        // act
        dropTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);

        // assert
        assertNotNull(promise.future().cause());
        assertEquals("DROP TABLE shares.accounts", changeQueryCaptor.getValue());
        assertEquals("changelog write new record error", promise.future().cause().getMessage());
    }

    @Test
    void executeWithSetEntityStateError() throws SqlParseException {
        // arrange
        prepareContext("drop table shares.accounts");
        Promise<QueryResult> promise = Promise.promise();
        Entity entity = context.getEntity();
        when(pluginService.getSourceTypes()).thenReturn(Collections.singleton(SourceType.ADB));
        when(entityDao.getEntity(SCHEMA, entity.getName()))
                .thenReturn(Future.succeededFuture(context.getEntity()));

        when(hsqlClient.getQueryResult(any()))
                .thenReturn(Future.succeededFuture(new ResultSet().setResults(Collections.EMPTY_LIST)));

        when(deltaServiceDao.getDeltaOk(SCHEMA))
                .thenReturn(Future.succeededFuture(deltaOk));

        when(changelogDao.writeNewRecord(anyString(), anyString(), changeQueryCaptor.capture(), any()))
                .thenReturn(Future.succeededFuture());

        when(metadataExecutor.execute(any()))
                .thenReturn(Future.succeededFuture());

        when(entityDao.setEntityState(any(), any(), anyString(), eq(SetEntityState.DELETE)))
                .thenReturn(Future.failedFuture(new DtmException("delete entity error")));

        // act
        dropTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);

        // assert
        assertNotNull(promise.future().cause());
        assertEquals("DROP TABLE shares.accounts", changeQueryCaptor.getValue());
        verify(evictQueryTemplateCacheService)
                .evictByEntityName(entity.getSchema(), entity.getName());
    }

    @Test
    void executeWithExistedViewError() throws SqlParseException {
        // arrange
        prepareContext("drop table shares.accounts");
        Promise<QueryResult> promise = Promise.promise();
        String viewName1 = "SYS_VIEWNAME_1";
        String viewName2 = "VIEWNAME_2";
        JsonArray views = new JsonArray().add(viewName1).add(viewName2);
        String expectedMessage = String.format("Views [VIEWNAME_1, VIEWNAME_2] using the '%s' must be dropped first", context.getEntity().getName().toUpperCase());

        when(entityDao.getEntity(SCHEMA, context.getEntity().getName()))
                .thenReturn(Future.succeededFuture(context.getEntity()));

        when(hsqlClient.getQueryResult(any()))
                .thenReturn(Future.succeededFuture(new ResultSet().setResults(Collections.singletonList(views))));

        // act
        dropTableDdlExecutor.execute(context, context.getEntity().getName())
                .onComplete(promise);

        // assert
        assertEquals(expectedMessage, promise.future().cause().getMessage());
    }

    @Test
    void executeCorrectlyExtractSourceType() throws SqlParseException {
        // arrange
        prepareContext("drop table shares.accounts datasource_type = 'ADB'");
        Entity entity = context.getEntity();

        Promise<QueryResult> promise = Promise.promise();

        when(pluginService.getSourceTypes()).thenReturn(Collections.singleton(SourceType.ADB));

        when(entityDao.getEntity(SCHEMA, entity.getName()))
                .thenReturn(Future.succeededFuture(entity));

        when(hsqlClient.getQueryResult(any()))
                .thenReturn(Future.succeededFuture(new ResultSet().setResults(Collections.EMPTY_LIST)));

        when(deltaServiceDao.getDeltaOk(SCHEMA))
                .thenReturn(Future.succeededFuture(deltaOk));

        when(changelogDao.writeNewRecord(anyString(), anyString(), changeQueryCaptor.capture(), any()))
                .thenReturn(Future.succeededFuture());


        when(metadataExecutor.execute(any()))
                .thenReturn(Future.succeededFuture());

        when(entityDao.setEntityState(any(), any(), anyString(), eq(SetEntityState.DELETE)))
                .thenReturn(Future.succeededFuture());

        // act
        dropTableDdlExecutor.execute(context, context.getEntity().getName())
                .onComplete(promise);

        // assert
        assertTrue(promise.future().succeeded());
        verify(metadataExecutor).execute(contextArgumentCaptor.capture());
        assertEquals("DROP TABLE shares.accounts DATASOURCE_TYPE = 'ADB'", changeQueryCaptor.getValue());
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
        ctxEntity.setEntityType(EntityType.TABLE);
        ctxEntity.setDestination(Collections.singleton(SourceType.ADB));
        context.setEntity(ctxEntity);
        context.setDatamartName(SCHEMA);
    }
}
