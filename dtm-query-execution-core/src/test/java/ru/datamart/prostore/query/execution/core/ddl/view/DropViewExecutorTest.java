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
package ru.datamart.prostore.query.execution.core.ddl.view;

import ru.datamart.prostore.cache.service.CacheService;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.query.calcite.core.configuration.CalciteCoreConfiguration;
import ru.datamart.prostore.query.calcite.core.framework.DtmCalciteFramework;
import ru.datamart.prostore.query.execution.core.base.dto.cache.EntityKey;
import ru.datamart.prostore.query.execution.core.base.exception.entity.EntityNotExistsException;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacade;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.*;
import ru.datamart.prostore.query.execution.core.base.service.metadata.MetadataExecutor;
import ru.datamart.prostore.query.execution.core.calcite.configuration.CalciteConfiguration;
import ru.datamart.prostore.query.execution.core.ddl.dto.DdlRequestContext;
import ru.datamart.prostore.query.execution.core.ddl.service.QueryResultDdlExecutor;
import ru.datamart.prostore.query.execution.core.ddl.service.impl.view.DropViewExecutor;
import ru.datamart.prostore.query.execution.core.delta.dto.OkDelta;
import ru.datamart.prostore.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import ru.datamart.prostore.query.execution.core.utils.TestUtils;
import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.verify;

@ExtendWith({VertxExtension.class, MockitoExtension.class})
class DropViewExecutorTest {

    private static final String SCHEMA = "shares";

    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
    private final SqlParser.Config parserConfig = calciteConfiguration.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory());
    private final FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
    private final OkDelta deltaOk = OkDelta.builder()
            .deltaNum(1)
            .build();
    private final Entity entity = Entity.builder()
            .schema(SCHEMA)
            .name("accounts")
            .build();

    @Mock
    private MetadataExecutor metadataExecutor;
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

    @Captor
    private ArgumentCaptor<String> changeQueryCaptor;

    private QueryResultDdlExecutor dropViewExecutor;
    private DdlRequestContext context;

    @BeforeEach
    void setUp() {
        lenient().when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        lenient().when(serviceDbDao.getDatamartDao()).thenReturn(datamartDao);
        lenient().when(serviceDbDao.getEntityDao()).thenReturn(entityDao);
        lenient().when(serviceDbFacade.getDeltaServiceDao()).thenReturn(deltaServiceDao);
        lenient().when(serviceDbDao.getChangelogDao()).thenReturn(changelogDao);
        dropViewExecutor = new DropViewExecutor(metadataExecutor,
                serviceDbFacade,
                TestUtils.SQL_DIALECT,
                cacheService);
        entity.setEntityType(EntityType.VIEW);
    }

    @Test
    void executeSuccess(VertxTestContext testContext) throws SqlParseException {
        // arrange
        prepareContext("drop view shares.accounts");

        when(entityDao.getEntity(SCHEMA, entity.getName()))
                .thenReturn(Future.succeededFuture(entity));

        when(deltaServiceDao.getDeltaOk(SCHEMA))
                .thenReturn(Future.succeededFuture(deltaOk));

        when(changelogDao.writeNewRecord(anyString(), anyString(), changeQueryCaptor.capture(), any()))
                .thenReturn(Future.succeededFuture());

        when(entityDao.setEntityState(any(), any(), anyString(), eq(SetEntityState.DELETE)))
                .thenReturn(Future.succeededFuture());

        // act
        dropViewExecutor.execute(context, null)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    assertEquals("DROP VIEW shares.accounts", changeQueryCaptor.getValue());
                }).completeNow());
    }

    @Test
    void executeGetDeltaOkError(VertxTestContext testContext) throws SqlParseException {
        // arrange
        prepareContext("drop view shares.accounts");

        when(entityDao.getEntity(SCHEMA, entity.getName()))
                .thenReturn(Future.succeededFuture(entity));

        when(deltaServiceDao.getDeltaOk(SCHEMA))
                .thenReturn(Future.failedFuture("get delta ok error"));

        // act
        dropViewExecutor.execute(context, null)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail(ar.cause());
                    }

                    assertEquals("get delta ok error", ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void executeWriteNewChangelogRecord(VertxTestContext testContext) throws SqlParseException {
        // arrange
        prepareContext("drop view shares.accounts");

        when(entityDao.getEntity(SCHEMA, entity.getName()))
                .thenReturn(Future.succeededFuture(entity));

        when(deltaServiceDao.getDeltaOk(SCHEMA))
                .thenReturn(Future.succeededFuture(deltaOk));

        when(changelogDao.writeNewRecord(anyString(), anyString(), changeQueryCaptor.capture(), any()))
                .thenReturn(Future.failedFuture("changelog write new record"));

        // act
        dropViewExecutor.execute(context, null)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail(ar.cause());
                    }

                    assertEquals("changelog write new record", ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void executeGetEntityError(VertxTestContext testContext) throws SqlParseException {
        // arrange
        prepareContext("drop view shares.accounts");

        when(entityDao.getEntity(SCHEMA, entity.getName()))
                .thenReturn(Future.failedFuture("get entity error"));

        // act
        dropViewExecutor.execute(context, null)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail(ar.cause());
                    }

                    assertEquals("get entity error", ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void executeWrongEntityTypeError(VertxTestContext testContext) throws SqlParseException {
        // arrange
        prepareContext("drop view shares.accounts");
        entity.setEntityType(EntityType.TABLE);

        when(entityDao.getEntity(SCHEMA, entity.getName()))
                .thenReturn(Future.succeededFuture(entity));

        // act
        dropViewExecutor.execute(context, null)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail(ar.cause());
                    }

                    assertTrue(ar.cause() instanceof EntityNotExistsException);
                }).completeNow());
    }

    private void prepareContext(String sql) throws SqlParseException {
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
        SqlNode sqlNode = planner.parse(sql);
        context = new DdlRequestContext(null, null, sqlNode, null, null);
    }
}
