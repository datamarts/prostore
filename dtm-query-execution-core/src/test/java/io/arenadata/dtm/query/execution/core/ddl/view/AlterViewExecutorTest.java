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
package io.arenadata.dtm.query.execution.core.ddl.view;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.request.DatamartRequest;
import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.extension.ddl.SqlAlterView;
import io.arenadata.dtm.query.calcite.core.framework.DtmCalciteFramework;
import io.arenadata.dtm.query.calcite.core.provider.CalciteContextProvider;
import io.arenadata.dtm.query.calcite.core.rel2sql.DtmRelToSqlConverter;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.execution.core.base.dto.cache.EntityKey;
import io.arenadata.dtm.query.execution.core.base.exception.entity.EntityNotExistsException;
import io.arenadata.dtm.query.execution.core.base.exception.table.ValidationDtmException;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.*;
import io.arenadata.dtm.query.execution.core.base.service.metadata.LogicalSchemaProvider;
import io.arenadata.dtm.query.execution.core.base.service.metadata.MetadataExecutor;
import io.arenadata.dtm.query.execution.core.calcite.configuration.CalciteConfiguration;
import io.arenadata.dtm.query.execution.core.calcite.factory.CoreCalciteSchemaFactory;
import io.arenadata.dtm.query.execution.core.calcite.factory.CoreSchemaFactory;
import io.arenadata.dtm.query.execution.core.calcite.service.CoreCalciteContextProvider;
import io.arenadata.dtm.query.execution.core.ddl.dto.DdlRequestContext;
import io.arenadata.dtm.query.execution.core.ddl.service.impl.view.AlterViewExecutor;
import io.arenadata.dtm.query.execution.core.delta.dto.OkDelta;
import io.arenadata.dtm.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.dml.service.ColumnMetadataService;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
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
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static io.arenadata.dtm.query.execution.core.utils.TestUtils.*;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class AlterViewExecutorTest {

    @Mock
    private ServiceDbFacade serviceDbFacade;
    @Mock
    private ServiceDbDao serviceDbDao;
    @Mock
    private ChangelogDao changelogDao;
    @Mock
    private EntityDao entityDao;
    @Mock
    private DatamartDao datamartDao;
    @Mock
    private DeltaServiceDao deltaServiceDao;
    @Mock
    private LogicalSchemaProvider logicalSchemaProvider;
    @Mock
    private ColumnMetadataService columnMetadataService;
    @Mock
    private MetadataExecutor metadataExecutor;
    @Mock
    private CacheService<EntityKey, Entity> entityCacheService;
    @Mock
    private QueryParserService parserService;

    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final SqlParser.Config parserConfig = calciteConfiguration
            .configEddlParser(calciteCoreConfiguration.eddlParserImplFactory());
    private final CoreCalciteSchemaFactory coreSchemaFactory = new CoreCalciteSchemaFactory(new CoreSchemaFactory());
    private final CalciteContextProvider contextProvider = new CoreCalciteContextProvider(parserConfig, coreSchemaFactory);
    private final DtmRelToSqlConverter relToSqlConverter = new DtmRelToSqlConverter(SQL_DIALECT);
    private final OkDelta deltaOk = OkDelta.builder()
            .deltaNum(1)
            .build();

    private AlterViewExecutor alterViewExecutor;
    private String sqlNodeName;
    private String schema;
    private final List<Entity> entityList = new ArrayList<>();
    private List<Datamart> logicSchema;

    @BeforeEach
    void setUp() {
        lenient().when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        lenient().when(serviceDbDao.getEntityDao()).thenReturn(entityDao);
        lenient().when(serviceDbDao.getDatamartDao()).thenReturn(datamartDao);
        lenient().when(serviceDbFacade.getDeltaServiceDao()).thenReturn(deltaServiceDao);
        lenient().when(serviceDbDao.getChangelogDao()).thenReturn(changelogDao);
        lenient().when(metadataExecutor.execute(any())).thenReturn(Future.succeededFuture());
        lenient().when(logicalSchemaProvider.getSchemaFromQuery(any(), any()))
                .thenReturn(Future.succeededFuture(logicSchema));

        alterViewExecutor = new AlterViewExecutor(metadataExecutor,
                serviceDbFacade,
                SQL_DIALECT,
                entityCacheService,
                logicalSchemaProvider,
                columnMetadataService,
                parserService,
                relToSqlConverter);
        schema = "shares";
        initEntityList(entityList, schema);
        List<Entity> informationSchemaEntity = singletonList(Entity.builder()
                .entityType(EntityType.TABLE)
                .name("tables")
                .fields(singletonList(
                        EntityField.builder()
                                .ordinalPosition(0)
                                .name("id")
                                .type(ColumnType.BIGINT)
                                .nullable(false)
                                .build()))
                .build());
        logicSchema = Arrays.asList(new Datamart(schema, true, entityList),
                new Datamart("information_schema", false, informationSchemaEntity));
        sqlNodeName = schema + "." + entityList.get(0).getName();
    }

    @Test
    void executeSuccess(VertxTestContext testContext) throws SqlParseException {
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);

        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(schema);
        queryRequest.setSql(String.format("ALTER VIEW %s.%s AS SELECT * FROM %s.%s",
                schema, entityList.get(0).getName(), schema, entityList.get(1).getName()));
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        DdlRequestContext context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);

        when(parserService.parse(any()))
                .thenReturn(Future.succeededFuture(parse(contextProvider, new QueryParserRequest(((SqlAlterView) sqlNode).getQuery(), logicSchema))));

        when(columnMetadataService.getColumnMetadata(any(QueryParserRequest.class)))
                .thenReturn(Future.succeededFuture(singletonList(ColumnMetadata.builder()
                        .name("id")
                        .type(ColumnType.BIGINT)
                        .build())));

        when(entityDao.getEntity(schema, entityList.get(1).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(1)));

        when(entityDao.getEntity(schema, entityList.get(0).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(0)));

        when(deltaServiceDao.getDeltaOk(schema))
                .thenReturn(Future.succeededFuture(deltaOk));

        when(changelogDao.writeNewRecord(anyString(), anyString(), anyString(), any()))
                .thenReturn(Future.succeededFuture());

        when(entityDao.setEntityState(any(), any(), anyString(), eq(SetEntityState.UPDATE)))
                .thenReturn(Future.succeededFuture());

        alterViewExecutor.execute(context, sqlNodeName)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    assertTrue(ar.succeeded());

                }).completeNow());
    }

    @Test
    void shouldFailWhenInformationSchemaInQuery(VertxTestContext testContext) throws SqlParseException {
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);

        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(schema);
        queryRequest.setSql(String.format("ALTER VIEW %s.%s AS SELECT * FROM information_schema.tables",
                schema, entityList.get(0).getName()));
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        DdlRequestContext context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);

        lenient().when(parserService.parse(any()))
                .thenReturn(Future.succeededFuture(parse(contextProvider, new QueryParserRequest(((SqlAlterView) sqlNode).getQuery(), logicSchema))));

        lenient().when(columnMetadataService.getColumnMetadata(any(QueryParserRequest.class)))
                .thenReturn(Future.succeededFuture(singletonList(ColumnMetadata.builder()
                        .name("id")
                        .type(ColumnType.BIGINT)
                        .build())));

        lenient().when(entityDao.getEntity(schema, entityList.get(1).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(1)));

        lenient().when(entityDao.getEntity(schema, entityList.get(0).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(0)));

        lenient().when(deltaServiceDao.getDeltaOk(schema))
                .thenReturn(Future.succeededFuture(deltaOk));

        lenient().when(changelogDao.writeNewRecord(anyString(), anyString(), anyString(), any()))
                .thenReturn(Future.succeededFuture());

        lenient().when(entityDao.setEntityState(any(), any(), anyString(), eq(SetEntityState.UPDATE)))
                .thenReturn(Future.succeededFuture());

        alterViewExecutor.execute(context, sqlNodeName)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }

                    assertEquals("Using of INFORMATION_SCHEMA is forbidden [information_schema.tables]", ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void executeWrongEntityTypeError(VertxTestContext testContext) throws SqlParseException {
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);

        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(schema);
        queryRequest.setSql(String.format("ALTER VIEW %s.%s AS SELECT * FROM %s.%s",
                schema, entityList.get(1).getName(), schema, entityList.get(2).getName()));
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        DdlRequestContext context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);

        when(parserService.parse(any()))
                .thenReturn(Future.succeededFuture(parse(contextProvider, new QueryParserRequest(((SqlAlterView) sqlNode).getQuery(), logicSchema))));

        when(columnMetadataService.getColumnMetadata(any(QueryParserRequest.class)))
                .thenReturn(Future.succeededFuture(singletonList(ColumnMetadata.builder()
                        .name("id")
                        .type(ColumnType.BIGINT)
                        .build())));

        lenient().when(deltaServiceDao.getDeltaOk(schema))
                .thenReturn(Future.succeededFuture(deltaOk));

        lenient().when(changelogDao.writeNewRecord(anyString(), anyString(), anyString(), any()))
                .thenReturn(Future.succeededFuture());

        lenient().when(entityDao.getEntity(schema, entityList.get(1).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(1)));

        when(entityDao.getEntity(schema, entityList.get(2).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(2)));

        alterViewExecutor.execute(context, sqlNodeName)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }

                    assertTrue(ar.failed());
                    assertEquals(String.format("Entity %s is not a view", entityList.get(1).getName()), ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void executeSuccessWithJoinQuery(VertxTestContext testContext) throws SqlParseException {
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);

        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(schema);
        queryRequest.setSql(String.format("ALTER VIEW %s.%s AS SELECT * " +
                        "FROM (select a.id FROM %s a " +
                        "JOIN %s.%s t on t.id = a.id)",
                schema, entityList.get(0).getName(), entityList.get(2).getName(),
                schema, entityList.get(3).getName()));

        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        DdlRequestContext context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);
        context.setDatamartName(schema);

        when(parserService.parse(any()))
                .thenReturn(Future.succeededFuture(parse(contextProvider, new QueryParserRequest(((SqlAlterView) sqlNode).getQuery(), logicSchema))));

        when(columnMetadataService.getColumnMetadata(any(QueryParserRequest.class)))
                .thenReturn(Future.succeededFuture(singletonList(ColumnMetadata.builder()
                        .name("id")
                        .type(ColumnType.BIGINT)
                        .build())));

        when(entityDao.getEntity(schema, entityList.get(2).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(2)));

        when(entityDao.getEntity(schema, entityList.get(3).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(3)));

        when(entityDao.getEntity(schema, entityList.get(0).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(0)));

        when(deltaServiceDao.getDeltaOk(schema))
                .thenReturn(Future.succeededFuture(deltaOk));

        when(changelogDao.writeNewRecord(anyString(), anyString(), anyString(), any()))
                .thenReturn(Future.succeededFuture());

        when(entityDao.setEntityState(any(), any(), anyString(), eq(SetEntityState.UPDATE)))
                .thenReturn(Future.succeededFuture());

        alterViewExecutor.execute(context, sqlNodeName)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.failed()) {
                        fail(ar.cause());
                    }
                    assertTrue(ar.succeeded());
                }).completeNow());
    }

    @Test
    void executeIsEntityExistsError(VertxTestContext testContext) throws SqlParseException {
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);

        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(schema);
        queryRequest.setSql(String.format("ALTER VIEW %s.%s AS SELECT * FROM %s.%s",
                schema, entityList.get(0).getName(), schema, entityList.get(1).getName()));
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        DdlRequestContext context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);

        when(entityDao.getEntity(schema, entityList.get(1).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(1)));

        lenient().when(columnMetadataService.getColumnMetadata(any(QueryParserRequest.class)))
                .thenReturn(Future.succeededFuture(singletonList(ColumnMetadata.builder()
                        .name("id")
                        .type(ColumnType.BIGINT)
                        .build())));

        lenient().when(entityDao.getEntity(schema, entityList.get(0).getName()))
                .thenReturn(Future.failedFuture(new EntityNotExistsException(entityList.get(0).getName())));

        alterViewExecutor.execute(context, sqlNodeName)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }
                    assertTrue(ar.failed());
                }).completeNow());
    }

    @Test
    void executeWithViewUpdateError(VertxTestContext testContext) throws SqlParseException {
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);

        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(schema);
        queryRequest.setSql(String.format("ALTER VIEW %s.%s AS SELECT * FROM %s.%s",
                schema, entityList.get(0).getName(), schema, entityList.get(1).getName()));
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        DdlRequestContext context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);

        when(entityDao.getEntity(schema, entityList.get(1).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(1)));

        lenient().when(columnMetadataService.getColumnMetadata(any(QueryParserRequest.class)))
                .thenReturn(Future.succeededFuture(singletonList(ColumnMetadata.builder()
                        .name("id")
                        .type(ColumnType.BIGINT)
                        .build())));

        lenient().when(entityDao.getEntity(schema, entityList.get(0).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(0)));

        lenient().when(entityDao.updateEntity(any()))
                .thenReturn(Future.failedFuture(new DtmException("Update error")));

        alterViewExecutor.execute(context, sqlNodeName)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }
                    assertTrue(ar.failed());
                }).completeNow());
    }

    @Test
    void executeQueryContainsViewError(VertxTestContext testContext) throws SqlParseException {
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);

        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(schema);
        queryRequest.setSql(String.format("ALTER VIEW %s.%s AS SELECT * FROM %s.%s",
                schema, entityList.get(0).getName(), schema, entityList.get(0).getName()));
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        DdlRequestContext context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);

        when(columnMetadataService.getColumnMetadata(any(QueryParserRequest.class)))
                .thenReturn(Future.succeededFuture(singletonList(ColumnMetadata.builder()
                        .name("id")
                        .type(ColumnType.BIGINT)
                        .build())));

        when(entityDao.getEntity(schema, entityList.get(1).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(1)));

        when(entityDao.getEntity(schema, entityList.get(1).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(1)));

        alterViewExecutor.execute(context, sqlNodeName)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }
                    assertTrue(ar.failed());
                }).completeNow());
    }

    @Test
    void executeContainsCollateViewError(VertxTestContext testContext) throws SqlParseException {
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);

        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(schema);
        queryRequest.setSql(String.format("ALTER VIEW %s.%s AS SELECT * FROM %s.%s WHERE varchar_col = 'test' COLLATE 'unicode_ci'",
                schema, entityList.get(0).getName(), schema, entityList.get(0).getName()));
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        DdlRequestContext context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);

        alterViewExecutor.execute(context, sqlNodeName)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }
                    assertTrue(ar.failed());
                }).completeNow());
    }

    @Test
    void executeSuccessColumnDuplication(VertxTestContext testContext) throws SqlParseException {
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);

        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(schema);
        queryRequest.setSql(String.format("ALTER VIEW %s.%s AS SELECT * " +
                        "FROM %s.%s a " +
                        "JOIN %s.%s t on t.id = a.id",
                schema, entityList.get(0).getName(),
                schema, entityList.get(4).getName(),
                schema, entityList.get(5).getName()));

        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        DdlRequestContext context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);
        context.setDatamartName(schema);

        when(parserService.parse(any()))
                .thenReturn(Future.succeededFuture(parse(contextProvider, new QueryParserRequest(((SqlAlterView) sqlNode).getQuery(), logicSchema))));

        when(columnMetadataService.getColumnMetadata(any(QueryParserRequest.class)))
                .thenReturn(Future.succeededFuture(singletonList(ColumnMetadata.builder()
                        .name("id")
                        .type(ColumnType.BIGINT)
                        .build())));

        when(entityDao.getEntity(schema, entityList.get(4).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(4)));

        when(entityDao.getEntity(schema, entityList.get(5).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(5)));

        when(entityDao.getEntity(schema, entityList.get(0).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(0)));

        when(deltaServiceDao.getDeltaOk(schema))
                .thenReturn(Future.succeededFuture(deltaOk));

        when(changelogDao.writeNewRecord(anyString(), anyString(), anyString(), any()))
                .thenReturn(Future.succeededFuture());

        when(entityDao.setEntityState(any(), any(), anyString(), eq(SetEntityState.UPDATE)))
                .thenReturn(Future.succeededFuture());

        alterViewExecutor.execute(context, sqlNodeName)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.failed()) {
                        fail(ar.cause());
                    }
                    assertTrue(ar.succeeded());
                }).completeNow());
    }

    @Test
    void executeWithTimestampSuccess(VertxTestContext testContext) throws SqlParseException {
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);

        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(schema);
        queryRequest.setSql(String.format("ALTER VIEW %s.%s AS SELECT * FROM %s.%s WHERE timestamp_col = '2020-12-01 00:00:00'",
                schema, entityList.get(0).getName(), schema, entityList.get(6).getName()));
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        DdlRequestContext context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);

        when(parserService.parse(any()))
                .thenReturn(Future.succeededFuture(parse(contextProvider, new QueryParserRequest(((SqlAlterView) sqlNode).getQuery(), logicSchema))));

        when(columnMetadataService.getColumnMetadata(any(QueryParserRequest.class)))
                .thenReturn(Future.succeededFuture(Arrays.asList(ColumnMetadata.builder()
                                .name("id")
                                .type(ColumnType.BIGINT)
                                .build(),
                        ColumnMetadata.builder()
                                .name("timestamp_col")
                                .type(ColumnType.TIMESTAMP)
                                .build())));

        when(entityDao.getEntity(schema, entityList.get(0).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(0)));

        when(entityDao.getEntity(schema, entityList.get(6).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(6)));

        when(deltaServiceDao.getDeltaOk(schema))
                .thenReturn(Future.succeededFuture(deltaOk));

        when(changelogDao.writeNewRecord(anyString(), anyString(), anyString(), any()))
                .thenReturn(Future.succeededFuture());

        when(entityDao.setEntityState(any(), any(), anyString(), eq(SetEntityState.UPDATE)))
                .thenReturn(Future.succeededFuture());

        alterViewExecutor.execute(context, sqlNodeName)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.failed()) {
                        fail(ar.cause());
                    }
                    assertTrue(ar.succeeded());
                }).completeNow());
    }

    @Test
    void executeWrongTimestampFormatError(VertxTestContext testContext) throws SqlParseException {
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);

        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(schema);

        queryRequest.setSql(String.format("ALTER VIEW %s.%s AS SELECT * FROM %s.%s WHERE timestamp_col = '123456'",
                schema, entityList.get(0).getName(), schema, entityList.get(6).getName()));
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        DdlRequestContext context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);

        when(parserService.parse(any()))
                .thenReturn(Future.succeededFuture(parse(contextProvider, new QueryParserRequest(((SqlAlterView) sqlNode).getQuery(), logicSchema))));

        lenient().when(columnMetadataService.getColumnMetadata(any(QueryParserRequest.class)))
                .thenReturn(Future.succeededFuture(Arrays.asList(ColumnMetadata.builder()
                                .name("id")
                                .type(ColumnType.BIGINT)
                                .build(),
                        ColumnMetadata.builder()
                                .name("timestamp_col")
                                .type(ColumnType.TIMESTAMP)
                                .build())));

        lenient().when(entityDao.getEntity(schema, entityList.get(0).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(0)));

        when(entityDao.getEntity(schema, entityList.get(6).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(6)));

        alterViewExecutor.execute(context, sqlNodeName)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof ValidationDtmException);
                }).completeNow());
    }
}
