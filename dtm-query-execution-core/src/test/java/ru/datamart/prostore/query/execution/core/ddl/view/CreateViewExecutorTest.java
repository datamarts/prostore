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
package ru.datamart.prostore.query.execution.core.ddl.view;

import ru.datamart.prostore.cache.service.CacheService;
import ru.datamart.prostore.common.dto.QueryParserRequest;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.common.reader.QueryRequest;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.common.request.DatamartRequest;
import ru.datamart.prostore.query.calcite.core.configuration.CalciteCoreConfiguration;
import ru.datamart.prostore.query.calcite.core.extension.ddl.SqlCreateView;
import ru.datamart.prostore.query.calcite.core.framework.DtmCalciteFramework;
import ru.datamart.prostore.query.calcite.core.provider.CalciteContextProvider;
import ru.datamart.prostore.query.calcite.core.rel2sql.DtmRelToSqlConverter;
import ru.datamart.prostore.query.calcite.core.service.QueryParserService;
import ru.datamart.prostore.query.execution.core.base.dto.cache.EntityKey;
import ru.datamart.prostore.query.execution.core.base.exception.entity.EntityAlreadyExistsException;
import ru.datamart.prostore.query.execution.core.base.exception.entity.EntityNotExistsException;
import ru.datamart.prostore.query.execution.core.base.exception.table.ValidationDtmException;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacade;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.*;
import ru.datamart.prostore.query.execution.core.base.service.metadata.InformationSchemaService;
import ru.datamart.prostore.query.execution.core.base.service.metadata.LogicalSchemaProvider;
import ru.datamart.prostore.query.execution.core.base.service.metadata.MetadataExecutor;
import ru.datamart.prostore.query.execution.core.calcite.configuration.CalciteConfiguration;
import ru.datamart.prostore.query.execution.core.calcite.factory.CoreCalciteSchemaFactory;
import ru.datamart.prostore.query.execution.core.calcite.factory.CoreSchemaFactory;
import ru.datamart.prostore.query.execution.core.calcite.service.CoreCalciteContextProvider;
import ru.datamart.prostore.query.execution.core.ddl.dto.DdlRequestContext;
import ru.datamart.prostore.query.execution.core.ddl.service.impl.view.CreateViewExecutor;
import ru.datamart.prostore.query.execution.core.delta.dto.OkDelta;
import ru.datamart.prostore.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import ru.datamart.prostore.query.execution.core.dml.service.ColumnMetadataService;
import ru.datamart.prostore.query.execution.model.metadata.ColumnMetadata;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static ru.datamart.prostore.query.execution.core.utils.TestUtils.*;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith({VertxExtension.class, MockitoExtension.class})
class CreateViewExecutorTest {


    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final SqlParser.Config parserConfig = calciteConfiguration
            .configEddlParser(calciteCoreConfiguration.eddlParserImplFactory());
    private final CoreCalciteSchemaFactory coreSchemaFactory = new CoreCalciteSchemaFactory(new CoreSchemaFactory());
    private final CalciteContextProvider contextProvider = new CoreCalciteContextProvider(parserConfig, coreSchemaFactory);
    private final DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
    private final FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
    private final DtmRelToSqlConverter relToSqlConverter = new DtmRelToSqlConverter(SQL_DIALECT);
    private final List<Entity> entityList = new ArrayList<>();
    private final OkDelta deltaOk = OkDelta.builder()
            .deltaNum(1)
            .build();

    @Mock
    private ServiceDbFacade serviceDbFacade;
    @Mock
    private ServiceDbDao serviceDbDao;
    @Mock
    private EntityDao entityDao;
    @Mock
    private DatamartDao datamartDao;
    @Mock
    private ChangelogDao changelogDao;
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
    private InformationSchemaService informationSchemaService;
    @Mock
    private QueryParserService parserService;
    @Captor
    private ArgumentCaptor<Entity> entityArgumentCaptor;
    @Captor
    private ArgumentCaptor<String> changeQueryCaptor;

    private Planner planner;
    private CreateViewExecutor createViewExecutor;
    private String sqlNodeName;
    private String schema;
    private List<Datamart> logicSchema;
    private QueryRequest queryRequest;

    @BeforeEach
    void setUp() {
        lenient().when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        lenient().when(serviceDbDao.getEntityDao()).thenReturn(entityDao);
        lenient().when(serviceDbDao.getDatamartDao()).thenReturn(datamartDao);
        lenient().when(serviceDbFacade.getDeltaServiceDao()).thenReturn(deltaServiceDao);
        lenient().when(serviceDbDao.getChangelogDao()).thenReturn(changelogDao);
        lenient().when(informationSchemaService.validate(anyString())).thenReturn(Future.succeededFuture());

        planner = DtmCalciteFramework.getPlanner(frameworkConfig);
        createViewExecutor = new CreateViewExecutor(metadataExecutor,
                serviceDbFacade,
                SQL_DIALECT,
                entityCacheService,
                logicalSchemaProvider,
                columnMetadataService,
                parserService,
                relToSqlConverter,
                informationSchemaService);
        schema = "shares";
        queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(schema);
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
        lenient().when(metadataExecutor.execute(any())).thenReturn(Future.succeededFuture());
        lenient().when(logicalSchemaProvider.getSchemaFromQuery(any(), any()))
                .thenReturn(Future.succeededFuture(logicSchema));
    }

    @Test
    void executeSuccess(VertxTestContext testContext) throws SqlParseException {
        queryRequest.setSql(String.format("CREATE VIEW %s.%s AS SELECT * FROM %s.%s",
                schema, entityList.get(0).getName(), schema, entityList.get(1).getName()));
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        DdlRequestContext context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);

        when(parserService.parse(any()))
                .thenReturn(Future.succeededFuture(parse(contextProvider, new QueryParserRequest(((SqlCreateView) sqlNode).getQuery(), logicSchema))));

        when(columnMetadataService.getColumnMetadata(any(QueryParserRequest.class)))
                .thenReturn(Future.succeededFuture(singletonList(ColumnMetadata.builder()
                        .name("id")
                        .type(ColumnType.BIGINT)
                        .build())));

        when(entityDao.getEntity(schema, entityList.get(1).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(1)));

        when(entityDao.getEntity(schema, entityList.get(0).getName()))
                .thenReturn(Future.failedFuture(new EntityNotExistsException(entityList.get(0).getName())));

        when(deltaServiceDao.getDeltaOk(schema)).thenReturn(Future.succeededFuture(deltaOk));
        when(changelogDao.writeNewRecord(anyString(), anyString(), changeQueryCaptor.capture(), any())).thenReturn(Future.succeededFuture());
        when(entityDao.setEntityState(entityArgumentCaptor.capture(), any(), anyString(), eq(SetEntityState.CREATE)))
                .thenReturn(Future.succeededFuture());


        createViewExecutor.execute(context, sqlNodeName)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    val value = entityArgumentCaptor.getValue();
                    assertEquals("SELECT id FROM shares.test_table", value.getViewQuery());
                    assertEquals("CREATE VIEW shares.test_view AS\n" +
                            "SELECT id\n" +
                            "FROM shares.test_table", changeQueryCaptor.getValue());
                }).completeNow());
    }

    @Test
    void shouldFailWhenInformationSchemaInQuery(VertxTestContext testContext) throws SqlParseException {
        queryRequest.setSql(String.format("CREATE VIEW %s.%s AS SELECT * FROM information_schema.tables",
                schema, entityList.get(0).getName()));
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        DdlRequestContext context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);

        lenient().when(parserService.parse(any()))
                .thenReturn(Future.succeededFuture(parse(contextProvider, new QueryParserRequest(((SqlCreateView) sqlNode).getQuery(), logicSchema))));

        lenient().when(columnMetadataService.getColumnMetadata(any(QueryParserRequest.class)))
                .thenReturn(Future.succeededFuture(singletonList(ColumnMetadata.builder()
                        .name("id")
                        .type(ColumnType.BIGINT)
                        .build())));

        lenient().when(entityDao.getEntity(schema, entityList.get(1).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(1)));

        lenient().when(entityDao.getEntity(schema, entityList.get(0).getName()))
                .thenReturn(Future.failedFuture(new EntityNotExistsException(entityList.get(0).getName())));

        lenient().when(deltaServiceDao.getDeltaOk(schema)).thenReturn(Future.succeededFuture(deltaOk));
        lenient().when(changelogDao.writeNewRecord(anyString(), anyString(), changeQueryCaptor.capture(), any())).thenReturn(Future.succeededFuture());
        lenient().when(entityDao.setEntityState(entityArgumentCaptor.capture(), any(), anyString(), eq(SetEntityState.CREATE)))
                .thenReturn(Future.succeededFuture());

        createViewExecutor.execute(context, sqlNodeName)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }

                    assertEquals("Using of INFORMATION_SCHEMA is forbidden [information_schema.tables]", ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void executeReplaceSuccess() throws SqlParseException {
        Promise<QueryResult> promise = Promise.promise();

        queryRequest.setSql(String.format("CREATE OR REPLACE VIEW %s.%s AS SELECT * FROM %s.%s",
                schema, entityList.get(0).getName(), schema, entityList.get(1).getName()));
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        DdlRequestContext context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);

        when(parserService.parse(any()))
                .thenReturn(Future.succeededFuture(parse(contextProvider, new QueryParserRequest(((SqlCreateView) sqlNode).getQuery(), logicSchema))));

        when(columnMetadataService.getColumnMetadata(any(QueryParserRequest.class)))
                .thenReturn(Future.succeededFuture(singletonList(ColumnMetadata.builder()
                        .name("id")
                        .type(ColumnType.BIGINT)
                        .build())));

        when(entityDao.getEntity(schema, entityList.get(1).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(1)))
                .thenReturn(Future.succeededFuture(entityList.get(1)));

        when(entityDao.getEntity(schema, entityList.get(0).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(0)));

        when(deltaServiceDao.getDeltaOk(schema)).thenReturn(Future.succeededFuture(deltaOk));
        when(changelogDao.writeNewRecord(anyString(), anyString(), changeQueryCaptor.capture(), any())).thenReturn(Future.succeededFuture());

        when(entityDao.setEntityState(entityArgumentCaptor.capture(), any(), anyString(), eq(SetEntityState.CREATE)))
                .thenReturn(Future.failedFuture(new EntityAlreadyExistsException("")));

        when(entityDao.setEntityState(any(), any(), anyString(), eq(SetEntityState.UPDATE)))
                .thenReturn(Future.succeededFuture());

        createViewExecutor.execute(context, sqlNodeName)
                .onComplete(promise);
        assertTrue(promise.future().succeeded());
        assertEquals("CREATE OR REPLACE VIEW shares.test_view AS\n" +
                "SELECT id\n" +
                "FROM shares.test_table", changeQueryCaptor.getValue());
    }

    @Test
    void executeCreateEntityError(VertxTestContext testContext) throws SqlParseException {
        queryRequest.setSql(String.format("CREATE VIEW %s.%s AS SELECT * FROM %s.%s",
                schema, entityList.get(0).getName(), schema, entityList.get(1).getName()));
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        DdlRequestContext context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);

        when(parserService.parse(any()))
                .thenReturn(Future.succeededFuture(parse(contextProvider, new QueryParserRequest(((SqlCreateView) sqlNode).getQuery(), logicSchema))));

        when(columnMetadataService.getColumnMetadata(any(QueryParserRequest.class)))
                .thenReturn(Future.succeededFuture(singletonList(ColumnMetadata.builder()
                        .name("id")
                        .type(ColumnType.BIGINT)
                        .build())));

        when(entityDao.getEntity(schema, entityList.get(1).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(1)));

        when(entityDao.getEntity(schema, entityList.get(0).getName()))
                .thenReturn(Future.failedFuture(new EntityNotExistsException(entityList.get(0).getName())));

        when(deltaServiceDao.getDeltaOk(schema)).thenReturn(Future.succeededFuture(deltaOk));
        when(changelogDao.writeNewRecord(anyString(), anyString(), changeQueryCaptor.capture(), any())).thenReturn(Future.succeededFuture());
        when(entityDao.setEntityState(any(), any(), anyString(), eq(SetEntityState.CREATE)))
                .thenReturn(Future.failedFuture(new DtmException("create entity error")));

        createViewExecutor.execute(context, sqlNodeName)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.failed());
                    assertEquals("create entity error", ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void executeInvalidViewError() throws SqlParseException {
        Promise<QueryResult> promise = Promise.promise();

        queryRequest.setSql(String.format("CREATE VIEW %s.%s AS SELECT * FROM %s.%s",
                schema, entityList.get(0).getName(), schema, entityList.get(0).getName()));
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        DdlRequestContext context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);

        createViewExecutor.execute(context, sqlNodeName)
                .onComplete(promise);
        assertTrue(promise.future().failed());
    }

    @Test
    void executeContainsCollateError() throws SqlParseException {
        Promise<QueryResult> promise = Promise.promise();

        queryRequest.setSql(String.format("CREATE VIEW %s.%s AS SELECT * FROM %s.%s WHERE varchar_col = 'test' COLLATE 'unicode_ci'",
                schema, entityList.get(0).getName(), schema, entityList.get(0).getName()));
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        DdlRequestContext context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);

        createViewExecutor.execute(context, sqlNodeName)
                .onComplete(promise);
        assertTrue(promise.future().failed());
    }

    @Test
    void executeEntityAlreadyExistError(VertxTestContext testContext) throws SqlParseException {
        queryRequest.setSql(String.format("CREATE VIEW %s.%s AS SELECT * FROM %s.%s",
                schema, entityList.get(0).getName(), schema, entityList.get(1).getName()));
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        DdlRequestContext context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);

        when(parserService.parse(any()))
                .thenReturn(Future.succeededFuture(parse(contextProvider, new QueryParserRequest(((SqlCreateView) sqlNode).getQuery(), logicSchema))));

        when(columnMetadataService.getColumnMetadata(any(QueryParserRequest.class)))
                .thenReturn(Future.succeededFuture(singletonList(ColumnMetadata.builder()
                        .name("id")
                        .type(ColumnType.BIGINT)
                        .build())));

        when(entityDao.getEntity(schema, entityList.get(0).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(0)));

        when(entityDao.getEntity(schema, entityList.get(1).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(1)));

        createViewExecutor.execute(context, sqlNodeName)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof EntityAlreadyExistsException);
                }).completeNow());
    }

    @Test
    void executeReplaceWrongEntityTypeError() throws SqlParseException {
        Promise<QueryResult> promise = Promise.promise();

        queryRequest.setSql(String.format("CREATE OR REPLACE VIEW %s.%s AS SELECT * FROM %s.%s",
                schema, entityList.get(1).getName(), schema, entityList.get(2).getName()));
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        DdlRequestContext context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);

        when(parserService.parse(any()))
                .thenReturn(Future.succeededFuture(parse(contextProvider, new QueryParserRequest(((SqlCreateView) sqlNode).getQuery(), logicSchema))));

        when(columnMetadataService.getColumnMetadata(any(QueryParserRequest.class)))
                .thenReturn(Future.succeededFuture(singletonList(ColumnMetadata.builder()
                        .name("id")
                        .type(ColumnType.BIGINT)
                        .build())));

        when(entityDao.getEntity(schema, entityList.get(2).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(2)))
                .thenReturn(Future.succeededFuture(entityList.get(2)));

        when(entityDao.getEntity(schema, entityList.get(1).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(1)));

        createViewExecutor.execute(context, sqlNodeName)
                .onComplete(promise);
        assertTrue(promise.future().failed());
        assertEquals(String.format("Entity %s is not a view", entityList.get(1).getName()), promise.future().cause().getMessage());
    }

    @Test
    void executeInnerJoinWrongEntityTypeError(VertxTestContext testContext) throws SqlParseException {
        testJoinWithWrongEntityType(testContext, "INNER");
    }

    @Test
    void executeFullJoinWrongEntityTypeError(VertxTestContext testContext) throws SqlParseException {
        testJoinWithWrongEntityType(testContext, "FULL");
    }

    @Test
    void executeLeftJoinWrongEntityTypeError(VertxTestContext testContext) throws SqlParseException {
        testJoinWithWrongEntityType(testContext, "LEFT");
    }

    @Test
    void executeRightJoinWrongEntityTypeError(VertxTestContext testContext) throws SqlParseException {
        testJoinWithWrongEntityType(testContext, "RIGHT");
    }

    @Test
    void executeCrossJoinWrongEntityTypeError(VertxTestContext testContext) throws SqlParseException {
        testJoinWithWrongEntityType(testContext, "CROSS");
    }

    @Test
    void executeMultipleJoinWrongEntityTypeError(VertxTestContext testContext) throws SqlParseException {
        queryRequest.setSql(String.format("CREATE OR REPLACE VIEW %s.%s AS SELECT * FROM %s.%s " +
                        "JOIN %s.%s ON %s.id = %s.id " +
                        "JOIN %s.%s ON %s.id = %s.id",
                schema, entityList.get(1).getName(), schema, entityList.get(2).getName(),
                schema, entityList.get(3).getName(), entityList.get(2).getName(), entityList.get(3).getName(),
                schema, entityList.get(0).getName(), entityList.get(2).getName(), entityList.get(0).getName()));
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        DdlRequestContext context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);

        when(entityDao.getEntity(schema, entityList.get(2).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(2)))
                .thenReturn(Future.succeededFuture(entityList.get(2)));

        when(entityDao.getEntity(schema, entityList.get(0).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(0)));

        when(entityDao.getEntity(schema, entityList.get(3).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(3)));

        createViewExecutor.execute(context, sqlNodeName)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.failed());
                    assertThat(ar.cause().getMessage()).startsWith("Disallowed view or directive in a subquery");
                }).completeNow());
    }

    @Test
    void executeWithTimestampSuccess(VertxTestContext testContext) throws SqlParseException {
        queryRequest.setSql(String.format("CREATE VIEW %s.%s AS SELECT * FROM %s.%s WHERE timestamp_col = '2020-12-01 00:00:00'",
                schema, entityList.get(0).getName(), schema, entityList.get(6).getName()));
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        DdlRequestContext context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);

        when(parserService.parse(any()))
                .thenReturn(Future.succeededFuture(parse(contextProvider, new QueryParserRequest(((SqlCreateView) sqlNode).getQuery(), logicSchema))));

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
                .thenReturn(Future.failedFuture(new EntityNotExistsException(entityList.get(0).getName())));

        when(entityDao.getEntity(schema, entityList.get(6).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(6)));

        when(deltaServiceDao.getDeltaOk(schema)).thenReturn(Future.succeededFuture(deltaOk));
        when(changelogDao.writeNewRecord(anyString(), anyString(), changeQueryCaptor.capture(), any())).thenReturn(Future.succeededFuture());
        when(entityDao.setEntityState(any(), any(), anyString(), eq(SetEntityState.CREATE)))
                .thenReturn(Future.succeededFuture());

        createViewExecutor.execute(context, sqlNodeName)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.failed()) {
                        fail(ar.cause());
                    }
                    assertEquals("CREATE VIEW shares.test_view AS\n" +
                            "SELECT id, timestamp_col\n" +
                            "FROM shares.entity\n" +
                            "WHERE timestamp_col = '2020-12-01 00:00:00'", changeQueryCaptor.getValue());
                }).completeNow());
    }

    @Test
    void shouldFailWhenQueryValidationFails(VertxTestContext testContext) throws SqlParseException {
        queryRequest.setSql(String.format("CREATE VIEW %s.%s AS SELECT * FROM %s.%s",
                schema, entityList.get(0).getName(), schema, entityList.get(1).getName()));
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        DdlRequestContext context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);

        when(parserService.parse(any()))
                .thenReturn(Future.succeededFuture(parse(contextProvider, new QueryParserRequest(((SqlCreateView) sqlNode).getQuery(), logicSchema))));

        when(informationSchemaService.validate(anyString())).thenReturn(Future.failedFuture("validation error"));

        when(columnMetadataService.getColumnMetadata(any(QueryParserRequest.class)))
                .thenReturn(Future.succeededFuture(singletonList(ColumnMetadata.builder()
                        .name("id")
                        .type(ColumnType.BIGINT)
                        .build())));

        when(entityDao.getEntity(schema, entityList.get(1).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(1)));

        createViewExecutor.execute(context, sqlNodeName)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }

                    assertEquals("validation error", ar.cause().getMessage());
                    verifyNoInteractions(changelogDao, metadataExecutor);
                }).completeNow());
    }

    @Test
    void executeWrongTimestampFormatError(VertxTestContext testContext) throws SqlParseException {
        queryRequest.setSql(String.format("CREATE VIEW %s.%s AS SELECT * FROM %s.%s WHERE timestamp_col = '123456'",
                schema, entityList.get(0).getName(), schema, entityList.get(6).getName()));
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        DdlRequestContext context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);

        when(parserService.parse(any()))
                .thenReturn(Future.succeededFuture(parse(contextProvider, new QueryParserRequest(((SqlCreateView) sqlNode).getQuery(), logicSchema))));

        when(entityDao.getEntity(schema, entityList.get(6).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(6)));

        createViewExecutor.execute(context, sqlNodeName)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof ValidationDtmException);
                }).completeNow());
    }

    @Test
    void shouldFailOnNameValidation(VertxTestContext testContext) throws SqlParseException {
        entityList.get(0).setName("имя");

        queryRequest.setSql(String.format("CREATE VIEW %s.%s AS SELECT * FROM %s.%s",
                schema, entityList.get(0).getName(), schema, entityList.get(1).getName()));
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        DdlRequestContext context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);

        when(parserService.parse(any()))
                .thenReturn(Future.succeededFuture(parse(contextProvider, new QueryParserRequest(((SqlCreateView) sqlNode).getQuery(), logicSchema))));

        when(columnMetadataService.getColumnMetadata(any(QueryParserRequest.class)))
                .thenReturn(Future.succeededFuture(singletonList(ColumnMetadata.builder()
                        .name("id")
                        .type(ColumnType.BIGINT)
                        .build())));

        when(entityDao.getEntity(schema, entityList.get(1).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(1)));

        lenient().when(entityDao.getEntity(schema, entityList.get(0).getName()))
                .thenReturn(Future.failedFuture(new EntityNotExistsException(entityList.get(0).getName())));

        lenient().when(deltaServiceDao.getDeltaOk(schema)).thenReturn(Future.succeededFuture(deltaOk));
        lenient().when(changelogDao.writeNewRecord(anyString(), anyString(), changeQueryCaptor.capture(), any())).thenReturn(Future.succeededFuture());
        lenient().when(entityDao.setEntityState(entityArgumentCaptor.capture(), any(), anyString(), eq(SetEntityState.CREATE)))
                .thenReturn(Future.succeededFuture());


        createViewExecutor.execute(context, sqlNodeName)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }

                    assertEquals("Entity name [shares.имя] is not valid, allowed pattern is [a-zA-Z][a-zA-Z0-9_]*", ar.cause().getMessage());
                }).completeNow());
    }

    private void testJoinWithWrongEntityType(VertxTestContext testContext, String joinType) throws SqlParseException {
        String sql = String.format("CREATE OR REPLACE VIEW %s.%s AS SELECT * FROM %s.%s " +
                        "%s JOIN %s.%s",
                schema, entityList.get(1).getName(), schema, entityList.get(2).getName(),
                joinType, schema, entityList.get(0).getName());
        if (!joinType.equals("CROSS")) {
            sql += String.format(" ON %s.id = %s.id", entityList.get(2).getName(), entityList.get(0).getName());
        }
        queryRequest.setSql(sql);
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        DdlRequestContext context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);

        lenient().when(parserService.parse(any()))
                .thenReturn(Future.succeededFuture(parse(contextProvider, new QueryParserRequest(((SqlCreateView) sqlNode).getQuery(), logicSchema))));

        lenient().when(columnMetadataService.getColumnMetadata(any(QueryParserRequest.class)))
                .thenReturn(Future.succeededFuture(singletonList(ColumnMetadata.builder()
                        .name("id")
                        .type(ColumnType.BIGINT)
                        .build())));

        lenient().when(entityDao.getEntity(schema, entityList.get(2).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(2)))
                .thenReturn(Future.succeededFuture(entityList.get(2)));

        lenient().when(entityDao.getEntity(schema, entityList.get(1).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(1)));

        lenient().when(entityDao.getEntity(schema, entityList.get(0).getName()))
                .thenReturn(Future.succeededFuture(entityList.get(0)));

        lenient().when(entityDao.createEntity(any()))
                .thenReturn(Future.failedFuture(new EntityAlreadyExistsException("")));

        createViewExecutor.execute(context, sqlNodeName)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.failed());
                    assertThat(ar.cause().getMessage()).startsWith("Disallowed view or directive in a subquery");
                }).completeNow());
    }
}
