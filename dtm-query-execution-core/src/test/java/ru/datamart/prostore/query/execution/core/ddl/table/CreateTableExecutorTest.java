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
package ru.datamart.prostore.query.execution.core.ddl.table;

import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.reader.QueryRequest;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.common.request.DatamartRequest;
import ru.datamart.prostore.query.calcite.core.configuration.CalciteCoreConfiguration;
import ru.datamart.prostore.query.calcite.core.framework.DtmCalciteFramework;
import ru.datamart.prostore.query.execution.core.base.exception.datamart.DatamartNotExistsException;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacade;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacadeImpl;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.ChangelogDao;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.DatamartDao;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.EntityDao;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.ServiceDbDao;
import ru.datamart.prostore.query.execution.core.base.service.metadata.MetadataCalciteGenerator;
import ru.datamart.prostore.query.execution.core.base.service.metadata.MetadataExecutor;
import ru.datamart.prostore.query.execution.core.calcite.configuration.CalciteConfiguration;
import ru.datamart.prostore.query.execution.core.ddl.dto.DdlRequestContext;
import ru.datamart.prostore.query.execution.core.ddl.service.QueryResultDdlExecutor;
import ru.datamart.prostore.query.execution.core.ddl.service.impl.table.CreateTableExecutor;
import ru.datamart.prostore.query.execution.core.delta.dto.OkDelta;
import ru.datamart.prostore.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import ru.datamart.prostore.query.execution.core.plugin.service.DataSourcePluginService;
import ru.datamart.prostore.query.execution.core.plugin.service.impl.DataSourcePluginServiceImpl;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.val;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static ru.datamart.prostore.query.execution.core.utils.TestUtils.SQL_DIALECT;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class CreateTableExecutorTest {

    private static final String DATAMART = "shares";
    private static final String SQL_NODE_NAME = "accounts";
    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final SqlParser.Config parserConfig = calciteConfiguration.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory());
    private final MetadataCalciteGenerator metadataCalciteGenerator = mock(MetadataCalciteGenerator.class);
    private final MetadataExecutor metadataExecutor = mock(MetadataExecutor.class);
    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final ChangelogDao changelogDao = mock(ChangelogDao.class);
    private final ServiceDbDao serviceDbDao = mock(ServiceDbDao.class);
    private final DatamartDao datamartDao = mock(DatamartDao.class);
    private final EntityDao entityDao = mock(EntityDao.class);
    private final DeltaServiceDao deltaServiceDao = mock(DeltaServiceDao.class);
    private final DataSourcePluginService dataSourcePluginService = mock(DataSourcePluginServiceImpl.class);
    private final OkDelta deltaOk = OkDelta.builder()
            .deltaNum(1)
            .build();
    private QueryResultDdlExecutor createTableDdlExecutor;
    private DdlRequestContext context;
    private Entity entity;

    @BeforeEach
    void setUp() {
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        when(serviceDbDao.getDatamartDao()).thenReturn(datamartDao);
        when(serviceDbDao.getEntityDao()).thenReturn(entityDao);
        when(serviceDbFacade.getDeltaServiceDao()).thenReturn(deltaServiceDao);
        when(serviceDbDao.getChangelogDao()).thenReturn(changelogDao);
        Set<SourceType> sourceTypes = new HashSet<>();
        sourceTypes.add(SourceType.ADB);
        sourceTypes.add(SourceType.ADG);
        sourceTypes.add(SourceType.ADQM);
        when(dataSourcePluginService.getSourceTypes()).thenReturn(sourceTypes);
        when(dataSourcePluginService.getSourceTypes()).thenReturn(sourceTypes);
        createTableDdlExecutor = new CreateTableExecutor(metadataExecutor, serviceDbFacade, SQL_DIALECT, metadataCalciteGenerator, dataSourcePluginService);

        EntityField f1 = new EntityField(0, "id", ColumnType.INT, false);
        f1.setPrimaryOrder(1);
        f1.setShardingOrder(1);
        EntityField f2 = new EntityField(1, "name", ColumnType.VARCHAR, true);
        f2.setSize(100);
        entity = new Entity(SQL_NODE_NAME, DATAMART, Arrays.asList(f1, f2));
    }

    private void prepareContext(String sql, boolean isLogicalOnly) throws SqlParseException {
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);

        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(DATAMART);

        if (isLogicalOnly) {
            sql += " logical_only";
        }
        queryRequest.setSql(sql);

        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);
        context.setDatamartName(DATAMART);
    }

    @Test
    void executeSuccess() throws SqlParseException {
        prepareContext("create table shares.accounts (id integer, name varchar(100))", false);

        Promise<QueryResult> promise = Promise.promise();
        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);

        when(datamartDao.existsDatamart(DATAMART))
                .thenReturn(Future.succeededFuture(true));
        when(entityDao.existsEntity(DATAMART, entity.getName()))
                .thenReturn(Future.succeededFuture(false));
        when(deltaServiceDao.getDeltaOk(DATAMART)).thenReturn(Future.succeededFuture(deltaOk));
        when(changelogDao.writeNewRecord(anyString(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(metadataExecutor.execute(any())).thenReturn(Future.succeededFuture());
        when(entityDao.setEntityState(any(), any(), anyString(), any()))
                .thenReturn(Future.succeededFuture());

        createTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);
        assertNotNull(promise.future().result());

        verify(datamartDao).existsDatamart(DATAMART);
        verify(entityDao).setEntityState(any(), any(), eq("CREATE TABLE shares.accounts (id INTEGER, name VARCHAR(100))"), any());
        verify(entityDao).existsEntity(DATAMART, entity.getName());
        verify(metadataExecutor).execute(context);
    }

    @Test
    void executeSuccessWithoutDeltaOk() throws SqlParseException {
        prepareContext("create table shares.accounts (id integer, name varchar(100))", false);

        Promise<QueryResult> promise = Promise.promise();
        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);

        when(datamartDao.existsDatamart(DATAMART))
                .thenReturn(Future.succeededFuture(true));
        when(entityDao.existsEntity(DATAMART, entity.getName()))
                .thenReturn(Future.succeededFuture(false));
        when(deltaServiceDao.getDeltaOk(DATAMART)).thenReturn(Future.succeededFuture(null));
        when(changelogDao.writeNewRecord(anyString(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(metadataExecutor.execute(any())).thenReturn(Future.succeededFuture());
        when(entityDao.setEntityState(any(), any(), anyString(), any()))
                .thenReturn(Future.succeededFuture());

        createTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);
        assertNotNull(promise.future().result());

        verify(datamartDao).existsDatamart(DATAMART);
        verify(entityDao).existsEntity(DATAMART, entity.getName());
        verify(entityDao).setEntityState(any(), eq(null), eq("CREATE TABLE shares.accounts (id INTEGER, name VARCHAR(100))"), any());
        verify(metadataExecutor).execute(context);
    }

    @Test
    void executeGetDeltaOkError() throws SqlParseException {
        prepareContext("create table shares.accounts (id integer, name varchar(100))", false);

        Promise<QueryResult> promise = Promise.promise();
        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);

        when(datamartDao.existsDatamart(DATAMART))
                .thenReturn(Future.succeededFuture(true));
        when(entityDao.existsEntity(DATAMART, entity.getName()))
                .thenReturn(Future.succeededFuture(false));
        when(deltaServiceDao.getDeltaOk(DATAMART)).thenReturn(Future.failedFuture("get delta ok error"));

        createTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);
        assertTrue(promise.future().failed());
        assertEquals("get delta ok error", promise.future().cause().getMessage());
    }

    @Test
    void executeSuccessLogicalOnly() throws SqlParseException {
        prepareContext("create table accounts (id integer, name varchar(100))", true);

        Promise<QueryResult> promise = Promise.promise();
        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);

        when(datamartDao.existsDatamart(DATAMART))
                .thenReturn(Future.succeededFuture(true));
        when(entityDao.existsEntity(DATAMART, entity.getName()))
                .thenReturn(Future.succeededFuture(false));
        when(deltaServiceDao.getDeltaOk(DATAMART)).thenReturn(Future.succeededFuture(deltaOk));
        when(changelogDao.writeNewRecord(anyString(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(entityDao.setEntityState(any(), any(), anyString(), any()))
                .thenReturn(Future.succeededFuture());

        createTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);
        assertNotNull(promise.future().result());

        verify(datamartDao).existsDatamart(DATAMART);
        verify(entityDao).existsEntity(DATAMART, entity.getName());
        verify(entityDao).setEntityState(any(), eq(deltaOk), eq("CREATE TABLE accounts (id INTEGER, name VARCHAR(100)) LOGICAL_ONLY"), any());
        verify(metadataExecutor, never()).execute(context);
    }

    @Test
    void executeWithInvalidShardingKeyError() throws SqlParseException {
        prepareContext("create table shares.accounts (id integer, name varchar(100))", false);

        Promise<QueryResult> promise = Promise.promise();
        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);
        entity.getFields().get(0).setShardingOrder(null);
        entity.getFields().get(1).setShardingOrder(1);

        when(datamartDao.existsDatamart(DATAMART))
                .thenReturn(Future.succeededFuture(true));

        when(entityDao.existsEntity(DATAMART, entity.getName()))
                .thenReturn(Future.succeededFuture(false));

        when(metadataExecutor.execute(any())).thenReturn(Future.succeededFuture());

        createTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);
        assertTrue(promise.future().failed());
        assertEquals("DISTRIBUTED BY clause must be a subset of the PRIMARY KEY", promise.future().cause().getMessage());
    }

    @Test
    void executeWithDuplicationFieldsError() throws SqlParseException {
        prepareContext("create table accounts (id integer, id integer)", false);

        EntityField f1 = new EntityField(0, "id", ColumnType.INT, false);
        f1.setPrimaryOrder(1);
        f1.setShardingOrder(1);
        EntityField f2 = new EntityField(1, "id", ColumnType.INT, false);
        entity = new Entity(SQL_NODE_NAME, DATAMART, Arrays.asList(f1, f2));

        Promise<QueryResult> promise = Promise.promise();
        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);

        createTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);
        assertTrue(promise.future().failed());
        assertEquals("Entity has duplication fields names", promise.future().cause().getMessage());
    }

    @Test
    void executeWithCharTypesZeroSizeError() throws SqlParseException {
        prepareContext("CREATE TABLE ACCOUNTS (ID INTEGER, NAME1 CHAR(0), NAME2 VARCHAR(0))", false);

        val f1 = EntityField.builder()
                .ordinalPosition(0)
                .name("ID")
                .type(ColumnType.INT)
                .nullable(false)
                .primaryOrder(1)
                .shardingOrder(1)
                .build();
        val f2 = EntityField.builder()
                .ordinalPosition(1)
                .name("NAME1")
                .type(ColumnType.CHAR)
                .size(0)
                .nullable(false)
                .build();
        val f3 = EntityField.builder()
                .ordinalPosition(2)
                .name("NAME2")
                .type(ColumnType.VARCHAR)
                .size(0)
                .nullable(false)
                .build();

        entity = new Entity(SQL_NODE_NAME, DATAMART, Arrays.asList(f1, f2, f3));

        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);

        Promise<QueryResult> promise = Promise.promise();
        createTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);
        assertTrue(promise.future().failed());
        assertEquals("Specifying the size for columns[NAME1, NAME2] with types[CHAR/VARCHAR] is required", promise.future().cause().getMessage());
    }

    @Test
    void executeWithCharTypesNullSizeError() throws SqlParseException {
        prepareContext("CREATE TABLE ACCOUNTS (ID INTEGER, NAME1 CHAR, NAME2 VARCHAR)", false);

        val f1 = EntityField.builder()
                .ordinalPosition(0)
                .name("ID")
                .type(ColumnType.INT)
                .nullable(false)
                .primaryOrder(1)
                .shardingOrder(1)
                .build();
        val f2 = EntityField.builder()
                .ordinalPosition(1)
                .name("NAME1")
                .type(ColumnType.CHAR)
                .nullable(false)
                .build();
        val f3 = EntityField.builder()
                .ordinalPosition(2)
                .name("NAME2")
                .type(ColumnType.VARCHAR)
                .nullable(false)
                .build();

        entity = new Entity(SQL_NODE_NAME, DATAMART, Arrays.asList(f1, f2, f3));

        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);

        Promise<QueryResult> promise = Promise.promise();
        createTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);
        assertTrue(promise.future().failed());
        assertEquals("Specifying the size for columns[NAME1] with types[CHAR/VARCHAR] is required", promise.future().cause().getMessage());
    }

    @Test
    void executeWithExistsDatamartError() throws SqlParseException {
        prepareContext("create table shares.accounts (id integer, name varchar(100))", false);

        Promise<QueryResult> promise = Promise.promise();

        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);

        when(datamartDao.existsDatamart(DATAMART))
                .thenReturn(Future.failedFuture(new DatamartNotExistsException(DATAMART)));

        createTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);
        assertNotNull(promise.future().cause());
        assertEquals(String.format("Database %s does not exist", DATAMART), promise.future().cause().getMessage());
    }

    @Test
    void executeWithNotExistsDatamartError() throws SqlParseException {
        prepareContext("create table shares.accounts (id integer, name varchar(100))", false);

        Promise<QueryResult> promise = Promise.promise();

        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);

        when(datamartDao.existsDatamart(DATAMART))
                .thenReturn(Future.succeededFuture(false));

        createTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);
        assertNotNull(promise.future().cause());
        assertEquals(String.format("Database %s does not exist", DATAMART), promise.future().cause().getMessage());
    }

    @Test
    void executeWithTableExists() throws SqlParseException {
        prepareContext("create table shares.accounts (id integer, name varchar(100))", false);

        Promise<QueryResult> promise = Promise.promise();
        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);

        when(datamartDao.existsDatamart(DATAMART))
                .thenReturn(Future.succeededFuture(true));

        when(entityDao.existsEntity(DATAMART, entity.getName()))
                .thenReturn(Future.succeededFuture(true));

        createTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);
        assertNotNull(promise.future().cause());
        assertEquals(String.format("Entity %s.%s already exists", DATAMART, entity.getName()), promise.future().cause().getMessage());
    }

    @Test
    void executeWithTableExistsError() throws SqlParseException {
        prepareContext("create table shares.accounts (id integer, name varchar(100))", false);

        Promise<QueryResult> promise = Promise.promise();
        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);

        when(datamartDao.existsDatamart(DATAMART))
                .thenReturn(Future.succeededFuture(true));

        when(entityDao.existsEntity(DATAMART, entity.getName()))
                .thenReturn(Future.failedFuture(new DtmException("exists entity error")));

        createTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);
        assertNotNull(promise.future().cause());
        assertEquals("exists entity error", promise.future().cause().getMessage());
    }

    @Test
    void executeWriteChangelogError() throws SqlParseException {
        prepareContext("create table shares.accounts (id integer, name varchar(100))", false);

        Promise<QueryResult> promise = Promise.promise();
        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);

        when(datamartDao.existsDatamart(DATAMART))
                .thenReturn(Future.succeededFuture(true));
        when(entityDao.existsEntity(DATAMART, entity.getName()))
                .thenReturn(Future.succeededFuture(false));
        when(deltaServiceDao.getDeltaOk(DATAMART)).thenReturn(Future.succeededFuture(deltaOk));
        when(changelogDao.writeNewRecord(DATAMART, entity.getName(), "CREATE TABLE shares.accounts (id INTEGER, name VARCHAR(100))", deltaOk))
                .thenReturn(Future.failedFuture("write new changelog record error"));

        createTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);
        assertTrue(promise.future().failed());
        assertEquals("write new changelog record error", promise.future().cause().getMessage());
    }

    @Test
    void executeWithMetadataDataSourceError() throws SqlParseException {
        prepareContext("create table shares.accounts (id integer, name varchar(100))", false);

        Promise<QueryResult> promise = Promise.promise();
        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);

        when(datamartDao.existsDatamart(DATAMART))
                .thenReturn(Future.succeededFuture(true));

        when(entityDao.existsEntity(DATAMART, entity.getName()))
                .thenReturn(Future.succeededFuture(false));

        when(deltaServiceDao.getDeltaOk(DATAMART))
                .thenReturn(Future.succeededFuture(deltaOk));

        when(changelogDao.writeNewRecord(anyString(), anyString(), anyString(), any()))
                .thenReturn(Future.succeededFuture());

        when(metadataExecutor.execute(any()))
                .thenReturn(Future.failedFuture(new DtmException("metadata executor error")));

        createTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);
        assertNotNull(promise.future().cause());
        assertEquals("metadata executor error", promise.future().cause().getMessage());
    }

    @Test
    void executeWithInsertTableError() throws SqlParseException {
        prepareContext("create table shares.accounts (id integer, name varchar(100))", false);

        Promise<QueryResult> promise = Promise.promise();
        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);

        when(datamartDao.existsDatamart(DATAMART))
                .thenReturn(Future.succeededFuture(true));
        when(entityDao.existsEntity(DATAMART, entity.getName()))
                .thenReturn(Future.succeededFuture(false));
        when(deltaServiceDao.getDeltaOk(DATAMART)).thenReturn(Future.succeededFuture(deltaOk));
        when(changelogDao.writeNewRecord(anyString(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(metadataExecutor.execute(any())).thenReturn(Future.succeededFuture());

        when(entityDao.setEntityState(any(), any(), anyString(), any()))
                .thenReturn(Future.failedFuture(new DtmException("create entity error")));

        createTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);
        assertNotNull(promise.future().cause());
        assertEquals("create entity error", promise.future().cause().getMessage());
    }

    @Test
    void executeWithMetadataGeneratorError() throws SqlParseException {
        prepareContext("create table shares.accounts (id integer, name varchar(100))", false);

        Promise<QueryResult> promise = Promise.promise();

        when(metadataCalciteGenerator.generateTableMetadata(any()))
                .thenThrow(new DtmException("metadata generator error"));

        createTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);
        assertNotNull(promise.future().cause());
        assertEquals("metadata generator error", promise.future().cause().getMessage());
    }

    @Test
    void shouldFailOnNameValidation() throws SqlParseException {
        val wrongDatamart = "схема";
        entity.setSchema(wrongDatamart);

        prepareContext("create table _схема.accounts (id integer, name varchar(100))", false);

        Promise<QueryResult> promise = Promise.promise();
        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);

        when(entityDao.existsEntity(wrongDatamart, entity.getName()))
                .thenReturn(Future.succeededFuture(false));
        when(deltaServiceDao.getDeltaOk(wrongDatamart)).thenReturn(Future.succeededFuture(deltaOk));
        when(changelogDao.writeNewRecord(anyString(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(metadataExecutor.execute(any())).thenReturn(Future.succeededFuture());
        when(entityDao.setEntityState(any(), any(), anyString(), any()))
                .thenReturn(Future.succeededFuture());

        createTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);

        if (promise.future().succeeded()) {
            fail("Unexpected success");
        }
        assertEquals("Entity name [схема.accounts] is not valid, allowed pattern is [a-zA-Z][a-zA-Z0-9_]*", promise.future().cause().getMessage());

        verifyNoInteractions(datamartDao);
    }
}
