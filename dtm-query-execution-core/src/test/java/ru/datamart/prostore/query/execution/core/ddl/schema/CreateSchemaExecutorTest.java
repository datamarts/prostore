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
package ru.datamart.prostore.query.execution.core.ddl.schema;

import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.reader.QueryRequest;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.common.request.DatamartRequest;
import ru.datamart.prostore.query.calcite.core.configuration.CalciteCoreConfiguration;
import ru.datamart.prostore.query.calcite.core.framework.DtmCalciteFramework;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacade;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacadeImpl;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.ChangelogDao;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.DatamartDao;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.ServiceDbDao;
import ru.datamart.prostore.query.execution.core.base.service.metadata.MetadataExecutor;
import ru.datamart.prostore.query.execution.core.calcite.configuration.CalciteConfiguration;
import ru.datamart.prostore.query.execution.core.ddl.dto.DdlRequestContext;
import ru.datamart.prostore.query.execution.core.ddl.service.QueryResultDdlExecutor;
import ru.datamart.prostore.query.execution.core.ddl.service.impl.schema.CreateSchemaExecutor;
import ru.datamart.prostore.query.execution.core.utils.TestUtils;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.SneakyThrows;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class CreateSchemaExecutorTest {

    private final MetadataExecutor metadataExecutor = mock(MetadataExecutor.class);
    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final ServiceDbDao serviceDbDao = mock(ServiceDbDao.class);
    private final ChangelogDao changelogDao = mock(ChangelogDao.class);
    private final DatamartDao datamartDao = mock(DatamartDao.class);
    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final SqlParser.Config parserConfig = calciteConfiguration.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory());
    private QueryResultDdlExecutor createSchemaDdlExecutor;
    private DdlRequestContext context;
    private final String schema = "shares";

    @BeforeEach
    void setUp() {
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        when(serviceDbDao.getDatamartDao()).thenReturn(datamartDao);
        createSchemaDdlExecutor = new CreateSchemaExecutor(metadataExecutor, serviceDbFacade, TestUtils.SQL_DIALECT);

        prepareContext("create database shares");
    }

    @SneakyThrows
    private void prepareContext(String sql) {
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);

        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(schema);

        queryRequest.setSql(sql);
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);
    }

    @Test
    void executeSuccess() {
        Promise<QueryResult> promise = Promise.promise();

        when(datamartDao.existsDatamart(schema))
                .thenReturn(Future.succeededFuture(false));
        when(metadataExecutor.execute(context))
                .thenReturn(Future.succeededFuture());
        when(datamartDao.createDatamart(schema))
                .thenReturn(Future.succeededFuture());

        createSchemaDdlExecutor.execute(context, null)
                .onComplete(promise);

        assertNotNull(promise.future().result());

        verify(datamartDao).existsDatamart(schema);
        verify(datamartDao).createDatamart(schema);
        verify(metadataExecutor).execute(context);
    }

    @Test
    void executeSuccessLogicalOnly() {
        prepareContext("create database shares logical_only");

        Promise<QueryResult> promise = Promise.promise();

        when(datamartDao.existsDatamart(schema))
                .thenReturn(Future.succeededFuture(false));
        when(metadataExecutor.execute(context))
                .thenReturn(Future.succeededFuture());
        when(datamartDao.createDatamart(schema))
                .thenReturn(Future.succeededFuture());

        createSchemaDdlExecutor.execute(context, null)
                .onComplete(promise);

        assertNotNull(promise.future().result());

        verify(datamartDao).existsDatamart(schema);
        verify(datamartDao).createDatamart(schema);
        verify(metadataExecutor, never()).execute(context);
    }

    @Test
    void executeWithExistDatamart() {
        Promise<QueryResult> promise = Promise.promise();
        when(datamartDao.existsDatamart(schema))
                .thenReturn(Future.succeededFuture(true));
        createSchemaDdlExecutor.execute(context, null)
                .onComplete(promise);

        assertTrue(promise.future().failed());
    }

    @Test
    void executeWithCheckExistsDatamartError() {
        Promise<QueryResult> promise = Promise.promise();
        when(datamartDao.existsDatamart(schema))
                .thenReturn(Future.failedFuture(new DtmException("exists error")));
        createSchemaDdlExecutor.execute(context, null)
                .onComplete(promise);

        assertTrue(promise.future().failed());
    }

    @Test
    void executeWithMetadataExecError() {
        Promise<QueryResult> promise = Promise.promise();
        when(datamartDao.existsDatamart(schema))
                .thenReturn(Future.succeededFuture(false));

        when(metadataExecutor.execute(any()))
                .thenReturn(Future.failedFuture(new DtmException("")));

        createSchemaDdlExecutor.execute(context, null)
                .onComplete(promise);

        assertTrue(promise.future().failed());
    }

    @Test
    void executeWithInsertDatamartError() {
        Promise<QueryResult> promise = Promise.promise();
        when(datamartDao.existsDatamart(schema))
                .thenReturn(Future.succeededFuture(false));

        when(metadataExecutor.execute(any()))
                .thenReturn(Future.succeededFuture());

        when(datamartDao.createDatamart(schema))
                .thenReturn(Future.failedFuture(new DtmException("create error")));

        createSchemaDdlExecutor.execute(context, null)
                .onComplete(promise);

        assertNotNull(promise.future().cause());
    }

    @Test
    void shouldFailWhenNotValidName() {
        Promise<QueryResult> promise = Promise.promise();

        prepareContext("create database имя");

        when(datamartDao.existsDatamart(schema))
                .thenReturn(Future.succeededFuture(false));
        when(metadataExecutor.execute(context))
                .thenReturn(Future.succeededFuture());
        when(datamartDao.createDatamart(schema))
                .thenReturn(Future.succeededFuture());

        createSchemaDdlExecutor.execute(context, null)
                .onComplete(promise);

        if (promise.future().succeeded()) {
            fail("Unexpected success");
        }

        assertEquals("Identifier [имя] is not valid, allowed pattern is [a-zA-Z][a-zA-Z0-9_]*", promise.future().cause().getMessage());

        verifyNoInteractions(datamartDao);
    }
}
