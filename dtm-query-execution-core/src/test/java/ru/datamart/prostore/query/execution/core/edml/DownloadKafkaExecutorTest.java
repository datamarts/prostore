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
package ru.datamart.prostore.query.execution.core.edml;

import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.calcite.core.configuration.CalciteCoreConfiguration;
import ru.datamart.prostore.query.calcite.core.framework.DtmCalciteFramework;
import ru.datamart.prostore.query.calcite.core.service.QueryParserService;
import ru.datamart.prostore.query.execution.core.base.service.column.CheckColumnTypesService;
import ru.datamart.prostore.query.execution.core.calcite.configuration.CalciteConfiguration;
import ru.datamart.prostore.query.execution.core.dml.dto.PluginDeterminationRequest;
import ru.datamart.prostore.query.execution.core.dml.service.ColumnMetadataService;
import ru.datamart.prostore.query.execution.core.dml.service.PluginDeterminationService;
import ru.datamart.prostore.query.execution.core.edml.configuration.EdmlProperties;
import ru.datamart.prostore.query.execution.core.edml.dto.EdmlRequestContext;
import ru.datamart.prostore.query.execution.core.edml.mppr.factory.MpprKafkaRequestFactory;
import ru.datamart.prostore.query.execution.core.edml.mppr.service.impl.DownloadKafkaExecutor;
import ru.datamart.prostore.query.execution.core.plugin.service.DataSourcePluginService;
import ru.datamart.prostore.query.execution.core.query.exception.QueriedEntityIsMissingException;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.List;

import static ru.datamart.prostore.query.execution.core.utils.TestUtils.SQL_DIALECT;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
class DownloadKafkaExecutorTest {

    public static final String DATAMART = "dtm";

    private final DataSourcePluginService pluginService = mock(DataSourcePluginService.class);
    private final MpprKafkaRequestFactory mpprKafkaRequestFactory = mock(MpprKafkaRequestFactory.class);
    private final CheckColumnTypesService checkColumnTypesService = mock(CheckColumnTypesService.class);
    private final ColumnMetadataService metadataService = mock(ColumnMetadataService.class);
    private final QueryParserService queryParserService = mock(QueryParserService.class);
    private final PluginDeterminationService pluginDeterminationService = mock(PluginDeterminationService.class);

    private final ArgumentCaptor<PluginDeterminationRequest> determinationRequestCaptor = ArgumentCaptor.forClass(PluginDeterminationRequest.class);

    private CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private SqlParser.Config parserConfig = calciteConfiguration.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory());
    private DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
    private FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();

    private Planner planner;
    private DownloadKafkaExecutor downloadKafkaExecutor;
    private EdmlRequestContext context;

    private Entity entity = Entity.builder()
            .schema(DATAMART)
            .destination(Collections.singleton(SourceType.ADQM))
            .entityType(EntityType.TABLE)
            .build();
    private List<Datamart> logicalSchema = Collections.singletonList(new Datamart(DATAMART, true, Collections.singletonList(entity)));

    @BeforeEach
    void setUp() {
        downloadKafkaExecutor = new DownloadKafkaExecutor(queryParserService, checkColumnTypesService, mpprKafkaRequestFactory, metadataService, pluginService, SQL_DIALECT, pluginDeterminationService);
        planner = DtmCalciteFramework.getPlanner(frameworkConfig);
    }

    @Test
    void failMismatchQueryDatasource(VertxTestContext testContext) throws SqlParseException {
        //arrange
        val sqlNode = planner.parse("INSERT INTO dtm.accounts_ext_download SELECT * FROM dtm.accounts DATASOURCE_TYPE = 'ADB'");
        context = new EdmlRequestContext(null, null, sqlNode, "env");
        context.setLogicalSchema(logicalSchema);
        when(pluginDeterminationService.determine(determinationRequestCaptor.capture()))
                .thenReturn(Future.failedFuture(new QueriedEntityIsMissingException(SourceType.ADB)));

        //act
        downloadKafkaExecutor.execute(context)
                .onComplete(testContext.failing(error -> testContext.verify(() -> {
                    //assert
                    assertTrue(error instanceof QueriedEntityIsMissingException);
                    assertEquals("Queried entity is missing for the specified DATASOURCE_TYPE ADB", error.getMessage());
                    assertEquals(SourceType.ADB, determinationRequestCaptor.getValue().getPreferredSourceType());

                }).completeNow()));
    }

    @Test
    void failMismatchQueryDatasourceWhenLimit(VertxTestContext testContext) throws SqlParseException {
        //arrange
        val sqlNode = planner.parse("INSERT INTO dtm.accounts_ext_download SELECT * FROM dtm.accounts LIMIT 1 DATASOURCE_TYPE = 'ADB'");
        context = new EdmlRequestContext(null, null, sqlNode, "env");
        context.setLogicalSchema(logicalSchema);
        when(pluginDeterminationService.determine(determinationRequestCaptor.capture()))
                .thenReturn(Future.failedFuture(new QueriedEntityIsMissingException(SourceType.ADB)));

        //act
        downloadKafkaExecutor.execute(context)
                .onComplete(testContext.failing(error -> testContext.verify(() -> {
                    //assert
                    assertTrue(error instanceof QueriedEntityIsMissingException);
                    assertEquals("Queried entity is missing for the specified DATASOURCE_TYPE ADB", error.getMessage());
                    assertEquals(SourceType.ADB, determinationRequestCaptor.getValue().getPreferredSourceType());

                }).completeNow()));
    }

    @Test
    void failMismatchQueryDatasourceWhenGroupBy(VertxTestContext testContext) throws SqlParseException {
        //arrange
        val sqlNode = planner.parse("INSERT INTO dtm.accounts_ext_download SELECT * FROM dtm.accounts GROUP BY id DATASOURCE_TYPE = 'ADB'");
        context = new EdmlRequestContext(null, null, sqlNode, "env");
        context.setLogicalSchema(logicalSchema);
        when(pluginDeterminationService.determine(determinationRequestCaptor.capture()))
                .thenReturn(Future.failedFuture(new QueriedEntityIsMissingException(SourceType.ADB)));

        //act
        downloadKafkaExecutor.execute(context)
                .onComplete(testContext.failing(error -> testContext.verify(() -> {
                    //assert
                    assertTrue(error instanceof QueriedEntityIsMissingException);
                    assertEquals("Queried entity is missing for the specified DATASOURCE_TYPE ADB", error.getMessage());
                    assertEquals(SourceType.ADB, determinationRequestCaptor.getValue().getPreferredSourceType());

                }).completeNow()));
    }

    @Test
    void failMismatchDefaultDatasource(VertxTestContext testContext) throws SqlParseException {
        //arrange
        val sqlNode = planner.parse("INSERT INTO dtm.accounts_ext_download SELECT * FROM dtm.accounts");
        context = new EdmlRequestContext(null, null, sqlNode, "env");
        context.setLogicalSchema(logicalSchema);
        when(pluginDeterminationService.determine(determinationRequestCaptor.capture()))
                .thenReturn(Future.failedFuture(new QueriedEntityIsMissingException(SourceType.ADG)));

        //act
        downloadKafkaExecutor.execute(context)
                .onComplete(testContext.failing(error -> testContext.verify(() -> {
                    //assert
                    assertTrue(error instanceof QueriedEntityIsMissingException);
                    assertEquals("Queried entity is missing for the specified DATASOURCE_TYPE ADG", error.getMessage());
                    assertNull(determinationRequestCaptor.getValue().getPreferredSourceType());

                }).completeNow()));
    }
}
