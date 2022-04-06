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
package ru.datamart.prostore.query.execution.plugin.adb.synchronize.executors.impl;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.datamart.prostore.common.delta.DeltaData;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.calcite.core.rel2sql.DtmRelToSqlConverter;
import ru.datamart.prostore.query.calcite.core.service.DefinitionService;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import ru.datamart.prostore.query.execution.plugin.adb.base.factory.adg.AdgConnectorSqlFactory;
import ru.datamart.prostore.query.execution.plugin.adb.base.service.castservice.AdgColumnsCastService;
import ru.datamart.prostore.query.execution.plugin.adb.base.service.castservice.ColumnsCastService;
import ru.datamart.prostore.query.execution.plugin.adb.calcite.configuration.CalciteConfiguration;
import ru.datamart.prostore.query.execution.plugin.adb.calcite.factory.AdbCalciteSchemaFactory;
import ru.datamart.prostore.query.execution.plugin.adb.calcite.factory.AdbSchemaFactory;
import ru.datamart.prostore.query.execution.plugin.adb.calcite.service.AdbCalciteContextProvider;
import ru.datamart.prostore.query.execution.plugin.adb.calcite.service.AdbCalciteDMLQueryParserService;
import ru.datamart.prostore.query.execution.plugin.adb.calcite.service.AdbCalciteDefinitionService;
import ru.datamart.prostore.query.execution.plugin.adb.enrichment.service.AdbDmlQueryExtendWithoutHistoryService;
import ru.datamart.prostore.query.execution.plugin.adb.enrichment.service.AdbQueryEnrichmentService;
import ru.datamart.prostore.query.execution.plugin.adb.enrichment.service.AdbSchemaExtender;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.adb.synchronize.service.PrepareQueriesOfChangesService;
import ru.datamart.prostore.query.execution.plugin.adb.synchronize.service.impl.AdgPrepareQueriesOfChangesService;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.QueryExtendService;
import ru.datamart.prostore.query.execution.plugin.api.service.shared.adg.AdgSharedService;
import ru.datamart.prostore.query.execution.plugin.api.shared.adg.AdgSharedProperties;
import ru.datamart.prostore.query.execution.plugin.api.synchronize.SynchronizeRequest;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

@ExtendWith({VertxExtension.class, MockitoExtension.class})
class AdgSynchronizeDestinationExecutorComplexTest {
    private static final String ENV = "env";
    private static final String DATAMART = "datamart1";
    private static final Long DELTA_NUM = 1L;
    private static final Long DELTA_NUM_CN_TO = 2L;
    private static final Long DELTA_NUM_CN_FROM = 0L;
    private static final Long PREVIOUS_DELTA_NUM_CN_TO = -1L;

    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final QueryExtendService queryExtender = new AdbDmlQueryExtendWithoutHistoryService();
    private final AdbCalciteContextProvider contextProvider = new AdbCalciteContextProvider(
            calciteConfiguration.configDdlParser(
                    calciteConfiguration.ddlParserImplFactory()
            ),
            new AdbCalciteSchemaFactory(new AdbSchemaFactory()));
    private final SqlDialect sqlDialect = calciteConfiguration.adbSqlDialect();
    private final DefinitionService<SqlNode> definitionService = new AdbCalciteDefinitionService(calciteConfiguration.configDdlParser(calciteConfiguration.ddlParserImplFactory()));
    private final DtmRelToSqlConverter relToSqlConverter = new DtmRelToSqlConverter(sqlDialect);
    private final ColumnsCastService adgColumnsCastService = new AdgColumnsCastService(sqlDialect);

    @Mock
    private DatabaseExecutor databaseExecutor;

    @Mock
    private AdgSharedService adgSharedService;

    private AdbCalciteDMLQueryParserService parserService;
    private AdbQueryEnrichmentService queryEnrichmentService;
    private PrepareQueriesOfChangesService prepareQueriesOfChangesService;
    private AdgConnectorSqlFactory synchronizeSqlFactory;
    private AdgSynchronizeDestinationExecutor adgSynchronizeDestinationExecutor;

    @Captor
    private ArgumentCaptor<String> stringArgumentCaptor;

    @BeforeEach
    void setUp(Vertx vertx) {
        val schemaExtender = new AdbSchemaExtender();
        parserService = new AdbCalciteDMLQueryParserService(contextProvider, vertx, schemaExtender);
        queryEnrichmentService = new AdbQueryEnrichmentService(queryExtender, sqlDialect, relToSqlConverter);
        prepareQueriesOfChangesService = new AdgPrepareQueriesOfChangesService(parserService, adgColumnsCastService, queryEnrichmentService);
        synchronizeSqlFactory = new AdgConnectorSqlFactory(adgSharedService);
        adgSynchronizeDestinationExecutor = new AdgSynchronizeDestinationExecutor(prepareQueriesOfChangesService, databaseExecutor, synchronizeSqlFactory, adgSharedService);

        lenient().when(databaseExecutor.execute(anyString())).thenReturn(Future.succeededFuture(Collections.emptyList()));
        lenient().when(adgSharedService.prepareStaging(any())).thenReturn(Future.succeededFuture());
        lenient().when(adgSharedService.transferData(any())).thenReturn(Future.succeededFuture());
        lenient().when(adgSharedService.getSharedProperties()).thenReturn(new AdgSharedProperties("tarantool:1234", "user", "pass", 3001L, 3002L, 3003L, 3004));
    }

    @Test
    void shouldCorrectlySynchronizeWhenOneTable(VertxTestContext testContext) {
        // arrange
        UUID uuid = UUID.randomUUID();

        String viewQuery = "SELECT * from datamart1.tbl1";
        Datamart datamart = prepareDatamart(viewQuery);
        Entity matView = datamart.getEntities().get(0);
        List<Datamart> datamarts = Arrays.asList(datamart);
        SqlNode sqlNode = definitionService.processingQuery(viewQuery);

        SynchronizeRequest synchronizeRequest = new SynchronizeRequest(uuid, ENV, DATAMART, datamarts, matView, sqlNode, new DeltaData(DELTA_NUM, DELTA_NUM_CN_FROM, DELTA_NUM_CN_TO), PREVIOUS_DELTA_NUM_CN_TO);

        // act
        Future<Long> result = adgSynchronizeDestinationExecutor.execute(synchronizeRequest);

        // assert
        result.onComplete(ar -> {
            if (ar.failed()) {
                testContext.failNow(ar.cause());
                return;
            }

            testContext.verify(() -> {
                verify(databaseExecutor, times(5)).execute(stringArgumentCaptor.capture());
                List<String> allInvocations = stringArgumentCaptor.getAllValues();
                assertThat(allInvocations.get(0))
                        .isEqualToIgnoringNewLines("DROP EXTERNAL TABLE IF EXISTS datamart1.TARANTOOL_EXT_matview");
                assertThat(allInvocations.get(1))
                        .isEqualToIgnoringNewLines("CREATE WRITABLE EXTERNAL TABLE datamart1.TARANTOOL_EXT_matview\n" +
                                "(id int8,col_varchar varchar,col_char varchar,col_bigint int8,col_int int8,col_int32 int4,col_double float8,col_float float4,col_date int8,col_time int8,col_timestamp int8,col_boolean bool,col_uuid varchar,col_link varchar,sys_op int8,bucket_id int8) LOCATION ('pxf://env__datamart1__matview_staging?PROFILE=tarantool-upsert&TARANTOOL_SERVER=tarantool:1234&USER=user&PASSWORD=pass&TIMEOUT_CONNECT=3001&TIMEOUT_READ=3002&TIMEOUT_REQUEST=3003&BUFFER_SIZE=3004')\n" +
                                "FORMAT 'CUSTOM' (FORMATTER = 'pxfwritable_export')");
                assertThat(allInvocations.get(2))
                        .isEqualToIgnoringNewLines("INSERT INTO datamart1.TARANTOOL_EXT_matview (id, sys_op) SELECT *, 1 FROM (SELECT id FROM datamart1.tbl1_actual WHERE COALESCE(sys_to, 9223372036854775807) >= -1 AND (COALESCE(sys_to, 9223372036854775807) <= 1 AND sys_op = 1)) as __temp_tbl");
                assertThat(allInvocations.get(3))
                        .isEqualToIgnoringNewLines("INSERT INTO datamart1.TARANTOOL_EXT_matview SELECT *, 0 FROM (SELECT id, col_varchar, col_char, col_bigint, col_int, col_int32, col_double, col_float, CAST(EXTRACT(EPOCH FROM col_date) / 86400 AS BIGINT) AS EXPR__8, CAST(EXTRACT(EPOCH FROM col_time) * 1000000 AS BIGINT) AS EXPR__9, CAST(EXTRACT(EPOCH FROM col_timestamp) * 1000000 AS BIGINT) AS EXPR__10, col_boolean, col_uuid, col_link FROM datamart1.tbl1_actual WHERE sys_from >= 0 AND sys_from <= 2) as __temp_tbl");
                assertThat(allInvocations.get(4))
                        .isEqualToIgnoringNewLines("DROP EXTERNAL TABLE IF EXISTS datamart1.TARANTOOL_EXT_matview");
                verifyNoMoreInteractions(databaseExecutor);
                assertEquals(DELTA_NUM, ar.result());
            }).completeNow();
        });
    }

    @Test
    void shouldCorrectlySynchronizeWhenMultipleTables(VertxTestContext testContext) {
        // arrange
        UUID uuid = UUID.randomUUID();
        String viewQuery = "SELECT tbl1.id, tbl1.col_varchar, tbl1.col_char, tbl1.col_bigint, tbl1.col_int, tbl2.col_int32, tbl2.col_double, tbl2.col_float, tbl2.col_date, tbl2.col_time, tbl2.col_timestamp, tbl2.col_boolean, tbl2.col_uuid, tbl2.col_link from datamart1.tbl1 join datamart1.tbl2 on tbl1.col_bigint = tbl2.col_bigint";
        SqlNode sqlNode = definitionService.processingQuery(viewQuery);
        Datamart datamart = prepareDatamart(viewQuery);
        Entity matView = datamart.getEntities().get(0);
        List<Datamart> datamarts = Arrays.asList(datamart);

        SynchronizeRequest synchronizeRequest = new SynchronizeRequest(uuid, ENV, DATAMART, datamarts, matView, sqlNode, new DeltaData(DELTA_NUM, DELTA_NUM_CN_FROM, DELTA_NUM_CN_TO), PREVIOUS_DELTA_NUM_CN_TO);

        // act
        Future<Long> result = adgSynchronizeDestinationExecutor.execute(synchronizeRequest);

        // assert
        result.onComplete(ar -> {
            if (ar.failed()) {
                testContext.failNow(ar.cause());
                return;
            }

            testContext.verify(() -> {
                verify(databaseExecutor, times(5)).execute(stringArgumentCaptor.capture());
                List<String> allInvocations = stringArgumentCaptor.getAllValues();
                assertThat(allInvocations.get(0))
                        .isEqualToIgnoringNewLines("DROP EXTERNAL TABLE IF EXISTS datamart1.TARANTOOL_EXT_matview");
                assertThat(allInvocations.get(1))
                        .isEqualToIgnoringNewLines("CREATE WRITABLE EXTERNAL TABLE datamart1.TARANTOOL_EXT_matview\n" +
                                "(id int8,col_varchar varchar,col_char varchar,col_bigint int8,col_int int8,col_int32 int4,col_double float8,col_float float4,col_date int8,col_time int8,col_timestamp int8,col_boolean bool,col_uuid varchar,col_link varchar,sys_op int8,bucket_id int8) LOCATION ('pxf://env__datamart1__matview_staging?PROFILE=tarantool-upsert&TARANTOOL_SERVER=tarantool:1234&USER=user&PASSWORD=pass&TIMEOUT_CONNECT=3001&TIMEOUT_READ=3002&TIMEOUT_REQUEST=3003&BUFFER_SIZE=3004')\n" +
                                "FORMAT 'CUSTOM' (FORMATTER = 'pxfwritable_export')");
                assertThat(allInvocations.get(2))
                        .isEqualToIgnoringNewLines("INSERT INTO datamart1.TARANTOOL_EXT_matview (id, sys_op) SELECT *, 1 FROM (SELECT t0.id FROM (SELECT id, col_varchar, col_char, col_bigint, col_int, col_int32, col_double, col_float, col_date, col_time, col_timestamp, col_boolean, col_uuid, col_link FROM datamart1.tbl1_actual WHERE sys_from <= -1 AND COALESCE(sys_to, 9223372036854775807) >= -1) AS t0 INNER JOIN (SELECT id, col_varchar, col_char, col_bigint, col_int, col_int32, col_double, col_float, col_date, col_time, col_timestamp, col_boolean, col_uuid, col_link FROM datamart1.tbl2_actual WHERE sys_from <= -1 AND COALESCE(sys_to, 9223372036854775807) >= -1) AS t2 ON t0.col_bigint = t2.col_bigint EXCEPT SELECT t0.id FROM (SELECT id, col_varchar, col_char, col_bigint, col_int, col_int32, col_double, col_float, col_date, col_time, col_timestamp, col_boolean, col_uuid, col_link FROM datamart1.tbl1_actual WHERE sys_from <= 2 AND COALESCE(sys_to, 9223372036854775807) >= 2) AS t0 INNER JOIN (SELECT id, col_varchar, col_char, col_bigint, col_int, col_int32, col_double, col_float, col_date, col_time, col_timestamp, col_boolean, col_uuid, col_link FROM datamart1.tbl2_actual WHERE sys_from <= 2 AND COALESCE(sys_to, 9223372036854775807) >= 2) AS t2 ON t0.col_bigint = t2.col_bigint) as __temp_tbl");
                assertThat(allInvocations.get(3))
                        .isEqualToIgnoringNewLines("INSERT INTO datamart1.TARANTOOL_EXT_matview SELECT *, 0 FROM (SELECT t0.id, t0.col_varchar, t0.col_char, t0.col_bigint, t0.col_int, t2.col_int32, t2.col_double, t2.col_float, CAST(EXTRACT(EPOCH FROM t2.col_date) / 86400 AS BIGINT) AS EXPR__8, CAST(EXTRACT(EPOCH FROM t2.col_time) * 1000000 AS BIGINT) AS EXPR__9, CAST(EXTRACT(EPOCH FROM t2.col_timestamp) * 1000000 AS BIGINT) AS EXPR__10, t2.col_boolean, t2.col_uuid, t2.col_link FROM (SELECT id, col_varchar, col_char, col_bigint, col_int, col_int32, col_double, col_float, col_date, col_time, col_timestamp, col_boolean, col_uuid, col_link FROM datamart1.tbl1_actual WHERE sys_from <= 2 AND COALESCE(sys_to, 9223372036854775807) >= 2) AS t0 INNER JOIN (SELECT id, col_varchar, col_char, col_bigint, col_int, col_int32, col_double, col_float, col_date, col_time, col_timestamp, col_boolean, col_uuid, col_link FROM datamart1.tbl2_actual WHERE sys_from <= 2 AND COALESCE(sys_to, 9223372036854775807) >= 2) AS t2 ON t0.col_bigint = t2.col_bigint EXCEPT SELECT t0.id, t0.col_varchar, t0.col_char, t0.col_bigint, t0.col_int, t2.col_int32, t2.col_double, t2.col_float, CAST(EXTRACT(EPOCH FROM t2.col_date) / 86400 AS BIGINT) AS EXPR__8, CAST(EXTRACT(EPOCH FROM t2.col_time) * 1000000 AS BIGINT) AS EXPR__9, CAST(EXTRACT(EPOCH FROM t2.col_timestamp) * 1000000 AS BIGINT) AS EXPR__10, t2.col_boolean, t2.col_uuid, t2.col_link FROM (SELECT id, col_varchar, col_char, col_bigint, col_int, col_int32, col_double, col_float, col_date, col_time, col_timestamp, col_boolean, col_uuid, col_link FROM datamart1.tbl1_actual WHERE sys_from <= -1 AND COALESCE(sys_to, 9223372036854775807) >= -1) AS t0 INNER JOIN (SELECT id, col_varchar, col_char, col_bigint, col_int, col_int32, col_double, col_float, col_date, col_time, col_timestamp, col_boolean, col_uuid, col_link FROM datamart1.tbl2_actual WHERE sys_from <= -1 AND COALESCE(sys_to, 9223372036854775807) >= -1) AS t2 ON t0.col_bigint = t2.col_bigint) as __temp_tbl");
                assertThat(allInvocations.get(4))
                        .isEqualToIgnoringNewLines("DROP EXTERNAL TABLE IF EXISTS datamart1.TARANTOOL_EXT_matview");
                verifyNoMoreInteractions(databaseExecutor);
                assertEquals(DELTA_NUM, ar.result());
            }).completeNow();
        });
    }

    @Test
    void shouldCorrectlyUseExceptWhenGroupByAggregate(VertxTestContext testContext) {
        // arrange
        UUID uuid = UUID.randomUUID();
        String viewQuery = "SELECT 1, SUM(id) FROM tbl1 GROUP BY (id)";
        SqlNode sqlNode = definitionService.processingQuery(viewQuery);
        Datamart datamart = prepareDatamart(viewQuery);
        Entity matView = datamart.getEntities().get(0);
        matView.setFields(Arrays.asList(
                EntityField.builder()
                        .name("id")
                        .primaryOrder(1)
                        .ordinalPosition(0)
                        .type(ColumnType.BIGINT)
                        .nullable(false)
                        .build(),
                EntityField.builder()
                        .name("sum")
                        .ordinalPosition(1)
                        .type(ColumnType.BIGINT)
                        .nullable(true)
                        .build()
        ));

        List<Datamart> datamarts = Arrays.asList(datamart);

        SynchronizeRequest synchronizeRequest = new SynchronizeRequest(uuid, ENV, DATAMART, datamarts, matView, sqlNode, new DeltaData(DELTA_NUM, DELTA_NUM_CN_FROM, DELTA_NUM_CN_TO), PREVIOUS_DELTA_NUM_CN_TO);

        // act
        Future<Long> result = adgSynchronizeDestinationExecutor.execute(synchronizeRequest);

        // assert
        result.onComplete(ar -> {
            if (ar.failed()) {
                testContext.failNow(ar.cause());
                return;
            }

            testContext.verify(() -> {
                verify(databaseExecutor, times(5)).execute(stringArgumentCaptor.capture());
                List<String> allInvocations = stringArgumentCaptor.getAllValues();
                assertThat(allInvocations.get(0))
                        .isEqualToIgnoringNewLines("DROP EXTERNAL TABLE IF EXISTS datamart1.TARANTOOL_EXT_matview");
                assertThat(allInvocations.get(1))
                        .isEqualToIgnoringNewLines("CREATE WRITABLE EXTERNAL TABLE datamart1.TARANTOOL_EXT_matview\n" +
                                "(id int8,sum int8,sys_op int8,bucket_id int8) LOCATION ('pxf://env__datamart1__matview_staging?PROFILE=tarantool-upsert&TARANTOOL_SERVER=tarantool:1234&USER=user&PASSWORD=pass&TIMEOUT_CONNECT=3001&TIMEOUT_READ=3002&TIMEOUT_REQUEST=3003&BUFFER_SIZE=3004')\n" +
                                "FORMAT 'CUSTOM' (FORMATTER = 'pxfwritable_export')");
                assertThat(allInvocations.get(2))
                        .isEqualToIgnoringNewLines("INSERT INTO datamart1.TARANTOOL_EXT_matview (id, sys_op) SELECT *, 1 FROM (SELECT 1 AS EXPR__0 FROM datamart1.tbl1_actual WHERE sys_from <= -1 AND COALESCE(sys_to, 9223372036854775807) >= -1 GROUP BY id EXCEPT SELECT 1 AS EXPR__0 FROM datamart1.tbl1_actual WHERE sys_from <= 2 AND COALESCE(sys_to, 9223372036854775807) >= 2 GROUP BY id) as __temp_tbl");
                assertThat(allInvocations.get(3))
                        .isEqualToIgnoringNewLines("INSERT INTO datamart1.TARANTOOL_EXT_matview SELECT *, 0 FROM (SELECT 1 AS EXPR__0, SUM(id) AS EXPR__1 FROM datamart1.tbl1_actual WHERE sys_from <= 2 AND COALESCE(sys_to, 9223372036854775807) >= 2 GROUP BY id EXCEPT SELECT 1 AS EXPR__0, SUM(id) AS EXPR__1 FROM datamart1.tbl1_actual WHERE sys_from <= -1 AND COALESCE(sys_to, 9223372036854775807) >= -1 GROUP BY id) as __temp_tbl");
                assertThat(allInvocations.get(4))
                        .isEqualToIgnoringNewLines("DROP EXTERNAL TABLE IF EXISTS datamart1.TARANTOOL_EXT_matview");
                verifyNoMoreInteractions(databaseExecutor);
                assertEquals(DELTA_NUM, ar.result());
            }).completeNow();
        });
    }

    @Test
    void shouldCorrectlyUseExceptWhenCountAggregate(VertxTestContext testContext) {
        // arrange
        UUID uuid = UUID.randomUUID();
        String viewQuery = "SELECT 1, COUNT(*) as t FROM tbl1";
        SqlNode sqlNode = definitionService.processingQuery(viewQuery);
        Datamart datamart = prepareDatamart(viewQuery);
        Entity matView = datamart.getEntities().get(0);
        matView.setFields(Arrays.asList(
                EntityField.builder()
                        .name("id")
                        .primaryOrder(1)
                        .ordinalPosition(0)
                        .type(ColumnType.BIGINT)
                        .nullable(false)
                        .build(),
                EntityField.builder()
                        .name("sum")
                        .ordinalPosition(1)
                        .type(ColumnType.BIGINT)
                        .nullable(true)
                        .build()
        ));

        List<Datamart> datamarts = Arrays.asList(datamart);

        SynchronizeRequest synchronizeRequest = new SynchronizeRequest(uuid, ENV, DATAMART, datamarts, matView, sqlNode, new DeltaData(DELTA_NUM, DELTA_NUM_CN_FROM, DELTA_NUM_CN_TO), PREVIOUS_DELTA_NUM_CN_TO);

        // act
        Future<Long> result = adgSynchronizeDestinationExecutor.execute(synchronizeRequest);

        // assert
        result.onComplete(ar -> {
            if (ar.failed()) {
                testContext.failNow(ar.cause());
                return;
            }

            testContext.verify(() -> {
                verify(databaseExecutor, times(5)).execute(stringArgumentCaptor.capture());
                List<String> allInvocations = stringArgumentCaptor.getAllValues();
                assertThat(allInvocations.get(0))
                        .isEqualToIgnoringNewLines("DROP EXTERNAL TABLE IF EXISTS datamart1.TARANTOOL_EXT_matview");
                assertThat(allInvocations.get(1))
                        .isEqualToIgnoringNewLines("CREATE WRITABLE EXTERNAL TABLE datamart1.TARANTOOL_EXT_matview\n" +
                                "(id int8,sum int8,sys_op int8,bucket_id int8) LOCATION ('pxf://env__datamart1__matview_staging?PROFILE=tarantool-upsert&TARANTOOL_SERVER=tarantool:1234&USER=user&PASSWORD=pass&TIMEOUT_CONNECT=3001&TIMEOUT_READ=3002&TIMEOUT_REQUEST=3003&BUFFER_SIZE=3004')\n" +
                                "FORMAT 'CUSTOM' (FORMATTER = 'pxfwritable_export')");
                assertThat(allInvocations.get(2))
                        .isEqualToIgnoringNewLines("INSERT INTO datamart1.TARANTOOL_EXT_matview (id, sys_op) SELECT *, 1 FROM (SELECT 1 AS EXPR__0 FROM datamart1.tbl1_actual WHERE sys_from <= -1 AND COALESCE(sys_to, 9223372036854775807) >= -1 EXCEPT SELECT 1 AS EXPR__0 FROM datamart1.tbl1_actual WHERE sys_from <= 2 AND COALESCE(sys_to, 9223372036854775807) >= 2) as __temp_tbl");
                assertThat(allInvocations.get(3))
                        .isEqualToIgnoringNewLines("INSERT INTO datamart1.TARANTOOL_EXT_matview SELECT *, 0 FROM (SELECT 1 AS EXPR__0, COUNT(*) AS t FROM datamart1.tbl1_actual WHERE sys_from <= 2 AND COALESCE(sys_to, 9223372036854775807) >= 2 EXCEPT SELECT 1 AS EXPR__0, COUNT(*) AS t FROM datamart1.tbl1_actual WHERE sys_from <= -1 AND COALESCE(sys_to, 9223372036854775807) >= -1) as __temp_tbl");
                assertThat(allInvocations.get(4))
                        .isEqualToIgnoringNewLines("DROP EXTERNAL TABLE IF EXISTS datamart1.TARANTOOL_EXT_matview");
                verifyNoMoreInteractions(databaseExecutor);
                assertEquals(DELTA_NUM, ar.result());
            }).completeNow();
        });
    }

    private Datamart prepareDatamart(String viewQuery) {
        List<EntityField> fields = new ArrayList<>();

        fields.add(EntityField.builder()
                .ordinalPosition(0)
                .primaryOrder(1)
                .name("id")
                .type(ColumnType.BIGINT)
                .nullable(true)
                .build());

        int pos = 1;
        for (ColumnType columnType : ColumnType.values()) {
            if (columnType == ColumnType.ANY || columnType == ColumnType.BLOB) continue;

            EntityField field = EntityField.builder()
                    .ordinalPosition(pos++)
                    .name("col_" + columnType.name().toLowerCase())
                    .type(columnType)
                    .nullable(true)
                    .build();

            switch (columnType) {
                case TIME:
                case TIMESTAMP:
                    field.setAccuracy(6);
                    break;
                case CHAR:
                case VARCHAR:
                    field.setSize(100);
                    break;
                case UUID:
                    field.setSize(36);
                    break;
            }

            fields.add(field);
        }

        Entity tbl1 = Entity.builder()
                .schema(DATAMART)
                .name("tbl1")
                .fields(fields)
                .entityType(EntityType.TABLE)
                .destination(EnumSet.of(SourceType.ADB))
                .build();

        Entity tbl2 = Entity.builder()
                .schema(DATAMART)
                .name("tbl2")
                .fields(fields)
                .entityType(EntityType.TABLE)
                .destination(EnumSet.of(SourceType.ADB))
                .build();
        Entity matView = Entity.builder()
                .schema(DATAMART)
                .name("matview")
                .fields(fields)
                .viewQuery(viewQuery)
                .entityType(EntityType.MATERIALIZED_VIEW)
                .destination(EnumSet.of(SourceType.ADG))
                .materializedDeltaNum(null)
                .materializedDataSource(SourceType.ADB)
                .build();

        return Datamart.builder()
                .isDefault(true)
                .mnemonic(DATAMART)
                .entities(Arrays.asList(
                        matView, tbl1, tbl2
                ))
                .build();
    }
}