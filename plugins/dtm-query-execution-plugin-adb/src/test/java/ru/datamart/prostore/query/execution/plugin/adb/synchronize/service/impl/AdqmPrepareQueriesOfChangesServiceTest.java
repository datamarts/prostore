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
package ru.datamart.prostore.query.execution.plugin.adb.synchronize.service.impl;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.datamart.prostore.common.calcite.CalciteContext;
import ru.datamart.prostore.common.delta.DeltaData;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.query.calcite.core.configuration.CalciteCoreConfiguration;
import ru.datamart.prostore.query.calcite.core.provider.CalciteContextProvider;
import ru.datamart.prostore.query.calcite.core.service.QueryParserService;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import ru.datamart.prostore.query.execution.plugin.adb.base.service.castservice.AdqmColumnsCastService;
import ru.datamart.prostore.query.execution.plugin.adb.base.service.castservice.ColumnsCastService;
import ru.datamart.prostore.query.execution.plugin.adb.calcite.configuration.CalciteConfiguration;
import ru.datamart.prostore.query.execution.plugin.adb.calcite.factory.AdbCalciteSchemaFactory;
import ru.datamart.prostore.query.execution.plugin.adb.calcite.factory.AdbSchemaFactory;
import ru.datamart.prostore.query.execution.plugin.adb.calcite.service.AdbCalciteContextProvider;
import ru.datamart.prostore.query.execution.plugin.adb.calcite.service.AdbCalciteDMLQueryParserService;
import ru.datamart.prostore.query.execution.plugin.adb.synchronize.service.PrepareQueriesOfChangesServiceBase;
import ru.datamart.prostore.query.execution.plugin.adb.synchronize.service.PrepareRequestOfChangesRequest;
import ru.datamart.prostore.query.execution.plugin.adb.synchronize.service.PrepareRequestOfChangesResult;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.dto.EnrichQueryRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.QueryEnrichmentService;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class AdqmPrepareQueriesOfChangesServiceTest {

    private static final Datamart DATAMART_1 = Datamart.builder()
            .mnemonic("datamart1")
            .entities(Arrays.asList(
                    Entity.builder()
                            .entityType(EntityType.TABLE)
                            .name("dates")
                            .schema("datamart1")
                            .fields(Arrays.asList(
                                    EntityField.builder()
                                            .name("id")
                                            .type(ColumnType.BIGINT)
                                            .nullable(false)
                                            .primaryOrder(1)
                                            .ordinalPosition(0)
                                            .build(),
                                    EntityField.builder()
                                            .name("col_timestamp")
                                            .type(ColumnType.TIMESTAMP)
                                            .nullable(true)
                                            .ordinalPosition(1)
                                            .build(),
                                    EntityField.builder()
                                            .name("col_time")
                                            .type(ColumnType.TIME)
                                            .nullable(true)
                                            .ordinalPosition(2)
                                            .build(),
                                    EntityField.builder()
                                            .name("col_date")
                                            .type(ColumnType.DATE)
                                            .nullable(true)
                                            .ordinalPosition(3)
                                            .build(),
                                    EntityField.builder()
                                            .name("col_boolean")
                                            .type(ColumnType.BOOLEAN)
                                            .nullable(true)
                                            .ordinalPosition(4)
                                            .build()
                            )).build(),
                    Entity.builder()
                            .entityType(EntityType.TABLE)
                            .name("surnames")
                            .schema("datamart1")
                            .fields(Arrays.asList(
                                    EntityField.builder()
                                            .name("id")
                                            .type(ColumnType.BIGINT)
                                            .nullable(false)
                                            .primaryOrder(1)
                                            .ordinalPosition(0)
                                            .build(),
                                    EntityField.builder()
                                            .name("surname")
                                            .type(ColumnType.VARCHAR)
                                            .size(100)
                                            .nullable(true)
                                            .ordinalPosition(1)
                                            .build()
                            )).build(),
                    Entity.builder()
                            .entityType(EntityType.TABLE)
                            .name("names")
                            .schema("datamart1")
                            .fields(Arrays.asList(
                                    EntityField.builder()
                                            .name("id")
                                            .type(ColumnType.BIGINT)
                                            .nullable(false)
                                            .primaryOrder(1)
                                            .ordinalPosition(0)
                                            .build(),
                                    EntityField.builder()
                                            .name("name")
                                            .type(ColumnType.VARCHAR)
                                            .size(100)
                                            .nullable(true)
                                            .ordinalPosition(1)
                                            .build()
                            )).build()
            ))
            .isDefault(true)
            .build();

    private static final List<Datamart> DATAMART_LIST = Collections.singletonList(DATAMART_1);
    private static final long DELTA_NUM = 0L;
    private static final long DELTA_NUM_CN_TO = 3L;
    private static final long DELTA_NUM_CN_FROM = 0L;
    private static final long PREVIOUS_DELTA_NUM_CN_TO = -1L;

    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final SqlParser.Config parserConfig = calciteConfiguration.configDdlParser(calciteCoreConfiguration.eddlParserImplFactory());
    private final AdbCalciteSchemaFactory coreSchemaFactory = new AdbCalciteSchemaFactory(new AdbSchemaFactory());
    private final CalciteContextProvider contextProvider = new AdbCalciteContextProvider(parserConfig, coreSchemaFactory);
    private final SqlDialect sqlDialect = calciteConfiguration.adbSqlDialect();
    private final ColumnsCastService adqmColumnsCastService = new AdqmColumnsCastService(sqlDialect);

    @Mock
    private QueryEnrichmentService queryEnrichmentService;

    @Captor
    private ArgumentCaptor<EnrichQueryRequest> enrichQueryRequestArgumentCaptor;

    private QueryParserService queryParserService;

    private PrepareQueriesOfChangesServiceBase queriesOfChangesService;

    @BeforeEach
    void setUp(Vertx vertx) {
        lenient().when(queryEnrichmentService.enrich(any(), any())).thenAnswer(invocationOnMock -> {
            EnrichQueryRequest argument = invocationOnMock.getArgument(0);
            return Future.succeededFuture(argument.getQuery().toSqlString(sqlDialect).toString());
        });
        queryParserService = new AdbCalciteDMLQueryParserService(contextProvider, vertx);
        queriesOfChangesService = new AdqmPrepareQueriesOfChangesService(queryParserService, adqmColumnsCastService, queryEnrichmentService);
    }

    @Test
    void shouldSuccessWhenOneTable(VertxTestContext ctx) {
        // arrange
        Entity matView = Entity.builder()
                .name("matview")
                .fields(Arrays.asList(
                        EntityField.builder()
                                .name("col_time")
                                .ordinalPosition(3)
                                .type(ColumnType.TIME)
                                .nullable(true)
                                .build(),
                        EntityField.builder()
                                .name("col_timestamp")
                                .ordinalPosition(1)
                                .type(ColumnType.TIMESTAMP)
                                .nullable(true)
                                .build(),
                        EntityField.builder()
                                .name("col_date")
                                .ordinalPosition(2)
                                .type(ColumnType.DATE)
                                .nullable(true)
                                .build(),
                        EntityField.builder()
                                .name("col_boolean")
                                .ordinalPosition(4)
                                .type(ColumnType.BOOLEAN)
                                .nullable(true)
                                .build(),
                        EntityField.builder()
                                .name("id")
                                .ordinalPosition(0)
                                .primaryOrder(1)
                                .type(ColumnType.BIGINT)
                                .nullable(false)
                                .build()
                ))
                .build();

        SqlNode query = parseWithValidate("SELECT id, col_timestamp, col_date, col_time, col_boolean FROM datamart1.dates", DATAMART_LIST);

        // act
        Future<PrepareRequestOfChangesResult> result = queriesOfChangesService.prepare(new PrepareRequestOfChangesRequest(DATAMART_LIST, "dev", new DeltaData(DELTA_NUM, DELTA_NUM_CN_FROM, DELTA_NUM_CN_TO), PREVIOUS_DELTA_NUM_CN_TO, query, matView));

        // assert
        String expectedNewQuery = "SELECT dates.id, CAST(EXTRACT(EPOCH FROM dates.col_timestamp) * 1000000 AS BIGINT), CAST(EXTRACT(EPOCH FROM dates.col_date) / 86400 AS BIGINT), CAST(EXTRACT(EPOCH FROM dates.col_time) * 1000000 AS BIGINT), CAST(dates.col_boolean AS INTEGER)\n" +
                "FROM datamart1.dates AS dates";
        String expectedDeletedQuery = "SELECT dates.id\n" +
                "FROM datamart1.dates AS dates";

        result.onComplete(event -> {
            if (event.failed()) {
                ctx.failNow(new AssertionError("Unexpected failure", event.cause()));
                return;
            }

            PrepareRequestOfChangesResult queriesOfChanges = event.result();

            ctx.verify(() -> {
                verify(queryEnrichmentService, times(2)).enrich(enrichQueryRequestArgumentCaptor.capture(), any());
                verifyNoMoreInteractions(queryEnrichmentService);

                List<String> allValues = enrichQueryRequestArgumentCaptor.getAllValues().stream()
                        .map(enrichQueryRequest -> enrichQueryRequest.getQuery().toSqlString(sqlDialect).toString().replace("\r\n", "\n"))
                        .collect(Collectors.toList());

                assertThat(allValues).contains(expectedDeletedQuery);
                assertThat(allValues).contains(expectedNewQuery);

                // assert result
                assertThat(expectedNewQuery).isEqualToNormalizingNewlines(queriesOfChanges.getNewRecordsQuery());
                assertThat(expectedDeletedQuery).isEqualToNormalizingNewlines(queriesOfChanges.getDeletedRecordsQuery());
            }).completeNow();
        });
    }

    @Test
    void shouldSuccessWhenOneTableAndNotNullColumns(VertxTestContext ctx) {
        // arrange
        Entity matView = Entity.builder()
                .name("matview")
                .fields(Arrays.asList(
                        EntityField.builder()
                                .name("col_boolean")
                                .ordinalPosition(1)
                                .type(ColumnType.BOOLEAN)
                                .nullable(false)
                                .build(),
                        EntityField.builder()
                                .name("col_time")
                                .ordinalPosition(2)
                                .type(ColumnType.TIME)
                                .nullable(true)
                                .build(),
                        EntityField.builder()
                                .name("id")
                                .ordinalPosition(0)
                                .primaryOrder(1)
                                .type(ColumnType.BIGINT)
                                .nullable(false)
                                .build()
                ))
                .build();

        SqlNode query = parseWithValidate("SELECT id, col_boolean, col_time FROM datamart1.dates", DATAMART_LIST);

        // act
        Future<PrepareRequestOfChangesResult> result = queriesOfChangesService.prepare(new PrepareRequestOfChangesRequest(DATAMART_LIST, "dev", new DeltaData(DELTA_NUM, DELTA_NUM_CN_FROM, DELTA_NUM_CN_TO), PREVIOUS_DELTA_NUM_CN_TO, query, matView));

        // assert
        String expectedNewQuery = "SELECT dates.id, CAST(dates.col_boolean AS INTEGER), CAST(EXTRACT(EPOCH FROM dates.col_time) * 1000000 AS BIGINT)\n" +
                "FROM datamart1.dates AS dates";
        String expectedDeletedQuery = "SELECT dates.id\n" +
                "FROM datamart1.dates AS dates";

        result.onComplete(event -> {
            if (event.failed()) {
                ctx.failNow(new AssertionError("Unexpected failure", event.cause()));
                return;
            }

            PrepareRequestOfChangesResult queriesOfChanges = event.result();

            ctx.verify(() -> {
                verify(queryEnrichmentService, times(2)).enrich(enrichQueryRequestArgumentCaptor.capture(), any());
                verifyNoMoreInteractions(queryEnrichmentService);

                List<String> allValues = enrichQueryRequestArgumentCaptor.getAllValues().stream()
                        .map(enrichQueryRequest -> enrichQueryRequest.getQuery().toSqlString(sqlDialect).toString().replace("\r\n", "\n"))
                        .collect(Collectors.toList());

                assertThat(allValues).contains(expectedDeletedQuery);
                assertThat(allValues).contains(expectedNewQuery);

                // assert result
                assertThat(expectedNewQuery).isEqualToNormalizingNewlines(queriesOfChanges.getNewRecordsQuery());
                assertThat(expectedDeletedQuery).isEqualToNormalizingNewlines(queriesOfChanges.getDeletedRecordsQuery());
            }).completeNow();
        });
    }

    @Test
    void shouldSuccessWhenOneTableAndConstants(VertxTestContext ctx) {
        // arrange
        Entity matView = Entity.builder()
                .name("matview")
                .fields(Arrays.asList(
                        EntityField.builder()
                                .name("col_time")
                                .ordinalPosition(3)
                                .type(ColumnType.TIME)
                                .nullable(true)
                                .build(),
                        EntityField.builder()
                                .name("col_timestamp")
                                .ordinalPosition(1)
                                .type(ColumnType.TIMESTAMP)
                                .nullable(true)
                                .build(),
                        EntityField.builder()
                                .name("col_date")
                                .ordinalPosition(2)
                                .type(ColumnType.DATE)
                                .nullable(true)
                                .build(),
                        EntityField.builder()
                                .name("col_boolean")
                                .ordinalPosition(4)
                                .type(ColumnType.BOOLEAN)
                                .nullable(true)
                                .build(),
                        EntityField.builder()
                                .name("id")
                                .ordinalPosition(0)
                                .primaryOrder(1)
                                .type(ColumnType.BIGINT)
                                .nullable(false)
                                .build()
                ))
                .build();

        SqlNode query = parseWithValidate("SELECT id, '2020-01-01 11:11:11', '2020-01-01', '11:11:11', true FROM datamart1.dates", DATAMART_LIST);

        // act
        Future<PrepareRequestOfChangesResult> result = queriesOfChangesService.prepare(new PrepareRequestOfChangesRequest(DATAMART_LIST, "dev", new DeltaData(DELTA_NUM, DELTA_NUM_CN_FROM, DELTA_NUM_CN_TO), PREVIOUS_DELTA_NUM_CN_TO, query, matView));

        // assert
        String expectedNewQuery = "SELECT dates.id, CAST(EXTRACT(EPOCH FROM CAST('2020-01-01 11:11:11' AS TIMESTAMP)) * 1000000 AS BIGINT), CAST(EXTRACT(EPOCH FROM CAST('2020-01-01' AS DATE)) / 86400 AS BIGINT), CAST(EXTRACT(EPOCH FROM CAST('11:11:11' AS TIME)) * 1000000 AS BIGINT), CAST(TRUE AS INTEGER)\n" +
                "FROM datamart1.dates AS dates";
        String expectedDeletedQuery = "SELECT dates.id\n" +
                "FROM datamart1.dates AS dates";

        result.onComplete(event -> {
            if (event.failed()) {
                ctx.failNow(new AssertionError("Unexpected failure", event.cause()));
                return;
            }

            PrepareRequestOfChangesResult queriesOfChanges = event.result();

            ctx.verify(() -> {
                verify(queryEnrichmentService, times(2)).enrich(enrichQueryRequestArgumentCaptor.capture(), any());
                verifyNoMoreInteractions(queryEnrichmentService);

                List<String> allValues = enrichQueryRequestArgumentCaptor.getAllValues().stream()
                        .map(enrichQueryRequest -> enrichQueryRequest.getQuery().toSqlString(sqlDialect).toString().replace("\r\n", "\n"))
                        .collect(Collectors.toList());

                assertThat(allValues).contains(expectedDeletedQuery);
                assertThat(allValues).contains(expectedNewQuery);

                // assert result
                assertThat(expectedNewQuery).isEqualToNormalizingNewlines(queriesOfChanges.getNewRecordsQuery());
                assertThat(expectedDeletedQuery).isEqualToNormalizingNewlines(queriesOfChanges.getDeletedRecordsQuery());
            }).completeNow();
        });
    }

    @Test
    void shouldSuccessWhenOneTableAndNullConstants(VertxTestContext ctx) {
        // arrange
        Entity matView = Entity.builder()
                .name("matview")
                .fields(Arrays.asList(
                        EntityField.builder()
                                .name("col_time")
                                .ordinalPosition(3)
                                .type(ColumnType.TIME)
                                .nullable(true)
                                .build(),
                        EntityField.builder()
                                .name("col_timestamp")
                                .ordinalPosition(1)
                                .type(ColumnType.TIMESTAMP)
                                .nullable(true)
                                .build(),
                        EntityField.builder()
                                .name("col_date")
                                .ordinalPosition(2)
                                .type(ColumnType.DATE)
                                .nullable(true)
                                .build(),
                        EntityField.builder()
                                .name("col_boolean")
                                .ordinalPosition(4)
                                .type(ColumnType.BOOLEAN)
                                .nullable(true)
                                .build(),
                        EntityField.builder()
                                .name("id")
                                .ordinalPosition(0)
                                .primaryOrder(1)
                                .type(ColumnType.BIGINT)
                                .nullable(false)
                                .build()
                ))
                .build();

        SqlNode query = parseWithValidate("SELECT id, null, null, null, null FROM datamart1.dates", DATAMART_LIST);

        // act
        Future<PrepareRequestOfChangesResult> result = queriesOfChangesService.prepare(new PrepareRequestOfChangesRequest(DATAMART_LIST, "dev", new DeltaData(DELTA_NUM, DELTA_NUM_CN_FROM, DELTA_NUM_CN_TO), PREVIOUS_DELTA_NUM_CN_TO, query, matView));

        // assert
        String expectedNewQuery = "SELECT dates.id, CAST(EXTRACT(EPOCH FROM CAST(NULL AS TIMESTAMP)) * 1000000 AS BIGINT), CAST(EXTRACT(EPOCH FROM CAST(NULL AS DATE)) / 86400 AS BIGINT), CAST(EXTRACT(EPOCH FROM CAST(NULL AS TIME)) * 1000000 AS BIGINT), CAST(NULL AS INTEGER)\n" +
                "FROM datamart1.dates AS dates";
        String expectedDeletedQuery = "SELECT dates.id\n" +
                "FROM datamart1.dates AS dates";

        result.onComplete(event -> {
            if (event.failed()) {
                ctx.failNow(new AssertionError("Unexpected failure", event.cause()));
                return;
            }

            PrepareRequestOfChangesResult queriesOfChanges = event.result();

            ctx.verify(() -> {
                verify(queryEnrichmentService, times(2)).enrich(enrichQueryRequestArgumentCaptor.capture(), any());
                verifyNoMoreInteractions(queryEnrichmentService);

                List<String> allValues = enrichQueryRequestArgumentCaptor.getAllValues().stream()
                        .map(enrichQueryRequest -> enrichQueryRequest.getQuery().toSqlString(sqlDialect).toString().replace("\r\n", "\n"))
                        .collect(Collectors.toList());

                assertThat(allValues).contains(expectedDeletedQuery);
                assertThat(allValues).contains(expectedNewQuery);

                // assert result
                assertThat(expectedNewQuery).isEqualToNormalizingNewlines(queriesOfChanges.getNewRecordsQuery());
                assertThat(expectedDeletedQuery).isEqualToNormalizingNewlines(queriesOfChanges.getDeletedRecordsQuery());
            }).completeNow();
        });
    }

    @Test
    void shouldSuccessWhenMultipleComplexTables(VertxTestContext ctx) {
        // arrange
        SqlNode query = parseWithValidate("SELECT t0.id, col_timestamp, col_date, col_time, name, surname, col_boolean FROM (SELECT t1.id,col_timestamp,col_date,col_time,name,col_boolean FROM datamart1.dates as t1 JOIN datamart1.names as t2 ON t1.id = t2.id) as t0 JOIN surnames as t3 ON t0.id = t3.id", DATAMART_LIST);
        Entity matView = Entity.builder()
                .name("matview")
                .fields(Arrays.asList(
                        EntityField.builder()
                                .name("id")
                                .ordinalPosition(0)
                                .primaryOrder(1)
                                .type(ColumnType.BIGINT)
                                .nullable(false)
                                .build(),
                        EntityField.builder()
                                .name("col_timestamp")
                                .ordinalPosition(1)
                                .type(ColumnType.TIMESTAMP)
                                .nullable(true)
                                .build(),
                        EntityField.builder()
                                .name("col_date")
                                .ordinalPosition(2)
                                .type(ColumnType.DATE)
                                .nullable(true)
                                .build(),
                        EntityField.builder()
                                .name("col_time")
                                .ordinalPosition(3)
                                .type(ColumnType.TIME)
                                .nullable(true)
                                .build(),
                        EntityField.builder()
                                .name("col_name")
                                .ordinalPosition(4)
                                .primaryOrder(2)
                                .type(ColumnType.VARCHAR)
                                .nullable(true)
                                .build(),
                        EntityField.builder()
                                .name("col_surname")
                                .ordinalPosition(5)
                                .type(ColumnType.VARCHAR)
                                .nullable(true)
                                .build(),
                        EntityField.builder()
                                .name("col_boolean")
                                .ordinalPosition(6)
                                .type(ColumnType.BOOLEAN)
                                .nullable(true)
                                .build()
                ))
                .build();

        // act
        Future<PrepareRequestOfChangesResult> result = queriesOfChangesService.prepare(new PrepareRequestOfChangesRequest(DATAMART_LIST, "dev", new DeltaData(DELTA_NUM, DELTA_NUM_CN_FROM, DELTA_NUM_CN_TO), PREVIOUS_DELTA_NUM_CN_TO, query, matView));

        // assert
        String expectedNewFreshQuery = "SELECT t0.id, CAST(EXTRACT(EPOCH FROM t0.col_timestamp) * 1000000 AS BIGINT), CAST(EXTRACT(EPOCH FROM t0.col_date) / 86400 AS BIGINT), CAST(EXTRACT(EPOCH FROM t0.col_time) * 1000000 AS BIGINT), t0.name, t3.surname, CAST(t0.col_boolean AS INTEGER)\n" +
                "FROM (SELECT t1.id, t1.col_timestamp, t1.col_date, t1.col_time, t2.name, t1.col_boolean\n" +
                "FROM datamart1.dates AS t1\n" +
                "INNER JOIN datamart1.names AS t2 ON t1.id = t2.id) AS t0\n" +
                "INNER JOIN datamart1.surnames AS t3 ON t0.id = t3.id";
        String expectedNewStaleQuery = "SELECT t0.id, CAST(EXTRACT(EPOCH FROM t0.col_timestamp) * 1000000 AS BIGINT), CAST(EXTRACT(EPOCH FROM t0.col_date) / 86400 AS BIGINT), CAST(EXTRACT(EPOCH FROM t0.col_time) * 1000000 AS BIGINT), t0.name, t3.surname, CAST(t0.col_boolean AS INTEGER)\n" +
                "FROM (SELECT t1.id, t1.col_timestamp, t1.col_date, t1.col_time, t2.name, t1.col_boolean\n" +
                "FROM datamart1.dates AS t1\n" +
                "INNER JOIN datamart1.names AS t2 ON t1.id = t2.id) AS t0\n" +
                "INNER JOIN datamart1.surnames AS t3 ON t0.id = t3.id";
        String expectedDeletedFreshQuery = "SELECT t0.id, t0.name\n" +
                "FROM (SELECT t1.id, t1.col_timestamp, t1.col_date, t1.col_time, t2.name, t1.col_boolean\n" +
                "FROM datamart1.dates AS t1\n" +
                "INNER JOIN datamart1.names AS t2 ON t1.id = t2.id) AS t0\n" +
                "INNER JOIN datamart1.surnames AS t3 ON t0.id = t3.id";
        String expectedDeletedStaleQuery = "SELECT t0.id, t0.name\n" +
                "FROM (SELECT t1.id, t1.col_timestamp, t1.col_date, t1.col_time, t2.name, t1.col_boolean\n" +
                "FROM datamart1.dates AS t1\n" +
                "INNER JOIN datamart1.names AS t2 ON t1.id = t2.id) AS t0\n" +
                "INNER JOIN datamart1.surnames AS t3 ON t0.id = t3.id";

        result.onComplete(event -> {
            if (event.failed()) {
                ctx.failNow(new AssertionError("Unexpected failure", event.cause()));
                return;
            }

            PrepareRequestOfChangesResult queriesOfChanges = event.result();

            ctx.verify(() -> {
                verify(queryEnrichmentService, times(4)).enrich(enrichQueryRequestArgumentCaptor.capture(), any());
                verifyNoMoreInteractions(queryEnrichmentService);

                List<String> allValues = enrichQueryRequestArgumentCaptor.getAllValues().stream()
                        .map(enrichQueryRequest -> enrichQueryRequest.getQuery().toSqlString(sqlDialect).toString().replace("\r\n", "\n"))
                        .collect(Collectors.toList());

                assertThat(allValues).contains(expectedNewFreshQuery);
                assertThat(allValues).contains(expectedNewStaleQuery);
                assertThat(allValues).contains(expectedDeletedStaleQuery);
                assertThat(allValues).contains(expectedDeletedFreshQuery);

                assertThat(expectedNewFreshQuery + " EXCEPT " + expectedNewStaleQuery).isEqualToNormalizingNewlines(queriesOfChanges.getNewRecordsQuery());
                assertThat(expectedDeletedStaleQuery + " EXCEPT " + expectedDeletedFreshQuery).isEqualToNormalizingNewlines(queriesOfChanges.getDeletedRecordsQuery());
            }).completeNow();
        });
    }

    @Test
    void shouldSuccessWhenAggregate(VertxTestContext ctx) {
        // arrange
        SqlNode query = parseWithValidate("SELECT 1, COUNT(*) FROM datamart1.dates", DATAMART_LIST);
        Entity matView = Entity.builder()
                .name("matview")
                .fields(Arrays.asList(
                        EntityField.builder()
                                .name("id")
                                .ordinalPosition(0)
                                .primaryOrder(1)
                                .type(ColumnType.BIGINT)
                                .nullable(false)
                                .build(),
                        EntityField.builder()
                                .name("col_sum")
                                .ordinalPosition(1)
                                .type(ColumnType.BIGINT)
                                .nullable(true)
                                .build()
                ))
                .build();

        // act
        Future<PrepareRequestOfChangesResult> result = queriesOfChangesService.prepare(new PrepareRequestOfChangesRequest(DATAMART_LIST, "dev", new DeltaData(DELTA_NUM, DELTA_NUM_CN_FROM, DELTA_NUM_CN_TO), PREVIOUS_DELTA_NUM_CN_TO, query, matView));

        // assert
        String expectedNewFreshQuery = "SELECT 1, COUNT(*)\n" +
                "FROM datamart1.dates AS dates";
        String expectedNewStaleQuery = "SELECT 1, COUNT(*)\n" +
                "FROM datamart1.dates AS dates";
        String expectedDeletedFreshQuery = "SELECT 1\n" +
                "FROM datamart1.dates AS dates";
        String expectedDeletedStaleQuery = "SELECT 1\n" +
                "FROM datamart1.dates AS dates";

        result.onComplete(event -> {
            if (event.failed()) {
                ctx.failNow(new AssertionError("Unexpected failure", event.cause()));
                return;
            }

            PrepareRequestOfChangesResult queriesOfChanges = event.result();

            ctx.verify(() -> {
                verify(queryEnrichmentService, times(4)).enrich(enrichQueryRequestArgumentCaptor.capture(), any());
                verifyNoMoreInteractions(queryEnrichmentService);

                List<String> allValues = enrichQueryRequestArgumentCaptor.getAllValues().stream()
                        .map(enrichQueryRequest -> enrichQueryRequest.getQuery().toSqlString(sqlDialect).toString().replace("\r\n", "\n"))
                        .collect(Collectors.toList());

                assertThat(allValues).contains(expectedNewFreshQuery);
                assertThat(allValues).contains(expectedNewStaleQuery);
                assertThat(allValues).contains(expectedDeletedStaleQuery);
                assertThat(allValues).contains(expectedDeletedFreshQuery);

                assertThat(expectedNewFreshQuery + " EXCEPT " + expectedNewStaleQuery).isEqualToNormalizingNewlines(queriesOfChanges.getNewRecordsQuery());
                assertThat(expectedDeletedStaleQuery + " EXCEPT " + expectedDeletedFreshQuery).isEqualToNormalizingNewlines(queriesOfChanges.getDeletedRecordsQuery());
            }).completeNow();
        });
    }

    @Test
    void shouldFailWhenParseFailed(VertxTestContext ctx) {
        // arrange
        SqlNode query = parse("SELECT * FROM dates, datamart2.names", DATAMART_LIST);

        // act
        Future<PrepareRequestOfChangesResult> result = queriesOfChangesService.prepare(new PrepareRequestOfChangesRequest(DATAMART_LIST, "dev", new DeltaData(DELTA_NUM, DELTA_NUM_CN_FROM, DELTA_NUM_CN_TO), PREVIOUS_DELTA_NUM_CN_TO, query, new Entity()));

        // assert
        result.onComplete(event -> {
            if (event.succeeded()) {
                ctx.failNow(new AssertionError("Unexpected success", event.cause()));
                return;
            }

            ctx.verify(() -> {
                Throwable cause = event.cause();
                Assertions.assertSame(DtmException.class, cause.getClass());
                assertTrue(cause.getMessage().contains("Object 'datamart2' not found"));
            }).completeNow();
        });
    }

    @Test
    void shouldFailWhenNoTables(VertxTestContext ctx) {
        // arrange
        Entity entity = Entity.builder().fields(Arrays.asList(EntityField.builder().type(ColumnType.BIGINT).build())).build();
        SqlNode query = parseWithValidate("SELECT 1", DATAMART_LIST);

        // act
        Future<PrepareRequestOfChangesResult> result = queriesOfChangesService.prepare(new PrepareRequestOfChangesRequest(DATAMART_LIST, "dev", new DeltaData(DELTA_NUM, DELTA_NUM_CN_FROM, DELTA_NUM_CN_TO), PREVIOUS_DELTA_NUM_CN_TO, query, entity));

        // assert
        result.onComplete(event -> {
            if (event.succeeded()) {
                ctx.failNow(new AssertionError("Unexpected success", event.cause()));
                return;
            }

            ctx.verify(() -> {
                Throwable cause = event.cause();
                Assertions.assertSame(DtmException.class, cause.getClass());
                assertTrue(cause.getMessage().contains("No tables in query"));
            }).completeNow();
        });
    }

    @Test
    void shouldFailWhenEnrichmentFailed(VertxTestContext ctx) {
        // arrange
        Entity entity = DATAMART_1.getEntities().get(0);
        DtmException expectedException = new DtmException("Enrich exception");
        reset(queryEnrichmentService);
        when(queryEnrichmentService.enrich(any(), any())).thenReturn(Future.failedFuture(expectedException));
        SqlNode query = parseWithValidate("SELECT id, col_timestamp, col_time, col_date, col_boolean FROM datamart1.dates", DATAMART_LIST);

        // act
        Future<PrepareRequestOfChangesResult> result = queriesOfChangesService.prepare(new PrepareRequestOfChangesRequest(DATAMART_LIST, "dev", new DeltaData(DELTA_NUM, DELTA_NUM_CN_FROM, DELTA_NUM_CN_TO), PREVIOUS_DELTA_NUM_CN_TO, query, entity));

        // assert
        result.onComplete(event -> {
            if (event.succeeded()) {
                ctx.failNow(new AssertionError("Unexpected success", event.cause()));
                return;
            }

            ctx.verify(() -> {
                Throwable cause = event.cause();
                Assertions.assertSame(cause, expectedException);
            }).completeNow();
        });
    }

    private SqlNode parseWithValidate(String query, List<Datamart> datamarts) {
        try {
            CalciteContext context = contextProvider.context(datamarts);
            SqlNode sqlNode = context.getPlanner().parse(query);
            return context.getPlanner().validate(sqlNode);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private SqlNode parse(String query, List<Datamart> datamarts) {
        try {
            CalciteContext context = contextProvider.context(datamarts);
            return context.getPlanner().parse(query);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}