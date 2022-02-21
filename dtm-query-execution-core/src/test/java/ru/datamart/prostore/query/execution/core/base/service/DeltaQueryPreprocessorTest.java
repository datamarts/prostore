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
package ru.datamart.prostore.query.execution.core.base.service;

import ru.datamart.prostore.common.delta.DeltaInformation;
import ru.datamart.prostore.common.delta.SelectOnInterval;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.query.calcite.core.configuration.CalciteCoreConfiguration;
import ru.datamart.prostore.query.calcite.core.framework.DtmCalciteFramework;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.EntityDao;
import ru.datamart.prostore.query.execution.core.base.service.delta.DeltaInformationExtractor;
import ru.datamart.prostore.query.execution.core.base.service.delta.DeltaInformationService;
import ru.datamart.prostore.query.execution.core.base.service.delta.DeltaQueryPreprocessor;
import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.List;

import static ru.datamart.prostore.common.delta.DeltaType.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class DeltaQueryPreprocessorTest {

    @Mock
    private DeltaInformationService deltaService;
    @Mock
    private EntityDao entityDao;

    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final DeltaInformationExtractor deltaInformationExtractor = new DeltaInformationExtractor();

    private SqlParser.Config parserConfig;
    private DeltaQueryPreprocessor deltaQueryPreprocessor;
    private Planner planner;

    @BeforeEach
    void setUp() {
        parserConfig = SqlParser.configBuilder()
                .setParserFactory(calciteCoreConfiguration.eddlParserImplFactory())
                .setConformance(SqlConformanceEnum.DEFAULT)
                .setLex(Lex.MYSQL)
                .setCaseSensitive(false)
                .setUnquotedCasing(Casing.TO_LOWER)
                .setQuotedCasing(Casing.TO_LOWER)
                .setQuoting(Quoting.DOUBLE_QUOTE)
                .build();
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        planner = DtmCalciteFramework.getPlanner(frameworkConfig);
        deltaQueryPreprocessor = new DeltaQueryPreprocessor(deltaService, deltaInformationExtractor, entityDao);
    }

    @Test
    void processWithDeltaByDates(VertxTestContext vertxTestContext) throws SqlParseException {
        // arrange
        val sql = "SELECT v.col1 AS c, (SELECT col4 FROM tblc FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59' t3 WHERE tblx.col6 = 0 ) AS r\n" +
                "FROM test.tbl FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14' AS t\n" +
                "INNER JOIN (SELECT col4, col5\n" +
                "FROM test2.tblx FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59'\n" +
                "WHERE tblx.col6 = 0) AS v ON t.col3 = v.col4\n" +
                "WHERE EXISTS (SELECT id\n" +
                "FROM (SELECT col4, col5 FROM tblz FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59' WHERE tblz.col6 = 0) AS view) order by v.col1";
        SqlNode sqlNode = planner.parse(sql);

        when(deltaService.getCnToByDeltaDatetime(any(), any())).thenReturn(Future.succeededFuture(1L))
                .thenReturn(Future.succeededFuture(2L))
                .thenReturn(Future.succeededFuture(3L))
                .thenReturn(Future.succeededFuture(4L));

        // act
        deltaQueryPreprocessor.process(sqlNode)
                .onComplete(promise -> vertxTestContext.verify(() -> {
                            // assert
                            if (promise.failed()) {
                                fail(promise.cause());
                            }

                            List<DeltaInformation> expected = Arrays.asList(
                                    new DeltaInformation("t3", "'2018-07-29 23:59:59'", false, DATETIME, 1L, null, "", "tblc", null),
                                    new DeltaInformation("", "'2018-07-29 23:59:59'", false, DATETIME, 2L, null, "", "tblz", null),
                                    new DeltaInformation("t", "'2019-12-23 15:15:14'", false, DATETIME, 3L, null, "test", "tbl", null),
                                    new DeltaInformation("", "'2018-07-29 23:59:59'", false, DATETIME, 4L, null, "test2", "tblx", null)
                            );
                            assertDeltas(expected, promise.result().getDeltaInformations());
                        })
                        .completeNow());

    }

    @Test
    void processWithFailureOnDelta(VertxTestContext vertxTestContext) throws SqlParseException {
        // arrange
        val sql = "SELECT v.col1 AS c, (SELECT col4 FROM tblc FOR SYSTEM_TIME AS OF DELTA_NUM 1 t3 WHERE tblx.col6 = 0 ) AS r\n" +
                "FROM test.tbl FOR SYSTEM_TIME AS OF DELTA_NUM 2 AS t\n" +
                "INNER JOIN (SELECT col4, col5\n" +
                "FROM test2.tblx FOR SYSTEM_TIME STARTED IN (3,4)\n" +
                "WHERE tblx.col6 = 0) AS v ON t.col3 = v.col4\n" +
                "WHERE EXISTS (SELECT id\n" +
                "FROM (SELECT col4, col5 FROM tblz FOR SYSTEM_TIME FINISHED IN (3,4) WHERE tblz.col6 = 0) AS view) order by v.col1";
        SqlNode sqlNode = planner.parse(sql);
        when(deltaService.getCnToByDeltaNum(any(), eq(1L))).thenReturn(Future.succeededFuture(-1L));
        when(deltaService.getCnToByDeltaNum(any(), eq(2L))).thenReturn(Future.succeededFuture(-1L));

        RuntimeException ex = new RuntimeException("delta range error");
        when(deltaService.getCnFromCnToByDeltaNums(any(), eq(3L), eq(4L))).thenReturn(Future.failedFuture(ex));

        // act
        deltaQueryPreprocessor.process(sqlNode)
                .onComplete(promise -> vertxTestContext.verify(() -> {
                            // assert
                            if (promise.succeeded()) {
                                fail("Unexpected success");
                            }

                            assertTrue(promise.cause().getMessage().contains("delta range error"));
                        })
                        .completeNow());
    }

    @Test
    void processWithDeltaNumAndIntervals(VertxTestContext vertxTestContext) throws SqlParseException {
        // arrange
        val sql = "SELECT v.col1 AS c, (SELECT col4 FROM tblc FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59' t3 WHERE tblx.col6 = 0 ) AS r\n" +
                "FROM test.tbl FOR SYSTEM_TIME AS OF DELTA_NUM 2 AS t\n" +
                "INNER JOIN (SELECT col4, col5\n" +
                "FROM test2.tblx FOR SYSTEM_TIME STARTED IN (3,4)\n" +
                "WHERE tblx.col6 = 0) AS v ON t.col3 = v.col4\n" +
                "WHERE EXISTS (SELECT id\n" +
                "FROM (SELECT col4, col5 FROM tblz FOR SYSTEM_TIME FINISHED IN (3,4) WHERE tblz.col6 = 0) AS view) order by v.col1";
        SqlNode sqlNode = planner.parse(sql);
        when(deltaService.getCnToByDeltaDatetime(any(), any())).thenReturn(Future.succeededFuture(1L));
        when(deltaService.getCnToByDeltaNum(any(), eq(2L))).thenReturn(Future.succeededFuture(2L));

        SelectOnInterval interval = new SelectOnInterval(3L, 4L);
        when(deltaService.getCnFromCnToByDeltaNums(any(), eq(3L), eq(4L))).thenReturn(Future.succeededFuture(interval));

        // act
        deltaQueryPreprocessor.process(sqlNode)
                .onComplete(promise -> vertxTestContext.verify(() -> {
                            // assert
                            if (promise.failed()) {
                                fail(promise.cause());
                            }

                            List<DeltaInformation> expected = Arrays.asList(
                                    new DeltaInformation("t3", "'2018-07-29 23:59:59'", false, DATETIME, 1L, null, "", "tblc", null),
                                    new DeltaInformation("", null, false, FINISHED_IN, null, new SelectOnInterval(3L, 4L), "", "tblz", null),
                                    new DeltaInformation("t", null, false, NUM, 2L, null, "test", "tbl", null),
                                    new DeltaInformation("", null, false, STARTED_IN, null, new SelectOnInterval(3L, 4L), "test2", "tblx", null)
                            );
                            assertDeltas(expected, promise.result().getDeltaInformations());
                        })
                        .completeNow());
    }

    @Test
    void processWithoutSnapshots(VertxTestContext vertxTestContext) throws SqlParseException {
        // arrange
        val sql = "SELECT v.col1 AS c, (SELECT col4 FROM tblc  t3 WHERE tblx.col6 = 0 ) AS r\n" +
                "FROM test.tbl AS t\n" +
                "INNER JOIN (SELECT col4, col5\n" +
                "FROM test2.tblx \n" +
                "WHERE tblx.col6 = 0) AS v ON t.col3 = v.col4\n" +
                "WHERE EXISTS (SELECT id\n" +
                "FROM (SELECT col4, col5 FROM tblz WHERE tblz.col6 = 0) AS view) order by v.col1";
        SqlNode sqlNode = planner.parse(sql);
        when(deltaService.getCnToDeltaOk(any()))
                .thenReturn(Future.succeededFuture(1L));
        when(entityDao.getEntity(any(), any()))
                .thenReturn(Future.succeededFuture(Entity.builder().entityType(EntityType.TABLE).build()));

        // act
        deltaQueryPreprocessor.process(sqlNode)
                .onComplete(promise -> vertxTestContext.verify(() -> {
                            // assert
                            if (promise.failed()) {
                                fail(promise.cause());
                            }

                            List<DeltaInformation> expected = Arrays.asList(
                                    new DeltaInformation("t3", null, false, WITHOUT_SNAPSHOT, 1L, null, "", "tblc", null),
                                    new DeltaInformation("", null, false, WITHOUT_SNAPSHOT, 1L, null, "", "tblz", null),
                                    new DeltaInformation("t", null, false, WITHOUT_SNAPSHOT, 1L, null, "test", "tbl", null),
                                    new DeltaInformation("", null, false, WITHOUT_SNAPSHOT, 1L, null, "test2", "tblx", null)
                            );
                            assertDeltas(expected, promise.result().getDeltaInformations());
                        })
                        .completeNow());
    }

    @Test
    void processWithoutSnapshotsAndMatview(VertxTestContext vertxTestContext) throws SqlParseException {
        // arrange
        val sql = "SELECT v.col1 AS c, (SELECT col4 FROM tblc  t3 WHERE tblx.col6 = 0 ) AS r\n" +
                "FROM test.tbl AS t\n" +
                "INNER JOIN (SELECT col4, col5\n" +
                "FROM test2.tblx \n" +
                "WHERE tblx.col6 = 0) AS v ON t.col3 = v.col4\n" +
                "WHERE EXISTS (SELECT id\n" +
                "FROM (SELECT col4, col5 FROM tblz WHERE tblz.col6 = 0) AS view) order by v.col1";
        SqlNode sqlNode = planner.parse(sql);
        when(deltaService.getCnToDeltaOk(any()))
                .thenReturn(Future.succeededFuture(10L));
        when(deltaService.getCnToByDeltaNum(any(), eq(5L)))
                .thenReturn(Future.succeededFuture(5L));
        when(deltaService.getCnToByDeltaNum(any(), eq(10L)))
                .thenReturn(Future.succeededFuture(10L));
        when(deltaService.getCnToByDeltaNum(any(), eq(20L)))
                .thenReturn(Future.succeededFuture(20L));
        when(entityDao.getEntity(any(), any()))
                .thenReturn(Future.succeededFuture(Entity.builder().entityType(EntityType.MATERIALIZED_VIEW).materializedDeltaNum(5L).build()))
                .thenReturn(Future.succeededFuture(Entity.builder().entityType(EntityType.MATERIALIZED_VIEW).materializedDeltaNum(10L).build()))
                .thenReturn(Future.succeededFuture(Entity.builder().entityType(EntityType.MATERIALIZED_VIEW).materializedDeltaNum(20L).build()))
                .thenReturn(Future.succeededFuture(Entity.builder().entityType(EntityType.TABLE).materializedDeltaNum(1L).build()));

        // act
        deltaQueryPreprocessor.process(sqlNode)
                .onComplete(promise -> vertxTestContext.verify(() -> {
                            // assert
                            if (promise.failed()) {
                                fail(promise.cause());
                            }

                            List<DeltaInformation> expected = Arrays.asList(
                                    new DeltaInformation("t3", null, false, WITHOUT_SNAPSHOT, 5L, null, "", "tblc", null),
                                    new DeltaInformation("", null, false, WITHOUT_SNAPSHOT, 10L, null, "", "tblz", null),
                                    new DeltaInformation("t", null, false, WITHOUT_SNAPSHOT, 10L, null, "test", "tbl", null),
                                    new DeltaInformation("", null, false, WITHOUT_SNAPSHOT, 10L, null, "test2", "tblx", null)
                            );
                            assertDeltas(expected, promise.result().getDeltaInformations());
                        })
                        .completeNow());
    }

    private void assertDeltas(List<DeltaInformation> expected, List<DeltaInformation> deltaInformations) {
        Matcher[] matchers = expected.stream()
                .map(deltaInformation -> {
                    return Matchers.allOf(
                            Matchers.hasProperty("tableAlias", Matchers.equalTo(deltaInformation.getTableAlias())),
                            Matchers.hasProperty("deltaTimestamp", Matchers.equalTo(deltaInformation.getDeltaTimestamp())),
                            Matchers.hasProperty("latestUncommittedDelta", Matchers.equalTo(deltaInformation.isLatestUncommittedDelta())),
                            Matchers.hasProperty("type", Matchers.equalTo(deltaInformation.getType())),
                            Matchers.hasProperty("selectOnNum", Matchers.equalTo(deltaInformation.getSelectOnNum())),
                            Matchers.hasProperty("selectOnInterval", deltaInformation.getSelectOnInterval() != null ? Matchers.allOf(
                                    Matchers.hasProperty("selectOnFrom", Matchers.equalTo(deltaInformation.getSelectOnInterval().getSelectOnFrom())),
                                    Matchers.hasProperty("selectOnTo", Matchers.equalTo(deltaInformation.getSelectOnInterval().getSelectOnTo()))
                            ) : Matchers.nullValue()),
                            Matchers.hasProperty("schemaName", Matchers.equalTo(deltaInformation.getSchemaName())),
                            Matchers.hasProperty("tableName", Matchers.equalTo(deltaInformation.getTableName()))
                    );
                })
                .toArray(Matcher[]::new);

        assertThat(deltaInformations, Matchers.contains(matchers));
    }
}
