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
package ru.datamart.prostore.query.execution.core.dml.service.view;

import ru.datamart.prostore.common.delta.DeltaInformation;
import ru.datamart.prostore.common.delta.DeltaType;
import ru.datamart.prostore.common.delta.SelectOnInterval;
import ru.datamart.prostore.common.exception.DeltaRangeInvalidException;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.query.calcite.core.configuration.CalciteCoreConfiguration;
import ru.datamart.prostore.query.calcite.core.service.DefinitionService;
import ru.datamart.prostore.query.calcite.core.util.CalciteUtil;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacade;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacadeImpl;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.EntityDao;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.ServiceDbDao;
import ru.datamart.prostore.query.execution.core.base.service.delta.DeltaInformationExtractor;
import ru.datamart.prostore.query.execution.core.base.service.delta.DeltaInformationService;
import ru.datamart.prostore.query.execution.core.calcite.configuration.CalciteConfiguration;
import ru.datamart.prostore.query.execution.core.calcite.service.CoreCalciteDefinitionService;
import io.vertx.core.Future;
import io.vertx.junit5.VertxTestContext;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@Slf4j
class ViewReplacerServiceTest {

    public static final String EXPECTED_WITHOUT_JOIN = "SELECT `v`.`col1` AS `c`, `v`.`col2` AS `r`\n" +
            "FROM (SELECT `col4`, `col5`\n" +
            "FROM `tblx` FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14'\n" +
            "WHERE `tblx`.`col6` = 0) AS `v`";

    public static final String EXPECTED_WITH_JOIN = "SELECT `v`.`col1` AS `c`, `v`.`col2` AS `r`\n" +
            "FROM `tbl` FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14' AS `t`\n" +
            "INNER JOIN (SELECT `col4`, `col5`\n" +
            "FROM `tblx` FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59'\n" +
            "WHERE `tblx`.`col6` = 0) AS `v` ON `t`.`col3` = `v`.`col4`";

    public static final String EXPECTED_WITH_JOIN_WITHOUT_ALIAS = "SELECT `view`.`col1` AS `c`, `view`.`col2` AS `r`\n" +
            "FROM (SELECT `col4`, `col5`\n" +
            "FROM `tblx`\n" +
            "WHERE `tblx`.`col6` = 0) AS `view`";

    public static final String EXPECTED_WITH_JOIN_AND_WHERE = "SELECT `v`.`col1` AS `c`, `v`.`col2` AS `r`\n" +
            "FROM (SELECT `col4`, `col5`\n" +
            "FROM `tblx` FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14'\n" +
            "WHERE `tblx`.`col6` = 0) AS `t`\n" +
            "INNER JOIN (SELECT `col4`, `col5`\n" +
            "FROM `tblx` FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59'\n" +
            "WHERE `tblx`.`col6` = 0) AS `v` ON `t`.`col3` = `v`.`col4`\n" +
            "WHERE EXISTS (SELECT `id`\n" +
            "FROM `view`)";

    public static final String EXPECTED_WITH_SELECT = "SELECT `t`.`col1` AS `c`, (SELECT `id`\n" +
            "FROM (SELECT `col4`, `col5`\n" +
            "FROM `tblx`\n" +
            "WHERE `tblx`.`col6` = 0) AS `view`\n" +
            "LIMIT 1) AS `r`\n" +
            "FROM `tblt` AS `t`";

    public static final String EXPECTED_WITH_DATAMART = "SELECT `v`.`col1` AS `c`, `v`.`col2` AS `r`\n" +
            "FROM (SELECT `col4`, `col5`\n" +
            "FROM `tblx`\n" +
            "WHERE `tblx`.`col6` = 0) AS `v`";

    public static final String EXPECTED_WITH_DELTA_NUM = "SELECT `v`.`col1` AS `c`, `v`.`col2` AS `r`\n" +
            "FROM (SELECT `col4`, `col5`\n" +
            "FROM `tblx` FOR SYSTEM_TIME AS OF DELTA_NUM 5\n" +
            "WHERE `tblx`.`col6` = 0) AS `v`";

    private final CalciteConfiguration config = new CalciteConfiguration();
    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final ServiceDbDao serviceDbDao = mock(ServiceDbDao.class);
    private final EntityDao entityDao = mock(EntityDao.class);
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final DefinitionService<SqlNode> definitionService =
            new CoreCalciteDefinitionService(config.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory()));
    private final LogicViewReplacer logicViewReplacer = new LogicViewReplacer(definitionService);
    private final DeltaInformationExtractor deltaInformationExtractor = mock(DeltaInformationExtractor.class);
    private final DeltaInformationService deltaInformationService = mock(DeltaInformationService.class);
    private final MaterializedViewReplacer materializedViewReplacer = new MaterializedViewReplacer(definitionService, deltaInformationExtractor, deltaInformationService);
    private final ViewReplacerService viewReplacerService = new ViewReplacerService(entityDao, logicViewReplacer, materializedViewReplacer);

    private VertxTestContext testContext;

    @BeforeEach
    void setUp() {
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        when(serviceDbDao.getEntityDao()).thenReturn(entityDao);

        testContext = new VertxTestContext();
    }

    @AfterEach
    public void check() throws InterruptedException {
        assertThat(testContext.awaitCompletion(5, TimeUnit.SECONDS)).isTrue();
        if (testContext.failed()) {
            fail(testContext.causeOfFailure());
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void withoutJoin() {
        when(entityDao.getEntity(any(), any()))
                .thenReturn(
                        Future.succeededFuture(Entity.builder()
                                .entityType(EntityType.VIEW)
                                .name("view")
                                .viewQuery("SELECT Col4, Col5 \n" +
                                        "FROM tblX \n" +
                                        "WHERE tblX.Col6 = 0")
                                .build()),
                        Future.succeededFuture(Entity.builder()
                                .entityType(EntityType.TABLE)
                                .name("tblX")
                                .build())
                );

        val sql = "SELECT v.Col1 as c, v.Col2 r\n" +
                "FROM test.view FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14' v";
        SqlNode sqlNode = definitionService.processingQuery(sql);

        viewReplacerService.replace(sqlNode, "datamart")
                .onComplete(sqlResult -> {
                    if (sqlResult.succeeded()) {
                        assertThat(sqlResult.result().toString()).isEqualToNormalizingNewlines(EXPECTED_WITHOUT_JOIN);
                        testContext.completeNow();
                    } else {
                        testContext.failNow(sqlResult.cause());
                    }
                });
    }

    @Test
    @SuppressWarnings("unchecked")
    void withDatamart() {
        when(entityDao.getEntity(any(), any()))
                .thenReturn(Future.succeededFuture(Entity.builder()
                                .entityType(EntityType.VIEW)
                                .name("view")
                                .viewQuery("SELECT Col4, Col5 \n" +
                                        "FROM tblX \n" +
                                        "WHERE tblX.Col6 = 0")
                                .build()),
                        Future.succeededFuture(Entity.builder()
                                .entityType(EntityType.TABLE)
                                .name("tblX")
                                .build())
                );

        val sql = "SELECT v.Col1 as c, v.Col2 r\n" +
                "FROM test.view v";
        SqlNode sqlNode = definitionService.processingQuery(sql);

        viewReplacerService.replace(sqlNode, "datamart")
                .onComplete(sqlResult -> {
                    if (sqlResult.succeeded()) {
                        assertThat(sqlResult.result().toString()).isEqualToNormalizingNewlines(EXPECTED_WITH_DATAMART);
                        testContext.completeNow();
                    } else {
                        testContext.failNow(sqlResult.cause());
                    }
                });
    }

    @Test
    @SuppressWarnings("unchecked")
    void withoutJoin_withoutAlias() {
        when(entityDao.getEntity(any(), any()))
                .thenReturn(Future.succeededFuture(Entity.builder()
                                .entityType(EntityType.VIEW)
                                .name("view")
                                .viewQuery("SELECT Col4, Col5 \n" +
                                        "FROM tblX \n" +
                                        "WHERE tblX.Col6 = 0")
                                .build()),
                        Future.succeededFuture(Entity.builder()
                                .entityType(EntityType.TABLE)
                                .name("tblX")
                                .build())
                );

        val sql = "SELECT view.Col1 as c, view.Col2 r\n" +
                "FROM view";
        SqlNode sqlNode = definitionService.processingQuery(sql);

        viewReplacerService.replace(sqlNode, "datamart")
                .onComplete(sqlResult -> {
                    if (sqlResult.succeeded()) {
                        assertThat(sqlResult.result().toString()).isEqualToNormalizingNewlines(EXPECTED_WITH_JOIN_WITHOUT_ALIAS);
                        testContext.completeNow();
                    } else {
                        testContext.failNow(sqlResult.cause());
                    }
                });
    }

    @Test
    @SuppressWarnings("unchecked")
    void withJoin() {
        when(entityDao.getEntity(any(), any()))
                .thenReturn(
                        Future.succeededFuture(Entity.builder()
                                .entityType(EntityType.TABLE)
                                .name("tbl")
                                .build()),
                        Future.succeededFuture(Entity.builder()
                                .entityType(EntityType.VIEW)
                                .name("view")
                                .viewQuery("SELECT Col4, Col5 \n" +
                                        "FROM tblX \n" +
                                        "WHERE tblX.Col6 = 0")
                                .build()),
                        Future.succeededFuture(Entity.builder()
                                .entityType(EntityType.TABLE)
                                .name("tblX")
                                .build())
                );
        val sql = "SELECT v.Col1 as c, v.Col2 r\n" +
                "FROM tbl FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14' t\n" +
                "JOIN view FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59' v\n" +
                "ON t.Col3 = v.Col4";
        SqlNode sqlNode = definitionService.processingQuery(sql);

        viewReplacerService.replace(sqlNode, "datamart")
                .onComplete(sqlResult -> {
                    if (sqlResult.succeeded()) {
                        assertThat(sqlResult.result().toString()).isEqualToNormalizingNewlines(EXPECTED_WITH_JOIN);
                        testContext.completeNow();
                    } else {
                        testContext.failNow(sqlResult.cause());
                    }
                });
    }

    @Test
    @SuppressWarnings("unchecked")
    void withJoinAndWhere() {
        when(entityDao.getEntity(any(), any()))
                .thenReturn(Future.succeededFuture(Entity.builder()
                                .entityType(EntityType.TABLE)
                                .name("tbl")
                                .build()),
                        Future.succeededFuture(Entity.builder()
                                .entityType(EntityType.VIEW)
                                .name("view")
                                .viewQuery("SELECT Col4, Col5 \n" +
                                        "FROM tblX \n" +
                                        "WHERE tblX.Col6 = 0")
                                .build()),
                        Future.succeededFuture(Entity.builder()
                                .entityType(EntityType.TABLE)
                                .name("tblX")
                                .build()), Future.succeededFuture(Entity.builder()
                                .entityType(EntityType.VIEW)
                                .name("view")
                                .viewQuery("SELECT Col4, Col5 \n" +
                                        "FROM tblX \n" +
                                        "WHERE tblX.Col6 = 0")
                                .build()),
                        Future.succeededFuture(Entity.builder()
                                .entityType(EntityType.TABLE)
                                .name("tblX")
                                .build())
                );

        val sql = "SELECT v.Col1 as c, v.Col2 r\n" +
                "FROM tbl FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14' t\n" +
                "JOIN view FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59' v\n" +
                "ON t.Col3 = v.Col4 \n" +
                "WHERE exists (select id from view)";
        SqlNode sqlNode = definitionService.processingQuery(sql);

        viewReplacerService.replace(sqlNode, "datamart")
                .onComplete(sqlResult -> {
                    if (sqlResult.succeeded()) {
                        assertThat(sqlResult.result().toString()).isEqualToNormalizingNewlines(EXPECTED_WITH_JOIN_AND_WHERE);
                        testContext.completeNow();
                    } else {
                        testContext.failNow(sqlResult.cause());
                    }
                });
    }

    @Test
    @SuppressWarnings("unchecked")
    void withJoinAndSelect() {
        when(entityDao.getEntity(any(), any()))
                .thenReturn(
                        Future.succeededFuture(Entity.builder()
                                .entityType(EntityType.VIEW)
                                .name("view")
                                .viewQuery("SELECT Col4, Col5 \n" +
                                        "FROM tblX \n" +
                                        "WHERE tblX.Col6 = 0")
                                .build()),
                        Future.succeededFuture(Entity.builder()
                                .entityType(EntityType.TABLE)
                                .name("tblt")
                                .build()),
                        Future.succeededFuture(Entity.builder()
                                .entityType(EntityType.TABLE)
                                .name("tblX")
                                .build())
                );

        val sql = "SELECT t.Col1 as c, (select id from view limit 1) r\n" +
                "FROM tblt t";
        SqlNode sqlNode = definitionService.processingQuery(sql);

        viewReplacerService.replace(sqlNode, "datamart")
                .onComplete(sqlResult -> {
                    if (sqlResult.succeeded()) {
                        assertThat(sqlResult.result().toString()).isEqualToNormalizingNewlines(EXPECTED_WITH_SELECT);
                        testContext.completeNow();
                    } else {
                        testContext.failNow(sqlResult.cause());
                    }
                });
    }

    @Test
    @SuppressWarnings("unchecked")
    void testMatViewNotReplacedWhenNoHints() {
        when(entityDao.getEntity(any(), any()))
                .thenReturn(
                        Future.succeededFuture(Entity.builder()
                                .entityType(EntityType.MATERIALIZED_VIEW)
                                .name("mat_view")
                                .build())
                );

        val deltaInformation = new DeltaInformation(
                "",
                null,
                false,
                DeltaType.WITHOUT_SNAPSHOT,
                null,
                null,
                "datamart",
                "mat_view",
                null
        );
        when(deltaInformationExtractor.getDeltaInformation(any(), any()))
                .thenReturn(deltaInformation);

        val sql = "SELECT * FROM mat_view v";
        SqlNode sqlNode = definitionService.processingQuery(sql);

        viewReplacerService.replace(sqlNode, "datamart")
                .onComplete(sqlResult -> {
                    if (sqlResult.succeeded()) {
                        assertThat(sqlResult.result().toString()).isEqualToNormalizingNewlines("SELECT *\nFROM `mat_view` AS `v`");
                        testContext.completeNow();
                    } else {
                        testContext.failNow(sqlResult.cause());
                    }
                });
    }

    @Test
    @SuppressWarnings("unchecked")
    void testMatViewWithoutDeltaNumReplacedForSystemTime() {
        when(entityDao.getEntity(any(), any()))
                .thenReturn(
                        Future.succeededFuture(Entity.builder()
                                .entityType(EntityType.MATERIALIZED_VIEW)
                                .materializedDeltaNum(null) // Never synced
                                .name("mat_view")
                                .viewQuery("SELECT Col4, Col5 \n" +
                                        "FROM tblX \n" +
                                        "WHERE tblX.Col6 = 0")
                                .build()),
                        Future.succeededFuture(Entity.builder()
                                .entityType(EntityType.TABLE)
                                .name("tblX")
                                .build())
                );

        val deltaInformation = new DeltaInformation(
                "",
                "'2019-12-23 15:15:14'",
                false,
                DeltaType.DATETIME,
                null,
                null,
                "datamart",
                "mat_view",
                null
        );
        when(deltaInformationExtractor.getDeltaInformation(any(), any()))
                .thenReturn(deltaInformation);
        doAnswer(answer -> Future.succeededFuture(5L)).when(deltaInformationService)
                .getDeltaNumByDatetime("datamart", CalciteUtil.parseLocalDateTime("2019-12-23 15:15:14"));

        val sql = "SELECT v.Col1 as c, v.Col2 r\n" +
                "FROM mat_view FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14' v";
        SqlNode sqlNode = definitionService.processingQuery(sql);

        viewReplacerService.replace(sqlNode, "datamart")
                .onComplete(sqlResult -> {
                    if (sqlResult.succeeded()) {
                        assertThat(sqlResult.result().toString()).isEqualToNormalizingNewlines(EXPECTED_WITHOUT_JOIN);
                        testContext.completeNow();
                    } else {
                        testContext.failNow(sqlResult.cause());
                    }
                });
    }

    @Test
    @SuppressWarnings("unchecked")
    void testMatViewReplacedForSystemTimeWhenNotSync() {
        when(entityDao.getEntity(any(), any()))
                .thenReturn(
                        Future.succeededFuture(Entity.builder()
                                .entityType(EntityType.MATERIALIZED_VIEW)
                                .materializedDeltaNum(4L) // deltaNum 4 is less then requested delta 5 below
                                .name("mat_view")
                                .viewQuery("SELECT Col4, Col5 \n" +
                                        "FROM tblX \n" +
                                        "WHERE tblX.Col6 = 0")
                                .build()),
                        Future.succeededFuture(Entity.builder()
                                .entityType(EntityType.TABLE)
                                .name("tblX")
                                .build())
                );

        val deltaInformation = new DeltaInformation(
                "",
                "'2019-12-23 15:15:14'",
                false,
                DeltaType.DATETIME,
                null,
                null,
                "datamart",
                "mat_view",
                null
        );
        when(deltaInformationExtractor.getDeltaInformation(any(), any()))
                .thenReturn(deltaInformation);
        doAnswer(answer -> Future.succeededFuture(5L)).when(deltaInformationService)
                .getDeltaNumByDatetime("datamart", CalciteUtil.parseLocalDateTime("2019-12-23 15:15:14"));

        val sql = "SELECT v.Col1 as c, v.Col2 r\n" +
                "FROM mat_view FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14' v";
        SqlNode sqlNode = definitionService.processingQuery(sql);

        viewReplacerService.replace(sqlNode, "datamart")
                .onComplete(sqlResult -> {
                    if (sqlResult.succeeded()) {
                        assertThat(sqlResult.result().toString()).isEqualToNormalizingNewlines(EXPECTED_WITHOUT_JOIN);
                        testContext.completeNow();
                    } else {
                        testContext.failNow(sqlResult.cause());
                    }
                });
    }

    @Test
    @SuppressWarnings("unchecked")
    void testMatViewNotReplacedForSystemTimeWhenSync() {
        when(entityDao.getEntity(any(), any()))
                .thenReturn(
                        Future.succeededFuture(Entity.builder()
                                .entityType(EntityType.MATERIALIZED_VIEW)
                                .materializedDeltaNum(6L) // deltaNum 6 is greater then requested delta 5 below
                                .name("mat_view")
                                .viewQuery("SELECT Col4, Col5 \n" +
                                        "FROM tblX \n" +
                                        "WHERE tblX.Col6 = 0")
                                .build()),
                        Future.succeededFuture(Entity.builder()
                                .entityType(EntityType.TABLE)
                                .name("tblX")
                                .build())
                );

        val deltaInformation = new DeltaInformation(
                "",
                "'2019-12-23 15:15:14'",
                false,
                DeltaType.DATETIME,
                null,
                null,
                "datamart",
                "mat_view",
                null
        );
        when(deltaInformationExtractor.getDeltaInformation(any(), any()))
                .thenReturn(deltaInformation);
        doAnswer(answer -> Future.succeededFuture(5L)).when(deltaInformationService)
                .getDeltaNumByDatetime("datamart", CalciteUtil.parseLocalDateTime("2019-12-23 15:15:14"));

        val sql = "SELECT v.Col1 as c, v.Col2 r\n" +
                "FROM mat_view FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14' v";
        SqlNode sqlNode = definitionService.processingQuery(sql);

        viewReplacerService.replace(sqlNode, "datamart")
                .onComplete(sqlResult -> {
                    if (sqlResult.succeeded()) {
                        assertThat(sqlResult.result().toString())
                                .isEqualToNormalizingNewlines("SELECT `v`.`col1` AS `c`, `v`.`col2` AS `r`\nFROM `mat_view` FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14' AS `v`");
                        testContext.completeNow();
                    } else {
                        testContext.failNow(sqlResult.cause());
                    }
                });
    }

    @Test
    @SuppressWarnings("unchecked")
    void testMatViewWithoutDeltaNumReplacedForDeltaNum() {
        when(entityDao.getEntity(any(), any()))
                .thenReturn(
                        Future.succeededFuture(Entity.builder()
                                .entityType(EntityType.MATERIALIZED_VIEW)
                                .materializedDeltaNum(null) // Never synced
                                .name("mat_view")
                                .viewQuery("SELECT Col4, Col5 \n" +
                                        "FROM tblX \n" +
                                        "WHERE tblX.Col6 = 0")
                                .build()),
                        Future.succeededFuture(Entity.builder()
                                .entityType(EntityType.TABLE)
                                .name("tblX")
                                .build())
                );

        val deltaInformation = new DeltaInformation(
                "",
                null,
                false,
                DeltaType.NUM,
                5L,
                null,
                "datamart",
                "mat_view",
                null
        );
        when(deltaInformationExtractor.getDeltaInformation(any(), any()))
                .thenReturn(deltaInformation);

        val sql = "SELECT v.Col1 as c, v.Col2 r\n" +
                "FROM mat_view FOR SYSTEM_TIME AS OF DELTA_NUM 5 v";
        SqlNode sqlNode = definitionService.processingQuery(sql);

        viewReplacerService.replace(sqlNode, "datamart")
                .onComplete(sqlResult -> {
                    if (sqlResult.succeeded()) {
                        assertThat(sqlResult.result().toString()).isEqualToNormalizingNewlines(EXPECTED_WITH_DELTA_NUM);
                        testContext.completeNow();
                    } else {
                        testContext.failNow(sqlResult.cause());
                    }
                });
    }

    @Test
    @SuppressWarnings("unchecked")
    void testMatViewReplacedForDeltaNumWhenNotSynced() {
        when(entityDao.getEntity(any(), any()))
                .thenReturn(
                        Future.succeededFuture(Entity.builder()
                                .entityType(EntityType.MATERIALIZED_VIEW)
                                .materializedDeltaNum(4L) // Less then delta from the request. Replace
                                .name("mat_view")
                                .viewQuery("SELECT Col4, Col5 \n" +
                                        "FROM tblX \n" +
                                        "WHERE tblX.Col6 = 0")
                                .build()),
                        Future.succeededFuture(Entity.builder()
                                .entityType(EntityType.TABLE)
                                .name("tblX")
                                .build())
                );

        val deltaInformation = new DeltaInformation(
                "",
                null,
                false,
                DeltaType.NUM,
                5L,
                null,
                "datamart",
                "mat_view",
                null
        );
        when(deltaInformationExtractor.getDeltaInformation(any(), any()))
                .thenReturn(deltaInformation);

        val sql = "SELECT v.Col1 as c, v.Col2 r\n" +
                "FROM mat_view FOR SYSTEM_TIME AS OF DELTA_NUM 5 v";
        SqlNode sqlNode = definitionService.processingQuery(sql);

        viewReplacerService.replace(sqlNode, "datamart")
                .onComplete(sqlResult -> {
                    if (sqlResult.succeeded()) {
                        assertThat(sqlResult.result().toString()).isEqualToNormalizingNewlines(EXPECTED_WITH_DELTA_NUM);
                        testContext.completeNow();
                    } else {
                        testContext.failNow(sqlResult.cause());
                    }
                });
    }

    @Test
    @SuppressWarnings("unchecked")
    void testMatViewNotReplacedForDeltaNumWhenSynced() {
        when(entityDao.getEntity(any(), any()))
                .thenReturn(
                        Future.succeededFuture(Entity.builder()
                                .entityType(EntityType.MATERIALIZED_VIEW)
                                .materializedDeltaNum(5L) // Equals to delta from the request. Not replacing
                                .name("mat_view")
                                .viewQuery("SELECT Col4, Col5 \n" +
                                        "FROM tblX \n" +
                                        "WHERE tblX.Col6 = 0")
                                .build()),
                        Future.succeededFuture(Entity.builder()
                                .entityType(EntityType.TABLE)
                                .name("tblX")
                                .build())
                );

        val deltaInformation = new DeltaInformation(
                "",
                null,
                false,
                DeltaType.NUM,
                5L,
                null,
                "datamart",
                "mat_view",
                null
        );
        when(deltaInformationExtractor.getDeltaInformation(any(), any()))
                .thenReturn(deltaInformation);

        val sql = "SELECT v.Col1 as c, v.Col2 r\n" +
                "FROM mat_view FOR SYSTEM_TIME AS OF DELTA_NUM 5 v";
        SqlNode sqlNode = definitionService.processingQuery(sql);

        viewReplacerService.replace(sqlNode, "datamart")
                .onComplete(sqlResult -> {
                    if (sqlResult.succeeded()) {
                        assertThat(sqlResult.result().toString()).isEqualToNormalizingNewlines("SELECT `v`.`col1` AS `c`, `v`.`col2` AS `r`\n" +
                                "FROM `mat_view` FOR SYSTEM_TIME AS OF DELTA_NUM 5 AS `v`");
                        testContext.completeNow();
                    } else {
                        testContext.failNow(sqlResult.cause());
                    }
                });
    }

    @Test
    @SuppressWarnings("unchecked")
    void testLatestUncommittedDeltaIsNotSupportedForMatViews() {
        when(entityDao.getEntity(any(), any()))
                .thenReturn(
                        Future.succeededFuture(Entity.builder()
                                .entityType(EntityType.MATERIALIZED_VIEW)
                                .materializedDeltaNum(10L)
                                .name("mat_view")
                                .viewQuery("SELECT Col4, Col5 \n" +
                                        "FROM tblX \n" +
                                        "WHERE tblX.Col6 = 0")
                                .build()),
                        Future.succeededFuture(Entity.builder()
                                .entityType(EntityType.TABLE)
                                .name("tblX")
                                .build())
                );

        val deltaInformation = new DeltaInformation(
                "",
                null,
                true,
                DeltaType.NUM,
                null,
                null,
                "datamart",
                "mat_view",
                null
        );
        when(deltaInformationExtractor.getDeltaInformation(any(), any()))
                .thenReturn(deltaInformation);

        val sql = "SELECT v.Col1 as c, v.Col2 r\n" +
                "FROM mat_view FOR SYSTEM_TIME AS OF LATEST_UNCOMMITTED_DELTA v";
        SqlNode sqlNode = definitionService.processingQuery(sql);

        viewReplacerService.replace(sqlNode, "datamart")
                .onComplete(sqlResult -> {
                    if (sqlResult.succeeded()) {
                        // DeltaRangeInvalidException is expected, the test should fail
                        testContext.failNow(sqlResult.cause());
                    } else {
                        assertThat(sqlResult.cause()).isInstanceOf(DeltaRangeInvalidException.class);
                        testContext.completeNow();
                    }
                });
    }

    @Test
    @SuppressWarnings("unchecked")
    void testMatViewThrowsErrorForStartedInWrongPeriod() {
        when(entityDao.getEntity(any(), any()))
                .thenReturn(
                        Future.succeededFuture(Entity.builder()
                                .entityType(EntityType.MATERIALIZED_VIEW)
                                .materializedDeltaNum(5L) // 5 is less than "to": (2, 6)
                                .name("mat_view")
                                .viewQuery("SELECT Col4, Col5 \n" +
                                        "FROM tblX \n" +
                                        "WHERE tblX.Col6 = 0")
                                .build()),
                        Future.succeededFuture(Entity.builder()
                                .entityType(EntityType.TABLE)
                                .name("tblX")
                                .build())
                );

        val deltaInformation = new DeltaInformation(
                "",
                null,
                false,
                DeltaType.STARTED_IN,
                null,
                new SelectOnInterval(2L, 6L),
                "datamart",
                "mat_view",
                null
        );
        when(deltaInformationExtractor.getDeltaInformation(any(), any()))
                .thenReturn(deltaInformation);

        val sql = "SELECT v.Col1 as c, v.Col2 r\n" +
                "FROM mat_view FOR SYSTEM_TIME STARTED IN (2, 6) v";
        SqlNode sqlNode = definitionService.processingQuery(sql);

        viewReplacerService.replace(sqlNode, "datamart")
                .onComplete(sqlResult -> {
                    if (sqlResult.succeeded()) {
                        // DeltaRangeInvalidException is expected, the test should fail
                        testContext.failNow(sqlResult.cause());
                    } else {
                        assertThat(sqlResult.cause()).isInstanceOf(DeltaRangeInvalidException.class);
                        testContext.completeNow();
                    }
                });
    }

    @Test
    @SuppressWarnings("unchecked")
    void testMatViewThrowsErrorForStartedInWhenNotSynced() {
        when(entityDao.getEntity(any(), any()))
                .thenReturn(
                        Future.succeededFuture(Entity.builder()
                                .entityType(EntityType.MATERIALIZED_VIEW)
                                .materializedDeltaNum(null) // never synced
                                .name("mat_view")
                                .viewQuery("SELECT Col4, Col5 \n" +
                                        "FROM tblX \n" +
                                        "WHERE tblX.Col6 = 0")
                                .build()),
                        Future.succeededFuture(Entity.builder()
                                .entityType(EntityType.TABLE)
                                .name("tblX")
                                .build())
                );

        val deltaInformation = new DeltaInformation(
                "",
                null,
                false,
                DeltaType.STARTED_IN,
                null,
                new SelectOnInterval(2L, 6L),
                "datamart",
                "mat_view",
                null
        );
        when(deltaInformationExtractor.getDeltaInformation(any(), any()))
                .thenReturn(deltaInformation);

        val sql = "SELECT v.Col1 as c, v.Col2 r\n" +
                "FROM mat_view FOR SYSTEM_TIME STARTED IN (2, 6) v";
        SqlNode sqlNode = definitionService.processingQuery(sql);

        viewReplacerService.replace(sqlNode, "datamart")
                .onComplete(sqlResult -> {
                    if (sqlResult.succeeded()) {
                        // DeltaRangeInvalidException is expected, the test should fail
                        testContext.failNow(sqlResult.cause());
                    } else {
                        assertThat(sqlResult.cause()).isInstanceOf(DeltaRangeInvalidException.class);
                        testContext.completeNow();
                    }
                });
    }

    @Test
    @SuppressWarnings("unchecked")
    void testMatViewForStartedIn() {
        when(entityDao.getEntity(any(), any()))
                .thenReturn(
                        Future.succeededFuture(Entity.builder()
                                .entityType(EntityType.MATERIALIZED_VIEW)
                                .materializedDeltaNum(15L) // 15 is considered "sync" for period: (10, 15)
                                .name("mat_view")
                                .viewQuery("SELECT Col4, Col5 \n" +
                                        "FROM tblX \n" +
                                        "WHERE tblX.Col6 = 0")
                                .build()),
                        Future.succeededFuture(Entity.builder()
                                .entityType(EntityType.TABLE)
                                .name("tblX")
                                .build())
                );

        val deltaInformation = new DeltaInformation(
                "",
                null,
                false,
                DeltaType.STARTED_IN,
                null,
                new SelectOnInterval(10L, 15L),
                "datamart",
                "mat_view",
                null
        );
        when(deltaInformationExtractor.getDeltaInformation(any(), any()))
                .thenReturn(deltaInformation);

        val sql = "SELECT v.Col1 as c, v.Col2 r\n" +
                "FROM mat_view FOR SYSTEM_TIME STARTED IN (10, 15) v";
        SqlNode sqlNode = definitionService.processingQuery(sql);

        viewReplacerService.replace(sqlNode, "datamart")
                .onComplete(sqlResult -> {
                    if (sqlResult.succeeded()) {
                        assertThat(sqlResult.result().toString()).isEqualToNormalizingNewlines("SELECT `v`.`col1` AS `c`, `v`.`col2` AS `r`\n" +
                                "FROM `mat_view` FOR SYSTEM_TIME STARTED IN (10,15) AS `v`");
                        testContext.completeNow();
                    } else {
                        testContext.failNow(sqlResult.cause());
                    }
                });
    }

    @Test
    @SuppressWarnings("unchecked")
    void testMatViewThrowsErrorForFinishedInWrongPeriod() {
        when(entityDao.getEntity(any(), any()))
                .thenReturn(
                        Future.succeededFuture(Entity.builder()
                                .entityType(EntityType.MATERIALIZED_VIEW)
                                .materializedDeltaNum(5L) // 5 is less than "to": (2, 6)
                                .name("mat_view")
                                .viewQuery("SELECT Col4, Col5 \n" +
                                        "FROM tblX \n" +
                                        "WHERE tblX.Col6 = 0")
                                .build()),
                        Future.succeededFuture(Entity.builder()
                                .entityType(EntityType.TABLE)
                                .name("tblX")
                                .build())
                );

        val deltaInformation = new DeltaInformation(
                "",
                null,
                false,
                DeltaType.FINISHED_IN,
                null,
                new SelectOnInterval(2L, 6L),
                "datamart",
                "mat_view",
                null
        );
        when(deltaInformationExtractor.getDeltaInformation(any(), any()))
                .thenReturn(deltaInformation);

        val sql = "SELECT v.Col1 as c, v.Col2 r\n" +
                "FROM mat_view FOR SYSTEM_TIME FINISHED IN (2, 6) v";
        SqlNode sqlNode = definitionService.processingQuery(sql);

        viewReplacerService.replace(sqlNode, "datamart")
                .onComplete(sqlResult -> {
                    if (sqlResult.succeeded()) {
                        // DeltaRangeInvalidException is expected, the test should fail
                        testContext.failNow(sqlResult.cause());
                    } else {
                        assertThat(sqlResult.cause()).isInstanceOf(DeltaRangeInvalidException.class);
                        testContext.completeNow();
                    }
                });
    }

    @Test
    @SuppressWarnings("unchecked")
    void testMatViewThrowsErrorForFinishedInWhenNotSynced() {
        when(entityDao.getEntity(any(), any()))
                .thenReturn(
                        Future.succeededFuture(Entity.builder()
                                .entityType(EntityType.MATERIALIZED_VIEW)
                                .materializedDeltaNum(null) // never synced
                                .name("mat_view")
                                .viewQuery("SELECT Col4, Col5 \n" +
                                        "FROM tblX \n" +
                                        "WHERE tblX.Col6 = 0")
                                .build()),
                        Future.succeededFuture(Entity.builder()
                                .entityType(EntityType.TABLE)
                                .name("tblX")
                                .build())
                );

        val deltaInformation = new DeltaInformation(
                "",
                null,
                false,
                DeltaType.FINISHED_IN,
                null,
                new SelectOnInterval(2L, 6L),
                "datamart",
                "mat_view",
                null
        );
        when(deltaInformationExtractor.getDeltaInformation(any(), any()))
                .thenReturn(deltaInformation);

        val sql = "SELECT v.Col1 as c, v.Col2 r\n" +
                "FROM mat_view FOR SYSTEM_TIME FINISHED IN (2, 6) v";
        SqlNode sqlNode = definitionService.processingQuery(sql);

        viewReplacerService.replace(sqlNode, "datamart")
                .onComplete(sqlResult -> {
                    if (sqlResult.succeeded()) {
                        // DeltaRangeInvalidException is expected, the test should fail
                        testContext.failNow(sqlResult.cause());
                    } else {
                        assertThat(sqlResult.cause()).isInstanceOf(DeltaRangeInvalidException.class);
                        testContext.completeNow();
                    }
                });
    }

    @Test
    @SuppressWarnings("unchecked")
    void testMatViewForFinishedIn() {
        when(entityDao.getEntity(any(), any()))
                .thenReturn(
                        Future.succeededFuture(Entity.builder()
                                .entityType(EntityType.MATERIALIZED_VIEW)
                                .materializedDeltaNum(15L) // 15 is considered "sync" for period: (10, 15)
                                .name("mat_view")
                                .viewQuery("SELECT Col4, Col5 \n" +
                                        "FROM tblX \n" +
                                        "WHERE tblX.Col6 = 0")
                                .build()),
                        Future.succeededFuture(Entity.builder()
                                .entityType(EntityType.TABLE)
                                .name("tblX")
                                .build())
                );

        val deltaInformation = new DeltaInformation(
                "",
                null,
                false,
                DeltaType.FINISHED_IN,
                null,
                new SelectOnInterval(10L, 15L),
                "datamart",
                "mat_view",
                null
        );
        when(deltaInformationExtractor.getDeltaInformation(any(), any()))
                .thenReturn(deltaInformation);

        val sql = "SELECT v.Col1 as c, v.Col2 r\n" +
                "FROM mat_view FOR SYSTEM_TIME FINISHED IN (10, 15) v";
        SqlNode sqlNode = definitionService.processingQuery(sql);

        viewReplacerService.replace(sqlNode, "datamart")
                .onComplete(sqlResult -> {
                    if (sqlResult.succeeded()) {
                        assertThat(sqlResult.result().toString()).isEqualToNormalizingNewlines("SELECT `v`.`col1` AS `c`, `v`.`col2` AS `r`\n" +
                                "FROM `mat_view` FOR SYSTEM_TIME FINISHED IN (10,15) AS `v`");
                        testContext.completeNow();
                    } else {
                        testContext.failNow(sqlResult.cause());
                    }
                });
    }
}
