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
package ru.datamart.prostore.query.execution.core.delta.repository.zookeeper.impl;

import ru.datamart.prostore.common.configuration.core.CoreConstants;
import ru.datamart.prostore.query.execution.core.base.configuration.properties.ServiceDbZookeeperProperties;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.DatamartDao;
import ru.datamart.prostore.query.execution.core.base.service.zookeeper.ZookeeperConnectionProvider;
import ru.datamart.prostore.query.execution.core.base.service.zookeeper.ZookeeperExecutor;
import ru.datamart.prostore.query.execution.core.base.service.zookeeper.impl.ZookeeperConnectionProviderImpl;
import ru.datamart.prostore.query.execution.core.base.service.zookeeper.impl.ZookeeperExecutorImpl;
import ru.datamart.prostore.query.execution.core.delta.dto.DeltaWriteOp;
import ru.datamart.prostore.query.execution.core.delta.dto.DeltaWriteOpRequest;
import ru.datamart.prostore.query.execution.core.delta.dto.OkDelta;
import ru.datamart.prostore.query.execution.core.delta.exception.DeltaIsNotCommittedException;
import ru.datamart.prostore.query.execution.core.delta.exception.DeltaNotFinishedException;
import ru.datamart.prostore.query.execution.core.delta.exception.TableBlockedException;
import ru.datamart.prostore.query.execution.core.delta.repository.executor.*;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxTestContext;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.*;

@Slf4j
class DeltaServiceDaoImplTest {
    private static final String ENV_NAME = "test";
    private static final String DATAMART = "dtm";
    private static final String BAD_DTM = "bad_dtm";
    private TestingServer testingServer;
    private DeltaServiceDaoImpl dao;

    @BeforeEach
    void before() throws Exception {
        testingServer = new TestingServer(55431, true);

        ServiceDbZookeeperProperties properties = new ServiceDbZookeeperProperties();
        properties.setChroot("/arena");
        properties.setConnectionString("localhost:55431");
        properties.setConnectionTimeoutMs(10_000);
        properties.setSessionTimeoutMs(30_000);
        ZookeeperConnectionProvider manager = new ZookeeperConnectionProviderImpl(properties, ENV_NAME);
        ZookeeperExecutor executor = new ZookeeperExecutorImpl(manager, Vertx.vertx());
        DatamartDao datamartDao = new DatamartDao(executor, ENV_NAME);
        List<DeltaDaoExecutor> executors = new ArrayList<>();
        executors.add(new DeleteDeltaHotExecutor(executor, ENV_NAME));
        executors.add(new DeleteWriteOperationExecutor(executor, ENV_NAME));
        executors.add(new GetDeltaByDateTimeExecutor(executor, ENV_NAME));
        executors.add(new GetDeltaByNumExecutor(executor, ENV_NAME));
        executors.add(new GetDeltaHotExecutor(executor, ENV_NAME));
        executors.add(new GetDeltaOkExecutor(executor, ENV_NAME));
        executors.add(new WriteDeltaErrorExecutor(executor, ENV_NAME));
        executors.add(new WriteDeltaHotSuccessExecutor(executor, ENV_NAME));
        executors.add(new WriteNewDeltaHotExecutor(executor, ENV_NAME));
        executors.add(new WriteNewOperationExecutor(executor, ENV_NAME));
        executors.add(new WriteOperationErrorExecutor(executor, ENV_NAME));
        executors.add(new WriteOperationSuccessExecutor(executor, ENV_NAME));
        executors.add(new GetDeltaWriteOperationsExecutor(executor, ENV_NAME));

        dao = new DeltaServiceDaoImpl(executors);

        val testContext = new VertxTestContext();
        datamartDao.createDatamart(DATAMART)
                .onSuccess(r -> testContext.completeNow())
                .onFailure(testContext::failNow);
        assertThat(testContext.awaitCompletion(5, TimeUnit.SECONDS)).isTrue();
    }

    @AfterEach
    void after() throws IOException {
        testingServer.stop();
        testingServer.close();
    }

    @Test
    void fullSuccess() {
        List<Long> sysCns = new ArrayList<>();
        val expectedTime = LocalDateTime.now(CoreConstants.CORE_ZONE_ID).withNano(0);
        val expectedDelta = OkDelta.builder()
                .deltaDate(expectedTime)
                .deltaNum(1)
                .cnFrom(5)
                .cnTo(15)
                .build();
        OkDelta[] actualDeltas = new OkDelta[2];
        dao.writeNewDeltaHot(DATAMART)
                .compose(r -> dao.writeNewOperation(getOpRequest("tbl0"))).map(sysCns::add)
                .compose(r -> dao.writeNewOperation(getOpRequest("tbl1"))).map(sysCns::add)
                .compose(r -> dao.writeNewOperation(getOpRequest("tbl2"))).map(sysCns::add)
                .compose(r -> dao.writeNewOperation(getOpRequest("tbl3"))).map(sysCns::add)
                .compose(r -> dao.writeNewOperation(getOpRequest("tbl4"))).map(sysCns::add)
                .compose(r -> dao.writeOperationSuccess(DATAMART, sysCns.get(2)))
                .compose(r -> dao.writeOperationSuccess(DATAMART, sysCns.get(3)))
                .compose(r -> dao.writeOperationSuccess(DATAMART, sysCns.get(4)))
                .compose(r -> dao.writeOperationSuccess(DATAMART, sysCns.get(0)))
                .compose(r -> dao.writeOperationSuccess(DATAMART, sysCns.get(1)))
                .compose(r -> dao.getDeltaHot(DATAMART))
                .map(r -> {
                    log.info("" + r);
                    return r;
                })
                .compose(r -> dao.writeDeltaHotSuccess(DATAMART,
                        LocalDateTime.now(CoreConstants.CORE_ZONE_ID).minusHours(1)))
                .compose(r -> dao.getDeltaOk(DATAMART))
                .map(r -> {
                    log.info("" + r);
                    sysCns.clear();
                    return r;
                })
                .compose(r -> dao.writeNewDeltaHot(DATAMART))
                .compose(r -> dao.getDeltaHot(DATAMART))
                .map(r -> {
                    log.info("" + r);
                    return r;
                })
                .compose(r -> dao.writeNewOperation(getOpRequest("tbl0"))).map(sysCns::add)
                .compose(r -> dao.writeNewOperation(getOpRequest("tbl1"))).map(sysCns::add)
                .compose(r -> dao.writeNewOperation(getOpRequest("tbl2"))).map(sysCns::add)
                .compose(r -> dao.writeNewOperation(getOpRequest("tbl3"))).map(sysCns::add)
                .compose(r -> dao.writeNewOperation(getOpRequest("tbl4"))).map(sysCns::add)
                .compose(r -> dao.writeNewOperation(getOpRequest("tbl5"))).map(sysCns::add)
                .compose(r -> dao.writeNewOperation(getOpRequest("tbl6"))).map(sysCns::add)
                .compose(r -> dao.writeNewOperation(getOpRequest("tbl7"))).map(sysCns::add)
                .compose(r -> dao.writeNewOperation(getOpRequest("tbl8"))).map(sysCns::add)
                .compose(r -> dao.writeNewOperation(getOpRequest("tbl9"))).map(sysCns::add)
                .compose(r -> dao.writeNewOperation(getOpRequest("tbl10"))).map(sysCns::add)
                .compose(r -> dao.writeOperationSuccess(DATAMART, sysCns.get(2)))
                .compose(r -> dao.writeOperationSuccess(DATAMART, sysCns.get(3)))
                .compose(r -> dao.writeOperationSuccess(DATAMART, sysCns.get(4)))
                .compose(r -> dao.writeOperationSuccess(DATAMART, sysCns.get(0)))
                .compose(r -> dao.writeOperationSuccess(DATAMART, sysCns.get(1)))
                .compose(r -> dao.writeOperationSuccess(DATAMART, sysCns.get(5)))
                .compose(r -> dao.writeOperationSuccess(DATAMART, sysCns.get(6)))
                .compose(r -> dao.writeOperationSuccess(DATAMART, sysCns.get(7)))
                .compose(r -> dao.writeOperationSuccess(DATAMART, sysCns.get(10)))
                .compose(r -> dao.writeOperationSuccess(DATAMART, sysCns.get(8)))
                .compose(r -> dao.writeOperationSuccess(DATAMART, sysCns.get(9)))
                .compose(r -> dao.getDeltaHot(DATAMART))
                .map(r -> {
                    log.info("" + r);
                    return r;
                })
                .compose(r -> dao.writeDeltaHotSuccess(DATAMART, expectedTime))
                .compose(r -> dao.getDeltaOk(DATAMART))
                .map(r -> {
                    log.info("" + r);
                    actualDeltas[0] = r;
                    return r;
                })
                .compose(r -> dao.writeNewDeltaHot(DATAMART))
                .compose(r -> dao.writeDeltaHotSuccess(DATAMART, LocalDateTime.now(CoreConstants.CORE_ZONE_ID).plusHours(1)))
                .compose(r -> dao.getDeltaByDateTime(DATAMART, LocalDateTime.now(CoreConstants.CORE_ZONE_ID)))
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    assertEquals(expectedDelta, actualDeltas[0]);
                    assertEquals(expectedDelta, actualDeltas[1]);
                });
    }

    @Test
    void writeNewDeltaHot() throws InterruptedException {
        val testContext = new VertxTestContext();
        dao.writeNewDeltaHot(DATAMART)
                .onSuccess(r -> {
                    log.info("result: [{}]", r);
                    testContext.completeNow();
                })
                .onFailure(error -> {
                    log.error("error", error);
                    testContext.failNow(error);
                });
        assertThat(testContext.awaitCompletion(120, TimeUnit.SECONDS)).isTrue();
        assertTrue(testContext.completed());
    }

    @Test
    void writeNewDeltaHotBad() throws InterruptedException {
        val testContext = new VertxTestContext();
        dao.writeNewDeltaHot(BAD_DTM)
                .onSuccess(r -> {
                    log.info("result: [{}]", r);
                    testContext.completeNow();
                })
                .onFailure(error -> {
                    log.error("error", error);
                    testContext.failNow(error);
                });
        assertThat(testContext.awaitCompletion(120, TimeUnit.SECONDS)).isTrue();
        assertTrue(testContext.failed());

    }

    @Test
    void writeNewDeltaHotAlreadyExists() throws InterruptedException {
        val testContext = new VertxTestContext();
        dao.writeNewDeltaHot(DATAMART)
                .compose(r -> dao.writeNewDeltaHot(DATAMART))
                .onSuccess(r -> {
                    log.info("result: [{}]", r);
                    testContext.completeNow();
                })
                .onFailure(error -> {
                    log.error("error", error);
                    testContext.failNow(error);
                });
        assertThat(testContext.awaitCompletion(120, TimeUnit.SECONDS)).isTrue();
        assertTrue(testContext.failed());
        assertTrue(testContext.causeOfFailure() instanceof DeltaIsNotCommittedException);
    }

    @Test
    void writeDeltaHotSuccess() throws InterruptedException {
        val testContext = new VertxTestContext();
        dao.writeNewDeltaHot(DATAMART)
                .compose(r -> dao.writeDeltaHotSuccess(DATAMART))
                .onSuccess(r -> {
                    log.info("result: [{}]", r);
                    testContext.completeNow();
                })
                .onFailure(error -> {
                    log.error("error", error);
                    testContext.failNow(error);
                });
        assertThat(testContext.awaitCompletion(120, TimeUnit.SECONDS)).isTrue();
        assertTrue(testContext.completed());
    }

    @Test
    void writeDeltaHotSuccessNotStarted() throws InterruptedException {
        val testContext = new VertxTestContext();
        dao.writeNewDeltaHot(DATAMART)
                .compose(r -> dao.writeDeltaHotSuccess(DATAMART))
                .compose(r -> dao.writeDeltaHotSuccess(DATAMART))
                .onSuccess(r -> {
                    log.info("result: [{}]", r);
                    testContext.completeNow();
                })
                .onFailure(error -> {
                    log.error("error", error);
                    testContext.failNow(error);
                });
        assertThat(testContext.awaitCompletion(120, TimeUnit.SECONDS)).isTrue();
        assertTrue(testContext.failed());
    }

    @Test
    void writeManyDeltaHotSuccess() {
        dao.writeNewDeltaHot(DATAMART)
                .compose(r -> dao.writeDeltaHotSuccess(DATAMART))
                .compose(r -> dao.writeNewDeltaHot(DATAMART))
                .compose(r -> dao.writeDeltaHotSuccess(DATAMART))
                .onComplete(ar -> assertTrue(ar.succeeded()));
    }

    @Test
    void writeDeltaError() throws InterruptedException {
        val testContext = new VertxTestContext();
        dao.writeNewDeltaHot(DATAMART)
                .compose(r -> dao.writeDeltaError(DATAMART, 0L))
                .onSuccess(r -> {
                    log.info("result: [{}]", r);
                    testContext.completeNow();
                })
                .onFailure(error -> {
                    log.error("error", error);
                    testContext.failNow(error);
                });
        assertThat(testContext.awaitCompletion(120, TimeUnit.SECONDS)).isTrue();
        assertTrue(testContext.completed());
    }

    @Test
    void deleteDeltaHot() throws InterruptedException {
        val testContext = new VertxTestContext();
        dao.writeNewDeltaHot(DATAMART)
                .compose(r -> dao.deleteDeltaHot(DATAMART))
                .onSuccess(r -> {
                    log.info("result: [{}]", r);
                    testContext.completeNow();
                })
                .onFailure(error -> {
                    log.error("error", error);
                    testContext.failNow(error);
                });
        assertThat(testContext.awaitCompletion(120, TimeUnit.SECONDS)).isTrue();
        assertTrue(testContext.completed());
    }

    @Test
    void writeNewOperation() throws InterruptedException {
        val testContext = new VertxTestContext();
        dao.writeNewDeltaHot(DATAMART)
                .compose(r -> {
                    DeltaWriteOpRequest operation = getOpRequest("tbl1");
                    return dao.writeNewOperation(operation);
                })
                .onSuccess(r -> {
                    log.info("result: [{}]", r);
                    testContext.completeNow();
                })
                .onFailure(error -> {
                    log.error("error", error);
                    testContext.failNow(error);
                });
        assertThat(testContext.awaitCompletion(120, TimeUnit.SECONDS)).isTrue();
        assertTrue(testContext.completed());
    }

    @Test
    void writeDeltaHotSuccessNotFinishedOperation() throws InterruptedException {
        val testContext = new VertxTestContext();
        dao.writeNewDeltaHot(DATAMART)
                .compose(r -> dao.writeNewOperation(getOpRequest("tbl0")))
                .compose(r -> dao.writeNewOperation(getOpRequest("tbl1")))
                .compose(r -> dao.writeNewOperation(getOpRequest("tbl2")))
                .compose(r -> dao.writeNewOperation(getOpRequest("tbl3")))
                .compose(r -> dao.writeNewOperation(getOpRequest("tbl4")))
                .compose(r -> dao.writeNewOperation(getOpRequest("tbl5")))
                .compose(r -> dao.writeNewOperation(getOpRequest("tbl6")))
                .compose(r -> dao.writeNewOperation(getOpRequest("tbl7")))
                .compose(r -> dao.writeNewOperation(getOpRequest("tbl8")))
                .compose(r -> dao.writeNewOperation(getOpRequest("tbl9")))
                .compose(r -> dao.writeNewOperation(getOpRequest("tbl10")))
                .compose(r -> dao.writeOperationSuccess(DATAMART, 2L))
                .compose(r -> dao.writeOperationSuccess(DATAMART, 3L))
                .compose(r -> dao.writeOperationSuccess(DATAMART, 4L))
                .compose(r -> dao.writeOperationSuccess(DATAMART, 0L))
                .compose(r -> dao.writeOperationSuccess(DATAMART, 1L))
                .compose(r -> dao.writeOperationSuccess(DATAMART, 5L))
                .compose(r -> dao.writeOperationSuccess(DATAMART, 6L))
                .compose(r -> dao.writeOperationSuccess(DATAMART, 10L))
                .compose(r -> dao.writeOperationSuccess(DATAMART, 8L))
                .compose(r -> dao.writeOperationSuccess(DATAMART, 9L))
                .compose(r -> dao.writeDeltaHotSuccess(DATAMART))
                .onSuccess(r -> {
                    log.info("result: [{}]", r);
                    testContext.completeNow();
                })
                .onFailure(error -> {
                    log.error("error", error);
                    testContext.failNow(error);
                });
        assertThat(testContext.awaitCompletion(120, TimeUnit.SECONDS)).isTrue();
        assertTrue(testContext.failed());
        assertTrue(testContext.causeOfFailure() instanceof DeltaNotFinishedException);
    }

    @Test
    void writeOperationError() throws InterruptedException {
        val testContext = new VertxTestContext();
        dao.writeNewDeltaHot(DATAMART)
                .compose(r -> dao.writeNewOperation(getOpRequest("tbl0")))
                .compose(r -> dao.writeOperationError(DATAMART, 0L))
                .onSuccess(r -> {
                    log.info("result: [{}]", r);
                    testContext.completeNow();
                })
                .onFailure(error -> {
                    log.error("error", error);
                    testContext.failNow(error);
                });
        assertThat(testContext.awaitCompletion(120, TimeUnit.SECONDS)).isTrue();
        assertTrue(testContext.completed());
    }

    @Test
    void writeOperationSuccessTableBlocked() throws InterruptedException {
        val testContext = new VertxTestContext();
        dao.writeNewDeltaHot(DATAMART)
                .compose(r -> dao.writeNewOperation(getOpRequest("tbl1")))
                .compose(r -> dao.writeNewOperation(getOpRequest("tbl1")))
                .onSuccess(r -> {
                    log.info("result: [{}]", r);
                    testContext.completeNow();
                })
                .onFailure(error -> {
                    log.error("error", error);
                    testContext.failNow(error);
                });
        assertThat(testContext.awaitCompletion(120, TimeUnit.SECONDS)).isTrue();
        assertTrue(testContext.failed());
        assertTrue(testContext.causeOfFailure() instanceof TableBlockedException);
    }

    @Test
    void getWriteOpListTest() throws InterruptedException {
        val testContext = new VertxTestContext();
        List<Long> sysCns = new ArrayList<>();
        final List<String> tables = Arrays.asList("tbl0", "tbl1", "tbl2", "tbl3", "tbl4");
        final Map<String, Long> expectedCnMap = new HashMap<>();
        expectedCnMap.put(tables.get(0), 5L);
        expectedCnMap.put(tables.get(1), 1L);
        expectedCnMap.put(tables.get(2), 2L);
        expectedCnMap.put(tables.get(3), 3L);
        expectedCnMap.put(tables.get(4), 4L);
        List<DeltaWriteOp> actualWriteOpList = new ArrayList<>();
        dao.writeNewDeltaHot(DATAMART)
                .compose(r -> dao.writeNewOperation(getOpRequest(tables.get(0)))).map(sysCns::add)
                .compose(r -> dao.writeNewOperation(getOpRequest(tables.get(1)))).map(sysCns::add)
                .compose(r -> dao.writeNewOperation(getOpRequest(tables.get(2)))).map(sysCns::add)
                .compose(r -> dao.writeNewOperation(getOpRequest(tables.get(3)))).map(sysCns::add)
                .compose(r -> dao.writeNewOperation(getOpRequest(tables.get(4)))).map(sysCns::add)
                .compose(r -> dao.writeOperationSuccess(DATAMART, sysCns.get(0)))
                .compose(r -> dao.writeNewOperation(getOpRequest(tables.get(0)))).map(sysCns::add)
                .compose(r -> dao.getDeltaWriteOperations(DATAMART))
                .onSuccess(r -> {
                    log.info("result: [{}]", r);
                    actualWriteOpList.addAll(r);
                    testContext.completeNow();
                })
                .onFailure(error -> {
                    log.error("error", error);
                    testContext.failNow(error);
                });
        assertThat(testContext.awaitCompletion(120, TimeUnit.SECONDS)).isTrue();
        assertTrue(testContext.completed());
        assertFalse(actualWriteOpList.isEmpty());
        actualWriteOpList.forEach(wrOp -> assertEquals(expectedCnMap.get(wrOp.getTableName()), wrOp.getSysCn()));
    }

    @Test
    void getNullWriteOpList() throws InterruptedException {
        val testContext = new VertxTestContext();
        List<List<DeltaWriteOp>> result = new ArrayList<>();
        dao.getDeltaWriteOperations(DATAMART)
                .onSuccess(r -> {
                    log.info("result: [{}]", r);
                    result.add(r);
                    testContext.completeNow();
                })
                .onFailure(error -> {
                    log.error("error", error);
                    testContext.failNow(error);
                });
        assertThat(testContext.awaitCompletion(120, TimeUnit.SECONDS)).isTrue();
        assertTrue(testContext.completed());
        assertTrue(result.get(0).isEmpty());
    }

    private DeltaWriteOpRequest getOpRequest(String tableName) {
        return DeltaWriteOpRequest.builder()
                .tableNameExt(tableName + "_ext")
                .datamart(DATAMART)
                .tableName(tableName)
                .query("select 1")
                .build();
    }

}
