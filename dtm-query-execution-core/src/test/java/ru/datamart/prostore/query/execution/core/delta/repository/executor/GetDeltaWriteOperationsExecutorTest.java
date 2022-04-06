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
package ru.datamart.prostore.query.execution.core.delta.repository.executor;

import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import ru.datamart.prostore.query.execution.core.base.service.zookeeper.ZookeeperExecutor;
import ru.datamart.prostore.query.execution.core.delta.dto.DeltaWriteOp;
import ru.datamart.prostore.query.execution.core.delta.exception.DeltaException;
import ru.datamart.prostore.serialization.CoreSerialization;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
class GetDeltaWriteOperationsExecutorTest {
    private static final String ENV = "test";
    private static final String DATAMART = "dm_test";
    private static final String PATH = "/" + ENV + "/" + DATAMART + "/run";
    private static final String ERROR_CHILD_PATH = "error_child";
    private static final String DELTA_EXCEPTION_MSG = "TEST ERROR";
    private static final Class<GetDeltaWriteOperationsExecutor> EXPECTED_EXECUTOR_IFACE = GetDeltaWriteOperationsExecutor.class;
    private final ZookeeperExecutor executor = mock(ZookeeperExecutor.class);
    private final GetDeltaWriteOperationsExecutor writeOperationsExecutor = new GetDeltaWriteOperationsExecutor(executor, ENV);

    @Test
    void shouldSuccessWhenPathExists(VertxTestContext testContext) {
        when(executor.exists(PATH)).thenReturn(Future.succeededFuture(true));
        when(executor.getChildren(PATH)).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(executor.getData(any(), nullable(Watcher.class), any(Stat.class)))
                .thenReturn(Future.succeededFuture(new byte[0]));

        writeOperationsExecutor.execute(DATAMART)
                .onComplete(
                        ar -> testContext.verify(() -> {
                                    assertTrue(ar.succeeded());
                                    assertTrue(ar.result().isEmpty());
                                }
                        ).completeNow()
                );
    }

    @Test
    void shouldSuccessWhenPathDoesNotExist(VertxTestContext testContext) {
        when(executor.exists(PATH)).thenReturn(Future.succeededFuture(false));

        writeOperationsExecutor.execute(DATAMART)
                .onComplete(
                        ar -> testContext.verify(() -> assertTrue(ar.succeeded())).completeNow()
                );
    }

    @Test
    void shouldSuccessWhenChildNodeDoesNotExist(VertxTestContext testContext) {
        when(executor.exists(PATH)).thenReturn(Future.succeededFuture(true));
        when(executor.getChildren(PATH)).thenReturn(Future.failedFuture(new KeeperException.NoNodeException()));

        writeOperationsExecutor.execute(DATAMART)
                .onComplete(
                        ar -> testContext.verify(() -> assertTrue(ar.succeeded())).completeNow()
                );
    }

    @Test
    public void shouldFailWhenDeltaExceptionOccursInChildrenPaths(VertxTestContext testContext) {
        when(executor.exists(PATH)).thenReturn(Future.succeededFuture(true));
        when(executor.getChildren(PATH)).thenReturn(Future.failedFuture(new DeltaException(DELTA_EXCEPTION_MSG)));

        writeOperationsExecutor.execute(DATAMART)
                .onComplete(
                        ar -> testContext.verify(() -> {
                            assertTrue(ar.failed());
                            assertEquals(DeltaException.class, ar.cause().getClass());
                            assertEquals(DELTA_EXCEPTION_MSG, ar.cause().getMessage());
                        }).completeNow()
                );
    }

    @Test
    public void shouldFailWhenChildPathContainsErrorString(VertxTestContext testContext) {
        when(executor.exists(PATH)).thenReturn(Future.succeededFuture(true));
        when(executor.getChildren(PATH)).thenReturn(Future.succeededFuture(Collections.singletonList(ERROR_CHILD_PATH)));

        writeOperationsExecutor.execute(DATAMART)
                .onComplete(
                        ar -> testContext.verify(() -> {
                            assertTrue(ar.failed());
                            assertEquals(DeltaException.class, ar.cause().getClass());
                            assertEquals("Can't get delta write operation list by datamart[" + DATAMART + "]: For input string: \"" + ERROR_CHILD_PATH + "\"", ar.cause().getMessage());
                        }).completeNow()
                );
    }

    @Test
    void shouldSuccessWhenDeltaWriteOpNotEmpty(VertxTestContext testContext) {
        when(executor.exists(PATH)).thenReturn(Future.succeededFuture(true));
        when(executor.getChildren(PATH)).thenReturn(Future.succeededFuture(Collections.singletonList("/1")));
        val deltaWriteOp = DeltaWriteOp.builder()
                .tableName("test").cnFrom(5).sysCn(10L).build();
        when(executor.getData(any())).thenReturn(Future.succeededFuture(CoreSerialization.serialize(deltaWriteOp)));

        writeOperationsExecutor.execute(DATAMART)
                .onComplete(
                        ar -> testContext.verify(() -> {
                                    assertTrue(ar.succeeded());
                                }
                        ).completeNow()
                );
    }

    @Test
    void shouldReturnCorrectInterface() {
        assertEquals(EXPECTED_EXECUTOR_IFACE, writeOperationsExecutor.getExecutorInterface());
    }
}