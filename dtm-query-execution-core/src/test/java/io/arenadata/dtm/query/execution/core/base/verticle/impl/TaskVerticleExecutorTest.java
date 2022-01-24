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
package io.arenadata.dtm.query.execution.core.base.verticle.impl;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.query.execution.core.base.configuration.properties.VertxPoolProperties;
import io.arenadata.dtm.query.execution.core.base.verticle.TaskVerticleExecutor;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(VertxExtension.class)
class TaskVerticleExecutorTest {
    private final VertxPoolProperties vertxPoolProperties = new VertxPoolProperties();
    private TaskVerticleExecutor taskVerticleExecutor;

    @BeforeEach
    void setUp(Vertx vertx, VertxTestContext initContext) {
        vertxPoolProperties.setTaskTimeout(100L);
        taskVerticleExecutor = new TaskVerticleExecutor(vertxPoolProperties);
        vertx.deployVerticle(taskVerticleExecutor, event -> {
            if (event.failed()) {
                initContext.failNow(event.cause());
            } else {
                initContext.completeNow();
            }
        });
    }

    @Test
    void shouldSuccessWhenFutureSucceed(VertxTestContext testContext) {
        // act
        String result = "RESULT";
        taskVerticleExecutor.execute((Handler<Promise<String>>) event -> Future.succeededFuture(result).onComplete(event))
                .onComplete(event -> {
                    testContext.verify(() -> {
                        // assert
                        if (event.failed()) {
                            fail(event.cause());
                        }
                        assertEquals(result, event.result());
                    });
                    testContext.completeNow();
                });
    }

    @Test
    void shouldSuccessWhenVoidFutureSucceed(VertxTestContext testContext) {
        // act
        taskVerticleExecutor.execute(event -> Future.succeededFuture().onComplete(event))
                .onComplete(event -> {
                    testContext.verify(() -> {
                        // assert
                        if (event.failed()) {
                            fail(event.cause());
                        }
                        assertNull(event.result());
                    });
                    testContext.completeNow();
                });
    }

    @Test
    void shouldTimeoutWhenExecuteTimeout(VertxTestContext testContext) {
        // arrange
        Handler<Promise<Object>> handler = promise -> Future.future(p -> {
            try {
                Thread.sleep(150L);
                p.complete();
            } catch (InterruptedException e) {
                p.fail(e);
            }
        }).onComplete(promise);

        // act
        taskVerticleExecutor.execute(handler)
                .onComplete(event -> {
                    testContext.verify(() -> {
                        if (event.succeeded()) {
                            fail("Unexpected success");
                        }
                        assertSame(ReplyException.class, event.cause().getClass());
                        assertTrue(event.cause().getMessage().contains("Timed out"));
                    });
                    testContext.completeNow();
                });
    }

    @Test
    void shouldErrorWhenFutureEndedUpWithError(VertxTestContext testContext) {
        // act
        DtmException exception = new DtmException("Exception");
        taskVerticleExecutor.execute(event -> Future.failedFuture(exception).onComplete(event))
                .onComplete(event -> {
                    testContext.verify(() -> {
                        // assert
                        if (event.succeeded()) {
                            fail("Unexpected success");
                        }
                        assertSame(exception, event.cause());
                    });
                    testContext.completeNow();
                });
    }
}