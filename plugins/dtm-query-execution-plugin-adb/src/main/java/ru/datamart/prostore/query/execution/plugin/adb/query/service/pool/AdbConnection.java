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
package ru.datamart.prostore.query.execution.plugin.adb.query.service.pool;

import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.query.execution.plugin.adb.base.configuration.properties.AdbProperties;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.pgclient.PgConnection;
import io.vertx.sqlclient.SqlConnection;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class AdbConnection {
    public static final String CONNECTION_FAILED_MESSAGE = "ADB connection in failed state and can't be restored";
    private final AtomicReference<PgConnection> connection = new AtomicReference<>(null);
    private final AtomicReference<State> connectionState = new AtomicReference<>(State.CLOSED);
    private final AtomicInteger operations = new AtomicInteger(0);
    private final AdbProperties adbProperties;
    private final Vertx vertx;
    private final AdbConnectionFactory connectionFactory;
    private final Queue<Task> queue = new ConcurrentLinkedQueue<>();

    public AdbConnection(AdbConnectionFactory connectionFactory, Vertx vertx, AdbProperties adbProperties) {
        this.vertx = vertx;
        this.adbProperties = adbProperties;
        this.connectionFactory = connectionFactory;

        reestablishConnection();
    }

    public Future<SqlConnection> acquire() {
        if (connectionState.get() == State.FAILED) {
            return Future.failedFuture(new DtmException(CONNECTION_FAILED_MESSAGE));
        }

        val context = vertx.getOrCreateContext();
        return Future.future(promise -> {
            queue.add(new Task(context, promise));
            runNextTask();
        });
    }

    public void release() {
        vertx.runOnContext(event -> {
            int currentOps = operations.incrementAndGet();
            if (adbProperties.getQueriesByConnectLimit() > 0 && currentOps >= adbProperties.getQueriesByConnectLimit()) {
                closeActiveConnection();
                reestablishConnection();
                return;
            }

            if (connectionState.compareAndSet(State.ACQUIRED, State.FREE)) {
                runNextTask();
            }
        });
    }

    private void closeActiveConnection() {
        if (connectionState.compareAndSet(State.ACQUIRED, State.CLOSED)) {
            val connection = this.connection.get();
            if (connection != null) {
                this.connection.set(null);
                connection.closeHandler(null);
                connection.close().onComplete(ar -> {
                    if (ar.succeeded()) {
                        log.trace("ADB Connection is closed");
                    } else {
                        log.error("ADB Connection failed to close", ar.cause());
                    }
                });
            }
        }
    }

    private void reestablishConnection() {
        while (true) {
            val state = connectionState.get();
            if (state == State.FAILED || state == State.RECONNECTING) {
                return;
            }

            if (connectionState.compareAndSet(state, State.RECONNECTING)) {
                break;
            }
        }

        log.trace("Reestablish connection for ADB");
        reconnect()
                .onSuccess(conn -> {
                    log.trace("Connection reconnected and available");
                    this.operations.set(0);
                    this.connection.set(conn);
                    this.connectionState.set(State.FREE);
                    runNextTask();
                })
                .onFailure(t -> {
                    log.error("Connection failed, could not reconnect", t);
                    this.connectionState.set(State.FAILED);
                    runNextTask();
                }).mapEmpty();
    }

    private Future<PgConnection> reconnect() {
        return Future.future(promise -> reconnectWithRetry(adbProperties.getMaxReconnections(), promise));
    }

    private void reconnectWithRetry(int reconnectsLeft, Promise<PgConnection> promise) {
        if (adbProperties.getMaxReconnections() > 0 && reconnectsLeft <= 0) {
            promise.fail(new DtmException(String.format("Failed to reestablish connection in %s retries", adbProperties.getMaxReconnections())));
            return;
        }

        if (adbProperties.getMaxReconnections() > 0) {
            log.trace("Trying to reconnect to ADB, try: {}/{}", 1 + adbProperties.getMaxReconnections() - reconnectsLeft, adbProperties.getMaxReconnections());
        } else {
            log.trace("Trying to reconnect to ADB, try: {}", 1 - reconnectsLeft);
        }

        connectionFactory.createPgConnection(vertx)
                .onSuccess(result -> {
                    if (adbProperties.getMaxReconnections() > 0) {
                        log.trace("Successful reconnect to ADB, try: {}/{}", 1 + adbProperties.getMaxReconnections() - reconnectsLeft, adbProperties.getMaxReconnections());
                    } else {
                        log.trace("Successful reconnect to ADB, try: {}", 1 - reconnectsLeft);
                    }

                    result.closeHandler(event -> {
                        log.warn("Unexpected connection close");
                        connectionState.set(State.CLOSED);
                        reestablishConnection();
                    });
                    promise.complete(result);
                })
                .onFailure(t -> {
                    if (adbProperties.getMaxReconnections() > 0) {
                        log.error("Could not reconnect to ADB, try: {}/{}", 1 + adbProperties.getMaxReconnections() - reconnectsLeft, adbProperties.getMaxReconnections(), t);
                    } else {
                        log.error("Could not reconnect to ADB, try: {}", 1 - reconnectsLeft, t);
                    }

                    if (adbProperties.getReconnectionInterval() > 0) {
                        vertx.setTimer(adbProperties.getReconnectionInterval(), i -> reconnectWithRetry(reconnectsLeft - 1, promise));
                    } else {
                        vertx.runOnContext(v -> reconnectWithRetry(reconnectsLeft - 1, promise));
                    }
                });
    }

    private void runNextTask() {
        if (connectionState.get() == State.FAILED) {
            while (!queue.isEmpty()) {
                val poll = queue.poll();
                poll.context.runOnContext(event -> poll.promise.fail(new DtmException(CONNECTION_FAILED_MESSAGE)));
            }
            return;
        }

        if (queue.isEmpty()) {
            return;
        }

        if (connectionState.compareAndSet(State.FREE, State.ACQUIRED)) {
            val task = queue.poll();
            if (task == null) {
                connectionState.compareAndSet(State.ACQUIRED, State.FREE);
                runNextTask();
                return;
            }

            task.context.runOnContext(event -> task.promise.complete(connection.get()));
        }
    }

    @AllArgsConstructor
    private static class Task {
        private final Context context;
        private final Promise<SqlConnection> promise;
    }

    enum State {
        FREE,
        ACQUIRED,
        RECONNECTING,
        CLOSED,
        FAILED
    }
}
