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
package ru.datamart.prostore.query.execution.plugin.adb.query.service.pool;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.TransactionRollbackException;
import lombok.val;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class AdbConnectionPool {
    private final AdbConnection[] connections;
    private final int poolSize;
    private final AtomicInteger roundRobin = new AtomicInteger(0);

    public AdbConnectionPool(AdbConnectionFactory connectionFactory, Vertx vertx, int poolSize) {
        this.poolSize = poolSize;
        this.connections = new AdbConnection[poolSize];
        for (int i = 0; i < poolSize; i++) {
            connections[i] = connectionFactory.createAdbConnection(vertx);
        }
    }

    public <T> Future<T> withConnection(Function<SqlConnection, Future<T>> function) {
        val connection = getConnection();
        return connection.acquire().compose(sqlConnection -> function.apply(sqlConnection)
                .onComplete(event -> connection.release()));
    }

    public <T> Future<T> withTransaction(Function<SqlConnection, Future<T>> function) {
        val connection = getConnection();
        return connection
                .acquire()
                .flatMap(conn -> conn
                        .begin()
                        .flatMap(tx -> function
                                .apply(conn)
                                .compose(
                                        res -> tx
                                                .commit()
                                                .flatMap(v -> Future.succeededFuture(res)),
                                        err -> {
                                            if (err instanceof TransactionRollbackException) {
                                                return Future.failedFuture(err);
                                            } else {
                                                return tx
                                                        .rollback()
                                                        .compose(v -> Future.failedFuture(err), failure -> Future.failedFuture(err));
                                            }
                                        }))
                        .onComplete(ar -> connection.release()));
    }

    private AdbConnection getConnection() {
        while (true) {
            int i = roundRobin.get();
            int next = (i + 1) % poolSize;
            if (roundRobin.compareAndSet(i, next)) {
                return connections[i];
            }
        }
    }
}
