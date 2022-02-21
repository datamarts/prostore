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
package ru.datamart.prostore.query.execution.plugin.adb.query.service.impl;

import io.vertx.core.*;
import io.vertx.sqlclient.*;
import io.vertx.sqlclient.impl.ArrayTuple;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import ru.datamart.prostore.async.AsyncUtils;
import ru.datamart.prostore.common.converter.SqlTypeConverter;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.plugin.sql.PreparedStatementRequest;
import ru.datamart.prostore.common.reader.QueryParameters;
import ru.datamart.prostore.query.execution.model.metadata.ColumnMetadata;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.pool.AdbConnectionPool;
import ru.datamart.prostore.query.execution.plugin.api.exception.LlrDatasourceException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class AdbQueryExecutor implements DatabaseExecutor {

    private final AdbConnectionPool pool;
    private final int fetchSize;
    private final SqlTypeConverter fromAdbSqlTypeConverter;
    private final SqlTypeConverter toAdbSqlTypeConverter;

    public AdbQueryExecutor(AdbConnectionPool pool,
                            int fetchSize,
                            SqlTypeConverter fromAdbSqlTypeConverter,
                            SqlTypeConverter toAdbSqlTypeConverter) {
        this.pool = pool;
        this.fetchSize = fetchSize;
        this.fromAdbSqlTypeConverter = fromAdbSqlTypeConverter;
        this.toAdbSqlTypeConverter = toAdbSqlTypeConverter;
    }

    @Override
    public Future<List<Map<String, Object>>> execute(String sql, List<ColumnMetadata> metadata) {
        return executeWithParams(sql, null, metadata);
    }

    @Override
    public Future<List<Map<String, Object>>> executeWithCursor(String sql, List<ColumnMetadata> metadata) {
        return Future.future(promise -> {
            log.debug("ADB. Execute with cursor: [{}]", sql);
            pool.withConnection(conn -> AsyncUtils.measureMs(prepareQuery(conn, sql)
                                    .compose(pgPreparedQuery -> readDataWithCursor(pgPreparedQuery, metadata, fetchSize)),
                            duration -> log.debug("ADB. Execute with cursor succeeded: [{}] in [{}]ms", sql, duration)))
                    .onSuccess(promise::complete)
                    .onFailure(e -> {
                        log.error("ADB. Execute with cursor failed: [{}]", sql, e);
                        promise.fail(e);
                    });
        });
    }

    @Override
    public Future<List<Map<String, Object>>> executeWithParams(String sql,
                                                               QueryParameters params,
                                                               List<ColumnMetadata> metadata) {
        return Future.future(promise -> {
            log.debug("ADB. Execute query: [{}] with params: [{}]", sql, params);
            pool.withConnection(conn -> AsyncUtils.measureMs(executePreparedQuery(conn, sql, createParamsArray(params)),
                    duration -> log.debug("ADB. Execute with params succeeded: [{}] in [{}]ms", sql, duration))
                    .map(rowSet -> createResult(metadata, rowSet)))
                    .onSuccess(promise::complete)
                    .onFailure(fail -> {
                        log.error("ADB. Execute with params failed: [{}]", sql, fail);
                        promise.fail(fail);
                    });
        });
    }

    private ArrayTuple createParamsArray(QueryParameters params) {
        if (params == null || params.getValues().isEmpty()) {
            return null;
        }

        return new ArrayTuple(IntStream.range(0, params.getValues().size())
                .mapToObj(n -> toAdbSqlTypeConverter.convert(params.getTypes().get(n),
                        params.getValues().get(n)))
                .collect(Collectors.toList()));
    }

    @Override
    public Future<Void> executeUpdate(String sql) {
        return Future.future(promise -> {
            log.debug("ADB. Execute update: [{}]", sql);
            pool.withConnection(conn -> AsyncUtils.measureMs(executeQueryUpdate(conn, sql),
                            duration -> log.debug("ADB. Execute update succeeded: [{}] in [{}]ms", sql, duration)))
                    .onSuccess(result -> promise.complete())
                    .onFailure(fail -> {
                        log.error("ADB. Execute update failed: [{}]", sql, fail);
                        promise.fail(fail);
                    });
        });
    }

    private Future<List<Map<String, Object>>> readDataWithCursor(PreparedStatement preparedQuery,
                                                                 List<ColumnMetadata> metadata,
                                                                 Integer fetchSize) {
        return Future.future(promise -> {
            val fixedPromise = wrapWithCurrentContext(promise);

            List<Map<String, Object>> result = new ArrayList<>();
            val pgCursor = preparedQuery.cursor();
            readCursor(pgCursor, fetchSize, metadata, ar -> {
                        if (ar.succeeded()) {
                            result.addAll(ar.result());
                        } else {
                            fixedPromise.fail(ar.cause());
                        }
                    },
                    rr -> {
                        if (rr.succeeded()) {
                            fixedPromise.complete(result);
                        } else {
                            fixedPromise.fail(new DtmException("Error executing fetching data with cursor", rr.cause()));
                        }
                    });
        });
    }

    private void readCursor(Cursor cursor,
                            int chunkSize,
                            List<ColumnMetadata> metadata,
                            Handler<AsyncResult<List<Map<String, Object>>>> itemHandler,
                            Handler<AsyncResult<List<Map<String, Object>>>> handler) {
        cursor.read(chunkSize, res -> {
            if (res.succeeded()) {
                val dataSet = createResult(metadata, res.result());
                itemHandler.handle(Future.succeededFuture(dataSet));
                if (cursor.hasMore()) {
                    readCursor(cursor,
                            chunkSize,
                            metadata,
                            itemHandler,
                            handler);
                } else {
                    cursor.close();
                    handler.handle(Future.succeededFuture(dataSet));
                }
            } else {
                handler.handle(Future.failedFuture(res.cause()));
            }
        });
    }

    @Override
    public Future<Void> executeInTransaction(List<PreparedStatementRequest> requests) {
        return Future.future(p -> {
            log.debug("ADB. Execute in transaction: {}", requests);
            AsyncUtils.measureMs(pool.withTransaction(connection -> {
                                Future<Void> lastFuture = Future.succeededFuture();
                                for (PreparedStatementRequest st : requests) {
                                    log.debug("ADB. Execute query in transaction: [{}] with params: [{}]", st.getSql(), st.getParams());
                                    lastFuture = lastFuture.compose(s -> AsyncUtils.measureMs(execute(st, connection),
                                            duration -> log.debug("ADB. Execute query in transaction succeeded: [{}] in [{}]ms", st.getSql(), duration)));
                                }
                                return lastFuture;
                            }),
                            duration -> log.debug("ADB. Execute in transaction sucess: [{}] in [{}]ms", requests, duration))
                    .onSuccess(event -> p.complete())
                    .onFailure(err -> {
                        log.error("ADB. Execute in transaction failed: [{}]", requests, err);
                        p.fail(new LlrDatasourceException(String.format("Error executing queries: %s",
                                err.getMessage())));
                    });
        });
    }

    private Future<PreparedStatement> prepareQuery(SqlConnection conn, String sql) {
        return Future.future(promise -> conn.prepare(sql, promise));
    }

    private Future<RowSet<Row>> executeQueryUpdate(SqlConnection conn, String sql) {
        return Future.future(promise -> conn.query(sql).execute(wrapWithCurrentContext(promise)));
    }

    private Future<RowSet<Row>> executePreparedQuery(SqlConnection conn, String sql, Tuple params) {
        return Future.future(promise -> {
            if (params == null) {
                conn.query(sql).execute(wrapWithCurrentContext(promise));
                return;
            }

            conn.preparedQuery(sql).execute(params, wrapWithCurrentContext(promise));
        });
    }

    private List<Map<String, Object>> createResult(List<ColumnMetadata> metadata,
                                                   RowSet<Row> pgRowSet) {
        List<Map<String, Object>> result = new ArrayList<>();
        Function<Row, Map<String, Object>> func = metadata.isEmpty()
                ? row -> createRowMap(row, pgRowSet.columnsNames().size())
                : row -> createRowMap(metadata, row);
        for (Row row : pgRowSet) {
            result.add(func.apply(row));
        }
        return result;
    }

    private Map<String, Object> createRowMap(List<ColumnMetadata> metadata, Row row) {
        Map<String, Object> rowMap = new HashMap<>();
        for (int i = 0; i < metadata.size(); i++) {
            ColumnMetadata columnMetadata = metadata.get(i);
            rowMap.put(columnMetadata.getName(),
                    fromAdbSqlTypeConverter.convert(columnMetadata.getType(), row.getValue(i)));
        }
        return rowMap;
    }

    private Map<String, Object> createRowMap(Row row, int size) {
        Map<String, Object> rowMap = new HashMap<>();
        for (int i = 0; i < size; i++) {
            rowMap.put(row.getColumnName(i), row.getValue(i));
        }
        return rowMap;
    }

    private Future<Void> execute(PreparedStatementRequest request, SqlConnection connection) {
        return Future.future((Handler<Promise<RowSet<Row>>>) promise -> connection.query(request.getSql()).execute(wrapWithCurrentContext(promise)))
                .mapEmpty();
    }

    private <T> Promise<T> wrapWithCurrentContext(Promise<T> promise) {
        val context = Vertx.currentContext();
        if (context == null) {
            return promise;
        }

        Promise<T> connectionPromise = Promise.promise();
        connectionPromise.future().onComplete(ar -> context.runOnContext(ignored -> promise.handle(ar)));
        return connectionPromise;
    }
}
